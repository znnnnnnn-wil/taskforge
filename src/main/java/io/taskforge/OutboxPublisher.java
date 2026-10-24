package io.taskforge;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.concurrent.TimeUnit;
import java.util.ArrayList;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageDeliveryMode;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(name = "taskforge.rabbit.enabled", havingValue = "true")
public class OutboxPublisher {
    private final OutboxStore outbox;
    private final RabbitTemplate rabbit;
    private final ObjectMapper json;
    private final MeterRegistry metrics;

    public OutboxPublisher(OutboxStore outbox, RabbitTemplate rabbit, ObjectMapper json, MeterRegistry metrics) {
        this.outbox = outbox;
        this.rabbit = rabbit;
        this.json = json;
        this.metrics = metrics;
    }

    @Scheduled(fixedDelayString = "${taskforge.rabbit.publish-ms:100}")
    public void publish() {
        var pending = new ArrayList<Pending>();
        for (int i = 0; i < 32 && !Thread.currentThread().isInterrupted(); i++) {
            var reserved = outbox.reserve();
            if (reserved.isEmpty()) break;
            var event = reserved.get();
            try {
                var properties = new MessageProperties();
                properties.setContentType(MessageProperties.CONTENT_TYPE_JSON);
                properties.setDeliveryMode(MessageDeliveryMode.PERSISTENT);
                properties.setMessageId(event.message().eventId());
                properties.setPriority(event.priority());
                var correlation = new CorrelationData(event.message().eventId());
                rabbit.send(RabbitTransport.EXCHANGE, "ready",
                        new Message(json.writeValueAsBytes(event.message()), properties), correlation);
                pending.add(new Pending(event, correlation));
            } catch (Exception failure) {
                failed(event, failure);
                break;
            }
        }
        // Publish the bounded batch first, then share one confirmation deadline across it.
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        for (Pending item : pending) {
            try {
                var confirmation = item.correlation().getFuture().get(
                        Math.max(1, deadline - System.nanoTime()), TimeUnit.NANOSECONDS);
                if (!confirmation.isAck() || item.correlation().getReturned() != null) {
                    throw new IllegalStateException("Broker 未确认或消息无法路由");
                }
                outbox.sent(item.event());
                metrics.counter("taskforge.rabbit.published").increment();
            } catch (Exception failure) {
                failed(item.event(), failure);
            }
        }
    }

    private void failed(OutboxStore.Event event, Exception failure) {
        outbox.failed(event, failure.getClass().getSimpleName());
        metrics.counter("taskforge.rabbit.publish.failed").increment();
        if (failure instanceof InterruptedException) Thread.currentThread().interrupt();
    }

    private record Pending(OutboxStore.Event event, CorrelationData correlation) { }
}
