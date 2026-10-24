package io.taskforge;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Channel;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.UUID;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.listener.api.ChannelAwareMessageListener;

public class RabbitConsumer implements ChannelAwareMessageListener, AutoCloseable {
    private final WorkerEngine worker;
    private final ObjectMapper json;
    private final MeterRegistry metrics;
    private volatile boolean closing;

    public RabbitConsumer(WorkerEngine worker, ObjectMapper json, MeterRegistry metrics) {
        this.worker = worker;
        this.json = json;
        this.metrics = metrics;
    }

    @Override
    public void onMessage(Message message, Channel channel) throws Exception {
        long tag = message.getMessageProperties().getDeliveryTag();
        metrics.counter("taskforge.rabbit.received").increment();
        DispatchMessage dispatch;
        try {
            if (message.getBody().length > 4096) throw new IllegalArgumentException("通知过大");
            dispatch = json.readValue(message.getBody(), DispatchMessage.class);
            if (dispatch == null || dispatch.expectedToken() < 0) throw new IllegalArgumentException("版本无效");
            UUID.fromString(dispatch.eventId());
            UUID.fromString(dispatch.taskId());
        } catch (Exception invalid) {
            metrics.counter("taskforge.rabbit.invalid").increment();
            channel.basicReject(tag, false);
            return;
        }
        try {
            // Prefetch limits deliveries, while accept independently enforces actual execution capacity.
            while (true) {
                if (closing) throw new InterruptedException("消费者正在关闭");
                if (worker.accept(dispatch.taskId(), dispatch.expectedToken())) break;
                Thread.sleep(100);
            }
            channel.basicAck(tag, false);
            metrics.counter("taskforge.rabbit.acknowledged").increment();
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw interrupted;
        } catch (RuntimeException unavailable) {
            // Back off before requeue so a database outage does not create a tight delivery loop.
            Thread.sleep(1000);
            channel.basicNack(tag, false, true);
            metrics.counter("taskforge.rabbit.requeued").increment();
        }
    }

    @Override public void close() { closing = true; }
}
