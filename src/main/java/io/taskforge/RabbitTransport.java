package io.taskforge;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.amqp.core.AcknowledgeMode;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.Declarables;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConditionalOnProperty(name = "taskforge.rabbit.enabled", havingValue = "true")
public class RabbitTransport {
    public static final String EXCHANGE = "taskforge.dispatch";
    public static final String QUEUE = "taskforge.ready";
    public static final String DEAD_QUEUE = "taskforge.dead";

    @Bean
    Declarables dispatchTopology() {
        var exchange = new DirectExchange(EXCHANGE, true, false);
        var dead = new DirectExchange(EXCHANGE + ".dead", true, false);
        var ready = QueueBuilder.durable(QUEUE).maxPriority(9)
                .deadLetterExchange(dead.getName()).deadLetterRoutingKey("dead").build();
        var failures = QueueBuilder.durable(DEAD_QUEUE).build();
        return new Declarables(exchange, dead, ready, failures,
                BindingBuilder.bind(ready).to(exchange).with("ready"),
                BindingBuilder.bind(failures).to(dead).with("dead"));
    }

    @Bean
    @ConditionalOnProperty(name = "taskforge.worker-enabled", havingValue = "true", matchIfMissing = true)
    SimpleMessageListenerContainer taskDeliveries(ConnectionFactory connectionFactory, WorkerEngine worker,
                                                 ObjectMapper json, MeterRegistry metrics, ForgeProperties config) {
        var listener = new RabbitConsumer(worker, json, metrics);
        var container = new SimpleMessageListenerContainer(connectionFactory) {
            @Override public void stop() { listener.close(); super.stop(); }
            @Override public void stop(Runnable callback) { listener.close(); super.stop(callback); }
        };
        container.setQueueNames(QUEUE);
        container.setAcknowledgeMode(AcknowledgeMode.MANUAL);
        container.setPrefetchCount(1);
        container.setConcurrentConsumers(config.concurrency());
        container.setMaxConcurrentConsumers(config.concurrency());
        container.setDefaultRequeueRejected(true);
        container.setMissingQueuesFatal(false);
        container.setRecoveryInterval(3000);
        container.setShutdownTimeout(3000);
        container.setForceCloseChannel(true);
        container.setMessageListener(listener);
        return container;
    }
}
