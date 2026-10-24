package io.taskforge;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.AmqpConnectException;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageDeliveryMode;
import org.springframework.amqp.core.ReturnedMessage;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;

class OutboxPublisherTest {
    final OutboxStore outbox = mock(OutboxStore.class);
    final RabbitTemplate rabbit = mock(RabbitTemplate.class);
    final ObjectMapper json = new ObjectMapper();
    final OutboxPublisher publisher = new OutboxPublisher(outbox, rabbit, json, new SimpleMeterRegistry());
    final OutboxStore.Event event = new OutboxStore.Event(new DispatchMessage(
            UUID.randomUUID().toString(), UUID.randomUUID().toString(), 4), 9, "reservation-owner", 1);

    void pendingEvent() {
        when(outbox.reserve()).thenReturn(Optional.of(event), Optional.empty());
    }

    @Test
    void confirmedAndRoutedPublishMarksEventSent() {
        pendingEvent();
        doAnswer(invocation -> {
            Message message = invocation.getArgument(2);
            assertThat(message.getMessageProperties().getDeliveryMode()).isEqualTo(MessageDeliveryMode.PERSISTENT);
            assertThat(message.getMessageProperties().getPriority()).isEqualTo(9);
            assertThat(message.getMessageProperties().getMessageId()).isEqualTo(event.message().eventId());
            assertThat(json.readValue(message.getBody(), DispatchMessage.class)).isEqualTo(event.message());
            invocation.<CorrelationData>getArgument(3).getFuture().complete(new CorrelationData.Confirm(true, null));
            return null;
        }).when(rabbit).send(eq(RabbitTransport.EXCHANGE), eq("ready"), any(Message.class), any(CorrelationData.class));
        publisher.publish();
        verify(outbox).sent(event);
        verify(outbox, never()).failed(any(), anyString());
    }

    @Test
    void negativeConfirmKeepsEventForRetry() {
        pendingEvent();
        doAnswer(invocation -> {
            invocation.<CorrelationData>getArgument(3).getFuture().complete(new CorrelationData.Confirm(false, "rejected"));
            return null;
        }).when(rabbit).send(anyString(), anyString(), any(Message.class), any(CorrelationData.class));
        publisher.publish();
        verify(outbox).failed(eq(event), anyString());
        verify(outbox, never()).sent(any());
    }

    @Test
    void mandatoryReturnFailsEvenWhenBrokerConfirmsThePublish() {
        pendingEvent();
        doAnswer(invocation -> {
            CorrelationData correlation = invocation.getArgument(3);
            correlation.setReturned(new ReturnedMessage(invocation.getArgument(2), 312,
                    "NO_ROUTE", RabbitTransport.EXCHANGE, "ready"));
            correlation.getFuture().complete(new CorrelationData.Confirm(true, null));
            return null;
        }).when(rabbit).send(anyString(), anyString(), any(Message.class), any(CorrelationData.class));
        publisher.publish();
        verify(outbox).failed(eq(event), anyString());
        verify(outbox, never()).sent(any());
    }

    @Test
    void connectionFailureKeepsEventForRetry() {
        pendingEvent();
        doThrow(new AmqpConnectException(new java.net.ConnectException("offline")))
                .when(rabbit).send(anyString(), anyString(), any(Message.class), any(CorrelationData.class));
        publisher.publish();
        verify(outbox).failed(eq(event), anyString());
        verify(outbox, never()).sent(any());
    }

    @Test
    void missingConfirmTimesOutWithoutMarkingEventSent() {
        pendingEvent();
        publisher.publish();
        verify(outbox).failed(eq(event), contains("TimeoutException"));
        verify(outbox, never()).sent(any());
    }
}
