package io.taskforge;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Channel;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.dao.DataAccessResourceFailureException;

class RabbitConsumerTest {
    final WorkerEngine worker = mock(WorkerEngine.class);
    final Channel channel = mock(Channel.class);
    final ObjectMapper json = new ObjectMapper();
    final RabbitConsumer consumer = new RabbitConsumer(worker, json, new SimpleMeterRegistry());

    @Test
    void acknowledgesOnlyAfterWorkerAcceptsTheNotification() throws Exception {
        DispatchMessage notification = notification();
        when(worker.accept(notification.taskId(), 0)).thenReturn(true);
        consumer.onMessage(message(json.writeValueAsBytes(notification)), channel);
        var order = inOrder(worker, channel);
        order.verify(worker).accept(notification.taskId(), 0);
        order.verify(channel).basicAck(7, false);
    }

    @Test
    void waitsForCapacityWithoutRequeueingTheDelivery() throws Exception {
        DispatchMessage notification = notification();
        when(worker.accept(notification.taskId(), 0)).thenReturn(false, true);
        consumer.onMessage(message(json.writeValueAsBytes(notification)), channel);
        verify(worker, times(2)).accept(notification.taskId(), 0);
        verify(channel).basicAck(7, false);
        verify(channel, never()).basicReject(anyLong(), anyBoolean());
        verify(channel, never()).basicNack(anyLong(), anyBoolean(), anyBoolean());
    }

    @Test
    void databaseFailureRequeuesWithoutAcknowledgingTheDelivery() throws Exception {
        DispatchMessage notification = notification();
        when(worker.accept(notification.taskId(), 0)).thenThrow(new DataAccessResourceFailureException("unavailable"));
        consumer.onMessage(message(json.writeValueAsBytes(notification)), channel);
        verify(channel, never()).basicAck(anyLong(), anyBoolean());
        verify(channel).basicNack(7, false, true);
    }

    @Test
    void malformedPayloadIsRejectedWithoutRequeueing() throws Exception {
        consumer.onMessage(message("{bad-json".getBytes(java.nio.charset.StandardCharsets.UTF_8)), channel);
        verify(channel).basicReject(7, false);
        verifyNoInteractions(worker);
        verify(channel, never()).basicAck(anyLong(), anyBoolean());
    }

    @Test
    void invalidGenerationIsRejectedBeforeDatabaseAccess() throws Exception {
        DispatchMessage notification = new DispatchMessage(UUID.randomUUID().toString(), UUID.randomUUID().toString(), -1);
        consumer.onMessage(message(json.writeValueAsBytes(notification)), channel);
        verify(channel).basicReject(7, false);
        verifyNoInteractions(worker);
    }

    @Test
    void closeReleasesAConsumerWaitingForExecutionCapacity() throws Exception {
        DispatchMessage notification = notification();
        CountDownLatch waiting = new CountDownLatch(1);
        when(worker.accept(notification.taskId(), 0)).thenAnswer(invocation -> {
            waiting.countDown();
            return false;
        });
        var executor = Executors.newSingleThreadExecutor();
        try {
            var finished = executor.submit(() -> {
                try {
                    consumer.onMessage(message(json.writeValueAsBytes(notification)), channel);
                    return null;
                } catch (Exception stopped) {
                    return stopped;
                }
            });
            assertThat(waiting.await(2, TimeUnit.SECONDS)).isTrue();
            consumer.close();
            assertThat(finished.get(2, TimeUnit.SECONDS)).isInstanceOf(InterruptedException.class);
            verify(channel, never()).basicAck(anyLong(), anyBoolean());
            verify(channel, never()).basicNack(anyLong(), anyBoolean(), anyBoolean());
        } finally {
            consumer.close();
            executor.shutdownNow();
        }
    }

    @Test
    void closedConsumerDoesNotClaimAnotherTask() throws Exception {
        Message delivery = message(json.writeValueAsBytes(notification()));
        consumer.close();
        try {
            assertThatThrownBy(() -> consumer.onMessage(delivery, channel)).isInstanceOf(InterruptedException.class);
            verifyNoInteractions(worker);
            verify(channel, never()).basicAck(anyLong(), anyBoolean());
        } finally {
            // Clear the flag set by the listener so it cannot leak into subsequent tests.
            Thread.interrupted();
        }
    }

    private DispatchMessage notification() {
        return new DispatchMessage(UUID.randomUUID().toString(), UUID.randomUUID().toString(), 0);
    }

    private Message message(byte[] body) {
        var properties = new MessageProperties();
        properties.setDeliveryTag(7);
        return new Message(body, properties);
    }
}
