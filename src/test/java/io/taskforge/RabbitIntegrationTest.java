package io.taskforge;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.autoconfigure.actuate.observability.AutoConfigureObservability;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
@AutoConfigureObservability
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@SpringBootTest(properties = {
        "taskforge.rabbit.enabled=true", "taskforge.rabbit.publish-ms=100",
        "taskforge.worker-enabled=true", "taskforge.concurrency=2", "taskforge.poll-ms=50"})
class RabbitIntegrationTest {
    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16");
    @Container
    static final GenericContainer<?> RABBIT = new GenericContainer<>("rabbitmq:4.2.9-management")
            .withEnv("RABBITMQ_DEFAULT_USER", "taskforge")
            .withEnv("RABBITMQ_DEFAULT_PASS", "integration-only-secret")
            .withExposedPorts(5672)
            .waitingFor(Wait.forLogMessage(".*Server startup complete.*", 1))
            .withStartupTimeout(Duration.ofMinutes(2));

    @DynamicPropertySource
    static void services(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
        registry.add("spring.rabbitmq.host", RABBIT::getHost);
        registry.add("spring.rabbitmq.port", () -> RABBIT.getMappedPort(5672));
        registry.add("spring.rabbitmq.username", () -> "taskforge");
        registry.add("spring.rabbitmq.password", () -> "integration-only-secret");
    }

    @Autowired TaskStore store;
    @Autowired TaskLifecycle lifecycle;
    @Autowired WorkerEngine worker;
    @Autowired RabbitTemplate rabbit;
    @Autowired ObjectMapper json;
    @Autowired MeterRegistry metrics;

    @AfterEach
    void cleanup() {
        store.list(null, 200, 0).stream().filter(task -> !task.terminal())
                .forEach(task -> lifecycle.cancel(task.id()));
        await().atMost(Duration.ofSeconds(8)).until(() -> worker.activeCount() == 0);
    }

    @Test
    void outboxDeliversTaskThroughBrokerAndPersistsResult() {
        double before = metrics.counter("taskforge.rabbit.received").count();
        Task task = submit("echo", 1, null, 0);
        until(task.id(), "SUCCEEDED");
        assertThat(store.attempts(task.id())).extracting(Attempt::status).containsExactly("SUCCEEDED");
        assertThat(store.require(task.id()).result()).contains("rabbit-test");
        assertThat(metrics.counter("taskforge.rabbit.received").count()).isGreaterThan(before);
    }

    @Test
    void duplicateNotificationsCannotStartAnotherAttempt() throws Exception {
        Task task = submit("delay", 1, null, 600);
        for (int i = 0; i < 24; i++) publish(new DispatchMessage(UUID.randomUUID().toString(), task.id(), 0));
        until(task.id(), "SUCCEEDED");
        assertThat(store.require(task.id()).attempts()).isEqualTo(1);
        assertThat(store.attempts(task.id())).extracting(Attempt::status).containsExactly("SUCCEEDED");
    }

    @Test
    void futureTasksAreNotPublishedBeforeTheirDueTime() {
        Task task = submit("echo", 1, Instant.now().plusSeconds(3), 0);
        await().during(Duration.ofMillis(700)).atMost(Duration.ofSeconds(2)).untilAsserted(() -> {
            assertThat(store.require(task.id()).status()).isEqualTo("PENDING");
            assertThat(store.require(task.id()).attempts()).isZero();
        });
        until(task.id(), "SUCCEEDED");
        assertThat(store.require(task.id()).updatedAt()).isAfterOrEqualTo(task.availableAt());
    }

    @Test
    void staleNotificationCannotConsumeTheNextRetryGeneration() throws Exception {
        Task task = submit("echo", 2, Instant.now().plusSeconds(3600), 0);
        store.jdbc().update("UPDATE forge_task SET available_at=CURRENT_TIMESTAMP WHERE id=?", task.id());
        Task first = lifecycle.claim(task.id(), 0, "failed-node").orElseThrow();
        assertThat(lifecycle.finish(first, null, "temporary failure")).isTrue();
        for (int i = 0; i < 12; i++) publish(new DispatchMessage(UUID.randomUUID().toString(), task.id(), 0));
        await().during(Duration.ofMillis(700)).atMost(Duration.ofSeconds(2)).untilAsserted(() -> {
            assertThat(store.require(task.id()).status()).isEqualTo("RETRY");
            assertThat(store.require(task.id()).attempts()).isEqualTo(1);
        });
        until(task.id(), "SUCCEEDED");
        assertThat(store.attempts(task.id())).extracting(Attempt::status).containsExactly("FAILED", "SUCCEEDED");
    }

    @Test
    void leaseRecoveryPublishesAnotherGenerationAndRejectsOldResult() {
        Task task = submit("echo", 2, Instant.now().plusSeconds(3600), 0);
        store.jdbc().update("UPDATE forge_task SET available_at=CURRENT_TIMESTAMP WHERE id=?", task.id());
        Task old = lifecycle.claim(task.id(), 0, "crashed-node").orElseThrow();
        store.jdbc().update("UPDATE forge_task SET lease_until=CURRENT_TIMESTAMP - INTERVAL '1 second' WHERE id=?", task.id());
        lifecycle.recoverExpired(100);
        until(task.id(), "SUCCEEDED");
        assertThat(lifecycle.finish(old, "{}", null)).isFalse();
        assertThat(store.attempts(task.id())).extracting(Attempt::status).containsExactly("LOST", "SUCCEEDED");
        assertThat(store.attempts(task.id()).get(1).nodeId()).isNotEqualTo("crashed-node");
    }

    @Test
    void invalidMessagesAreDeadLetteredWithoutCreatingTasks() {
        byte[] invalid = "{broken-json".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        rabbit.send(RabbitTransport.EXCHANGE, "ready", new Message(invalid, new MessageProperties()));
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            Message dead = rabbit.receive(RabbitTransport.DEAD_QUEUE);
            assertThat(dead).isNotNull();
            assertThat(dead.getBody()).isEqualTo(invalid);
        });
    }

    @Test
    void manualAckDoesNotBypassLocalExecutionCapacity() {
        List<String> ids = new ArrayList<>();
        for (int i = 0; i < 6; i++) ids.add(submit("delay", 1, null, 10000).id());
        await().atMost(Duration.ofSeconds(10)).until(() -> worker.activeCount() == 2);
        await().during(Duration.ofMillis(600)).atMost(Duration.ofSeconds(3)).untilAsserted(() -> {
            assertThat(worker.activeCount()).isEqualTo(2);
            List<Task> tasks = ids.stream().map(store::require).toList();
            assertThat(tasks.stream().filter(task -> task.status().equals("RUNNING"))).hasSize(2);
            assertThat(tasks.stream().filter(task -> task.status().equals("PENDING"))).hasSize(4);
        });
    }

    private Task submit(String handler, int attempts, Instant available, int delay) {
        var payload = json.createObjectNode().put("message", "rabbit-test");
        if (handler.equals("delay")) payload.put("milliseconds", delay);
        return store.submit(new Submission(UUID.randomUUID().toString(), handler, payload,
                0, attempts, 3, 30, available));
    }

    private void publish(DispatchMessage message) throws Exception {
        var properties = new MessageProperties();
        properties.setContentType(MessageProperties.CONTENT_TYPE_JSON);
        rabbit.send(RabbitTransport.EXCHANGE, "ready", new Message(json.writeValueAsBytes(message), properties));
    }

    private void until(String id, String status) {
        await().atMost(Duration.ofSeconds(25)).until(() -> store.require(id).status().equals(status));
    }
}
