package io.taskforge;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;

@SpringBootTest(properties = {
        "spring.datasource.url=jdbc:h2:mem:worker;MODE=PostgreSQL;DB_CLOSE_DELAY=-1",
        "taskforge.poll-ms=50", "taskforge.concurrency=2", "taskforge.lease-seconds=6"})
class WorkerTest {
    @Autowired TaskStore store;
    @Autowired TaskLifecycle lifecycle;
    @Autowired WorkerEngine worker;
    @Autowired ObjectMapper json;

    Task submit(String handler, JsonNode payload, int attempts, int timeout) {
        return store.submit(new Submission(UUID.randomUUID().toString(), handler, payload, 0, attempts, 1, timeout, null));
    }

    void until(String id, String state) {
        await().atMost(Duration.ofSeconds(12)).until(() -> store.require(id).status().equals(state));
    }

    @AfterEach
    void cleanup() {
        store.list(null, 200, 0).stream().filter(task -> !task.terminal()).forEach(task -> lifecycle.cancel(task.id()));
        await().atMost(Duration.ofSeconds(5)).until(() -> worker.activeCount() == 0);
    }

    @Test
    void executesRegisteredHandlersAndRetriesTransientFailure() {
        Task echo = submit("echo", json.createObjectNode().put("text", "hello"), 1, 30);
        until(echo.id(), "SUCCEEDED");
        assertThat(store.require(echo.id()).result()).contains("hello");
        Task flaky = submit("flaky", json.createObjectNode(), 3, 30);
        until(flaky.id(), "SUCCEEDED");
        assertThat(store.require(flaky.id()).attempts()).isEqualTo(2);
        assertThat(store.attempts(flaky.id())).extracting(Attempt::status).containsExactly("FAILED", "SUCCEEDED");
    }

    @Test
    void enforcesTimeoutAndCooperativelyCancelsRunningTask() {
        Task timeout = submit("delay", json.createObjectNode().put("milliseconds", 10000), 1, 1);
        until(timeout.id(), "FAILED");
        assertThat(store.require(timeout.id()).error()).contains("超时");
        Task cancelled = submit("delay", json.createObjectNode().put("milliseconds", 10000), 1, 30);
        until(cancelled.id(), "RUNNING");
        lifecycle.cancel(cancelled.id());
        await().atMost(Duration.ofSeconds(4)).until(() -> worker.activeCount() == 0);
        assertThat(store.attempts(cancelled.id()).get(0).status()).isEqualTo("CANCELLED");
    }

    @Test
    void doesNotPrefetchMoreThanExecutionCapacity() {
        for (int i = 0; i < 6; i++) submit("delay", json.createObjectNode().put("milliseconds", 10000), 1, 30);
        await().atMost(Duration.ofSeconds(4)).until(() -> worker.activeCount() == 2);
        assertThat(store.list("RUNNING", 200, 0)).hasSize(2);
        assertThat(store.list("PENDING", 200, 0)).hasSize(4);
    }

    @TestConfiguration
    static class ExtraHandlers {
        @Bean TaskHandler flaky(ObjectMapper json) {
            return new TaskHandler() {
                public String name() { return "flaky"; }
                public void validate(JsonNode payload) { }
                public JsonNode execute(JsonNode payload, TaskContext context) {
                    if (context.attempt() == 1) throw new IllegalStateException("临时错误");
                    return json.createObjectNode().put("recovered", true);
                }
            };
        }
    }
}
