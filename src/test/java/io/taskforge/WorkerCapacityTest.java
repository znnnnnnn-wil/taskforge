package io.taskforge;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest(properties = {
        "spring.datasource.url=jdbc:h2:mem:capacity;MODE=PostgreSQL;DB_CLOSE_DELAY=-1",
        "taskforge.worker-enabled=false"})
class WorkerCapacityTest {
    @Autowired TaskStore store;
    @Autowired TaskLifecycle lifecycle;
    @Autowired ObjectMapper json;

    @Test
    void overlappingRetriesKeepTheirSlotsUntilBothExecutionsExit() throws Exception {
        CountDownLatch firstExit = new CountDownLatch(1);
        CountDownLatch secondExit = new CountDownLatch(1);
        CountDownLatch firstStarted = new CountDownLatch(1);
        CountDownLatch secondStarted = new CountDownLatch(1);
        TaskHandler stubborn = new TaskHandler() {
            public String name() { return "stubborn"; }
            public void validate(JsonNode payload) { }
            public JsonNode execute(JsonNode payload, TaskContext context) {
                boolean first = context.attempt() == 1;
                (first ? firstStarted : secondStarted).countDown();
                CountDownLatch exit = first ? firstExit : secondExit;
                // Model an external operation that continues after interruption.
                while (exit.getCount() > 0) {
                    try { exit.await(); }
                    catch (InterruptedException ignored) { }
                }
                return json.createObjectNode();
            }
        };
        TaskHandler echo = new BuiltinHandlers().echoHandler();
        var properties = new ForgeProperties("local-development-key", true, "capacity", 2, 30, 50);
        WorkerEngine worker = new WorkerEngine(store, lifecycle, new HandlerRegistry(List.of(stubborn, echo)),
                properties, json, new SimpleMeterRegistry());
        try {
            Task retry = submit("stubborn", 2, 1);
            worker.poll();
            assertThat(firstStarted.await(5, TimeUnit.SECONDS)).isTrue();
            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
                worker.maintain();
                assertThat(store.require(retry.id()).status()).isEqualTo("RETRY");
            });
            store.jdbc().update("UPDATE forge_task SET available_at=CURRENT_TIMESTAMP WHERE id=?", retry.id());
            worker.poll();
            assertThat(secondStarted.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(worker.activeCount()).isEqualTo(2);

            Task waiting = submit("echo", 1, 1);
            worker.poll();
            assertThat(store.require(waiting.id()).status()).isEqualTo("PENDING");
            assertThat(store.require(waiting.id()).attempts()).isZero();

            // Exiting the old attempt releases only its slot, preserving the retry's record.
            firstExit.countDown();
            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
                assertThat(store.require(waiting.id()).status()).isEqualTo("SUCCEEDED");
                assertThat(worker.activeCount()).isEqualTo(1);
            });
            secondExit.countDown();
            await().atMost(Duration.ofSeconds(5)).until(() -> worker.activeCount() == 0);
            assertThat(store.attempts(waiting.id())).extracting(Attempt::status).containsExactly("SUCCEEDED");
        } finally {
            firstExit.countDown();
            secondExit.countDown();
            worker.close();
        }
    }

    private Task submit(String handler, int attempts, int timeout) {
        return store.submit(new Submission(UUID.randomUUID().toString(), handler,
                json.createObjectNode(), 0, attempts, 1, timeout, null));
    }
}
