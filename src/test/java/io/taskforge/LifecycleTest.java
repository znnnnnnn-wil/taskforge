package io.taskforge;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest(properties = {
        "spring.datasource.url=jdbc:h2:mem:lifecycle;MODE=PostgreSQL;DB_CLOSE_DELAY=-1",
        "taskforge.worker-enabled=false"})
class LifecycleTest {
    @Autowired TaskStore store;
    @Autowired TaskLifecycle lifecycle;
    @Autowired ObjectMapper json;

    Task task(int attempts) {
        return store.submit(new Submission(UUID.randomUUID().toString(), "echo",
                json.createObjectNode(), 0, attempts, 1, 60, null));
    }

    @Test
    void onlyOneNodeClaimsEachExecution() throws Exception {
        Task pending = task(3);
        var calls = new ArrayList<Callable<Optional<Task>>>();
        for (int i = 0; i < 16; i++) {
            String node = "node-" + i;
            calls.add(() -> lifecycle.claim(pending.id(), 0, node));
        }
        var pool = Executors.newFixedThreadPool(8);
        try {
            long winners = 0;
            for (var result : pool.invokeAll(calls)) {
                if (result.get().isPresent()) winners++;
            }
            assertThat(winners).isEqualTo(1);
        } finally {
            pool.shutdownNow();
        }
        assertThat(store.attempts(pending.id())).hasSize(1);
    }

    @Test
    void expiredOwnerCannotRenewOrComplete() {
        Task first = lifecycle.claim(task(3).id(), 0, "dead-node").orElseThrow();
        store.jdbc().update("UPDATE forge_task SET lease_until = DATEADD('SECOND', -1, CURRENT_TIMESTAMP) WHERE id = ?", first.id());
        assertThat(lifecycle.heartbeat(first, 10)).isFalse();
        assertThat(lifecycle.finish(first, "{}", null)).isFalse();
        assertThat(lifecycle.recoverExpired(100)).isEqualTo(1);
        store.jdbc().update("UPDATE forge_task SET available_at = CURRENT_TIMESTAMP WHERE id = ?", first.id());
        Task second = lifecycle.claim(first.id(), store.require(first.id()).token(), "new-node").orElseThrow();
        assertThat(second.token()).isGreaterThan(first.token());
        assertThat(lifecycle.finish(first, "old-result", null)).isFalse();
        assertThat(lifecycle.finish(second, "new-result", null)).isTrue();
        assertThat(store.require(first.id()).result()).isEqualTo("new-result");
        assertThat(store.attempts(first.id())).extracting(Attempt::status).containsExactly("LOST", "SUCCEEDED");
    }

    @Test
    void retryBudgetAndCancellationAreDurable() {
        Task running = lifecycle.claim(task(1).id(), 0, "node").orElseThrow();
        assertThat(lifecycle.finish(running, null, "failure")).isTrue();
        assertThat(store.require(running.id()).status()).isEqualTo("FAILED");
        assertThat(lifecycle.claim(running.id(), running.token(), "node")).isEmpty();
        Task other = lifecycle.claim(task(3).id(), 0, "node").orElseThrow();
        assertThat(lifecycle.cancel(other.id()).status()).isEqualTo("CANCELLED");
        assertThat(lifecycle.finish(other, "late", null)).isFalse();
        assertThat(lifecycle.cancel(other.id()).status()).isEqualTo("CANCELLED");
        assertThat(store.attempts(other.id()).get(0).status()).isEqualTo("CANCELLED");
    }
}
