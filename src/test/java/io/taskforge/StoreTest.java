package io.taskforge;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest(properties = {
        "spring.datasource.url=jdbc:h2:mem:store;MODE=PostgreSQL;DB_CLOSE_DELAY=-1",
        "taskforge.worker-enabled=false"})
class StoreTest {
    @Autowired JdbcTemplate jdbc;
    @Autowired TaskStore store;
    @Autowired ObjectMapper json;

    Submission request(String key) throws Exception {
        return new Submission(key, "echo", json.readTree("{\"message\":\"你好\"}"),
                3, 3, 1, 30, null);
    }

    @Test
    void durableSubmissionAndIdempotentReplay() throws Exception {
        Submission request = request(UUID.randomUUID().toString());
        Task first = store.submit(request);
        assertThat(store.submit(request).id()).isEqualTo(first.id());
        assertThat(first.status()).isEqualTo("PENDING");
        assertThat(store.require(first.id()).payload()).contains("你好");
        assertThat(store.attempts(first.id())).isEmpty();
    }

    @Test
    void rejectsConflictingIdempotencyKey() throws Exception {
        String key = UUID.randomUUID().toString();
        store.submit(request(key));
        Submission changed = new Submission(key, "echo", json.createObjectNode(), 3, 3, 1, 30, null);
        assertThatThrownBy(() -> store.submit(changed)).isInstanceOf(ForgeException.class)
                .hasMessageContaining("不同内容");
    }

    @Test
    void futureTaskIsNotDue() throws Exception {
        Submission input = request(UUID.randomUUID().toString());
        Task task = store.submit(new Submission(input.requestKey(), input.handler(), input.payload(),
                0, 1, 1, 10, Instant.now().plusSeconds(3600)));
        assertThat(store.due(100)).extracting(Task::id).doesNotContain(task.id());
    }

    @Test
    void validatesQueriesAndMissingTasks() {
        assertThatThrownBy(() -> store.list(null, 1000, 0)).isInstanceOf(ForgeException.class);
        assertThatThrownBy(() -> store.require("missing")).isInstanceOf(ForgeException.class);
    }

    @Test
    void explicitScheduledTimeRemainsIdempotentAfterExecutionChangesAvailability() throws Exception {
        String key = UUID.randomUUID().toString();
        Submission request = new Submission(key, "echo", json.createObjectNode(), 0, 1, 1, 30,
                Instant.parse("2026-01-01T00:00:00.123456789Z"));
        Task task = store.submit(request);
        jdbc.update("UPDATE forge_task SET available_at = CURRENT_TIMESTAMP, status = 'SUCCEEDED' WHERE id = ?", task.id());
        assertThat(store.submit(request).id()).isEqualTo(task.id());
    }
}
