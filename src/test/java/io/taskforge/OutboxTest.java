package io.taskforge;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
import java.util.ArrayList;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.transaction.support.TransactionTemplate;

@SpringBootTest(properties = {
        "spring.datasource.url=jdbc:h2:mem:outbox;MODE=PostgreSQL;DB_CLOSE_DELAY=-1",
        "taskforge.worker-enabled=false"})
class OutboxTest {
    @Autowired JdbcTemplate jdbc;
    @Autowired TaskStore tasks;
    @Autowired TaskLifecycle lifecycle;
    @Autowired ScheduleStore schedules;
    @Autowired OutboxStore outbox;
    @Autowired TransactionTemplate tx;
    @Autowired ObjectMapper json;

    @BeforeEach
    void clean() {
        jdbc.update("DELETE FROM forge_outbox");
        jdbc.update("DELETE FROM forge_attempt");
        jdbc.update("DELETE FROM forge_task");
        jdbc.update("DELETE FROM forge_schedule");
    }

    Submission request(Instant availableAt) {
        return new Submission(UUID.randomUUID().toString(), "echo", json.createObjectNode(), 0, 3, 1, 30, availableAt);
    }

    int count() { return jdbc.queryForObject("SELECT COUNT(*) FROM forge_outbox", Integer.class); }

    @Test
    void submissionAndOutboxCommitAndRollbackTogether() {
        Submission request = request(null);
        Task first = tasks.submit(request);
        assertThat(tasks.submit(request).id()).isEqualTo(first.id());
        assertThat(count()).isEqualTo(1);
        assertThatThrownBy(() -> tx.executeWithoutResult(status -> {
            tasks.submit(request(null));
            throw new IllegalStateException("rollback");
        })).isInstanceOf(IllegalStateException.class);
        assertThat(count()).isEqualTo(1);
        assertThat(tasks.list(null, 10, 0)).hasSize(1);
    }

    @Test
    void futureTaskWaitsUntilItsDueTime() {
        Task task = tasks.submit(request(Instant.now().plusSeconds(3600)));
        assertThat(count()).isEqualTo(1);
        assertThat(outbox.reserve()).isEmpty();
        jdbc.update("UPDATE forge_task SET available_at=DATEADD('SECOND', -1, CURRENT_TIMESTAMP) WHERE id=?", task.id());
        jdbc.update("UPDATE forge_outbox SET available_at=DATEADD('SECOND', -1, CURRENT_TIMESTAMP), "
                + "next_attempt_at=DATEADD('SECOND', -1, CURRENT_TIMESTAMP)");
        assertThat(outbox.reserve()).isPresent();
    }

    @Test
    void idempotentReplayDoesNotAbortAnOuterTransaction() {
        Submission original = request(null);
        Task existing = tasks.submit(original);
        tx.executeWithoutResult(status -> {
            assertThat(tasks.submit(original).id()).isEqualTo(existing.id());
            tasks.submit(request(null));
        });
        assertThat(count()).isEqualTo(2);
        assertThatThrownBy(() -> tx.executeWithoutResult(status -> {
            tasks.submit(original);
            tasks.submit(request(null));
            throw new IllegalStateException("outer rollback");
        })).isInstanceOf(IllegalStateException.class);
        assertThat(count()).isEqualTo(2);
        assertThat(tasks.list(null, 10, 0)).hasSize(2);
    }

    @Test
    void concurrentPublishReservationsHaveOneWinner() throws Exception {
        tasks.submit(request(null));
        var pool = Executors.newFixedThreadPool(8);
        try {
            var calls = new ArrayList<Callable<Boolean>>();
            for (int i = 0; i < 24; i++) calls.add(() -> outbox.reserve().isPresent());
            int winners = 0;
            for (var result : pool.invokeAll(calls)) if (result.get()) winners++;
            assertThat(winners).isEqualTo(1);
        } finally { pool.shutdownNow(); }
    }

    @Test
    void immediateSubmissionsCanBeClaimedWithoutWaitingForTimestampRounding() {
        for (int i = 0; i < 50; i++) {
            Task task = tasks.submit(request(null));
            assertThat(lifecycle.claim(task.id(), 0, "immediate")).isPresent();
        }
    }

    @Test
    void failedPublishBacksOffAndExpiredOwnerCannotConfirmReplacement() {
        tasks.submit(request(null));
        OutboxStore.Event first = outbox.reserve().orElseThrow();
        outbox.failed(first, "broker unavailable");
        assertThat(outbox.reserve()).isEmpty();
        jdbc.update("UPDATE forge_outbox SET next_attempt_at=DATEADD('SECOND', -1, CURRENT_TIMESTAMP)");
        OutboxStore.Event second = outbox.reserve().orElseThrow();
        assertThat(second.attempts()).isEqualTo(2);
        assertThat(second.owner()).isNotEqualTo(first.owner());
        outbox.sent(first);
        assertThat(jdbc.queryForObject("SELECT sent_at FROM forge_outbox", java.sql.Timestamp.class)).isNull();
        jdbc.update("UPDATE forge_outbox SET lease_until=DATEADD('SECOND', -1, CURRENT_TIMESTAMP)");
        OutboxStore.Event replacement = outbox.reserve().orElseThrow();
        outbox.failed(second, "late failure");
        outbox.sent(replacement);
        assertThat(outbox.reserve()).isEmpty();
        assertThat(jdbc.queryForObject("SELECT attempts FROM forge_outbox", Integer.class)).isEqualTo(3);
    }

    @Test
    void retryAndLeaseRecoveryCreateNewGenerations() {
        Task task = tasks.submit(request(null));
        Task first = lifecycle.claim(task.id(), 0, "first").orElseThrow();
        assertThat(lifecycle.finish(first, null, "retry")).isTrue();
        assertThat(count()).isEqualTo(2);
        jdbc.update("UPDATE forge_task SET available_at=DATEADD('SECOND', -1, CURRENT_TIMESTAMP) WHERE id=?", task.id());
        Task second = lifecycle.claim(task.id(), first.token(), "second").orElseThrow();
        jdbc.update("UPDATE forge_task SET lease_until=DATEADD('SECOND', -1, CURRENT_TIMESTAMP) WHERE id=?", task.id());
        assertThat(lifecycle.recoverExpired(100)).isEqualTo(1);
        assertThat(count()).isEqualTo(3);
        assertThat(jdbc.queryForList("SELECT expected_token FROM forge_outbox ORDER BY expected_token", Long.class))
                .containsExactly(0L, first.token(), second.token() + 1);
    }

    @Test
    void cronInstanceAndNotificationAreCreatedOnce() {
        TaskSchedule schedule = schedules.create(new ScheduleRequest("outbox cron", "*/5 * * * * *", "UTC",
                new TaskSpec("echo", json.createObjectNode(), 0, 2, 1, 30)));
        jdbc.update("UPDATE forge_schedule SET next_run=DATEADD('SECOND', -1, CURRENT_TIMESTAMP) WHERE id=?", schedule.id());
        TaskSchedule due = schedules.require(schedule.id());
        assertThat(schedules.fire(due)).isTrue();
        assertThat(schedules.fire(due)).isFalse();
        assertThat(count()).isEqualTo(1);
    }

    @Test
    void repairRestoresMissingAndOldSentNotificationsWithoutStealingPublishLease() {
        tasks.submit(request(null));
        jdbc.update("UPDATE forge_task SET available_at=DATEADD('SECOND', -120, CURRENT_TIMESTAMP)");
        OutboxStore.Event first = outbox.reserve().orElseThrow();
        ReflectionTestUtils.setField(outbox, "enabled", true);
        try {
            outbox.repair();
            assertThat(outbox.reserve()).isEmpty();
            outbox.sent(first);
            jdbc.update("UPDATE forge_outbox SET sent_at=DATEADD('SECOND', -120, CURRENT_TIMESTAMP)");
            outbox.repair();
            await().atMost(java.time.Duration.ofSeconds(2)).until(() -> outbox.reserve().isPresent());
            jdbc.update("DELETE FROM forge_outbox");
            outbox.repair();
            assertThat(count()).isEqualTo(1);
            await().atMost(java.time.Duration.ofSeconds(2)).until(() -> outbox.reserve().isPresent());
        } finally { ReflectionTestUtils.setField(outbox, "enabled", false); }
    }
}
