package io.taskforge;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.annotation.DirtiesContext;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@SpringBootTest(properties = "taskforge.worker-enabled=false")
class PostgresTest {
    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16");

    @DynamicPropertySource
    static void database(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
    }

    @Autowired JdbcTemplate jdbc;
    @Autowired TaskStore store;
    @Autowired TaskLifecycle lifecycle;
    @Autowired ScheduleStore schedules;
    @Autowired ObjectMapper json;
    @Autowired org.springframework.transaction.support.TransactionTemplate tx;

    @Test
    void idempotentReplayPreservesOuterTransactionAndOutbox() {
        var request = new Submission(UUID.randomUUID().toString(), "echo", json.createObjectNode(), 0, 1, 1, 30, null);
        Task existing = store.submit(request);
        tx.executeWithoutResult(status -> {
            assertThat(store.submit(request).id()).isEqualTo(existing.id());
            assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM forge_outbox WHERE task_id=?",
                    Integer.class, existing.id())).isEqualTo(1);
            store.submit(new Submission(UUID.randomUUID().toString(), "echo", json.createObjectNode(), 0, 1, 1, 30, null));
        });
    }

    @ParameterizedTest
    @EnumSource(LeaseContentionTest.Operation.class)
    void leaseTimeIsFreshAfterRowLockWait(LeaseContentionTest.Operation operation) throws Exception {
        LeaseContentionTest.checkLockWait(store, lifecycle, json, jdbc, operation);
    }

    @Test
    void databaseClockAdvancesInsideTransaction() {
        LeaseContentionTest.checkClock(store, tx, jdbc);
    }

    @Test
    void concurrentEnableAndFirePreserveOneDueInstance() throws Exception {
        TaskSchedule schedule = schedules.create(new ScheduleRequest("PG enable race", "*/5 * * * * *", "UTC",
                new TaskSpec("echo", json.createObjectNode(), 0, 1, 1, 30)));
        jdbc.update("UPDATE forge_schedule SET next_run=? WHERE id=?",
                TaskStore.timestamp(store.now().minusSeconds(60)), schedule.id());
        TaskSchedule due = schedules.require(schedule.id());
        var pool = Executors.newFixedThreadPool(8);
        try {
            var calls = new ArrayList<Callable<Boolean>>();
            for (int i = 0; i < 12; i++) {
                calls.add(() -> { schedules.enable(schedule.id(), true); return false; });
                calls.add(() -> schedules.fire(due));
            }
            int winners = 0;
            for (var result : pool.invokeAll(calls)) if (result.get()) winners++;
            assertThat(winners).isEqualTo(1);
            assertThat(schedules.instances(schedule.id(), 10, 0)).hasSize(1);
        } finally {
            pool.shutdownNow();
            schedules.enable(schedule.id(), false);
        }
    }

    @Test
    void concurrentIdempotentSubmissionAndClaimOnRealDatabase() throws Exception {
        Submission request = new Submission(UUID.randomUUID().toString(), "echo", json.createObjectNode(), 0, 3, 1, 60, null);
        var pool = Executors.newFixedThreadPool(8);
        try {
            var submissions = new ArrayList<Callable<String>>();
            for (int i = 0; i < 24; i++) submissions.add(() -> store.submit(request).id());
            var ids = new java.util.HashSet<String>();
            for (var submitted : pool.invokeAll(submissions)) ids.add(submitted.get());
            assertThat(ids).hasSize(1);
            String id = ids.iterator().next();
            var claims = new ArrayList<Callable<Boolean>>();
            for (int i = 0; i < 24; i++) {
                String node = "pg-node-" + i;
                claims.add(() -> lifecycle.claim(id, 0, node).isPresent());
            }
            int winners = 0;
            for (var claim : pool.invokeAll(claims)) if (claim.get()) winners++;
            assertThat(winners).isEqualTo(1);
            Task running = store.require(id);
            assertThat(lifecycle.finish(running, "{}", null)).isTrue();
            assertThat(store.attempts(id)).hasSize(1);
        } finally { pool.shutdownNow(); }
    }

    @Test
    void schedulerInstancesDoNotMaterializeDuplicateRuns() throws Exception {
        TaskSchedule schedule = schedules.create(new ScheduleRequest("PG trigger", "* * * * * *", "UTC",
                new TaskSpec("echo", json.createObjectNode(), 0, 1, 1, 30)));
        jdbc.update("UPDATE forge_schedule SET next_run = CURRENT_TIMESTAMP - INTERVAL '1 second' WHERE id = ?", schedule.id());
        TaskSchedule due = schedules.require(schedule.id());
        var pool = Executors.newFixedThreadPool(8);
        try {
            var triggers = new ArrayList<Callable<Boolean>>();
            for (int i = 0; i < 24; i++) triggers.add(() -> schedules.fire(due));
            int winners = 0;
            for (var result : pool.invokeAll(triggers)) if (result.get()) winners++;
            assertThat(winners).isEqualTo(1);
            assertThat(schedules.instances(schedule.id(), 20, 0)).hasSize(1);
        } finally { pool.shutdownNow(); }
    }

    @Test
    void crashedWorkerLeaseIsRecoveredAndStaleResultRejected() {
        Task task = store.submit(new Submission(UUID.randomUUID().toString(), "echo", json.createObjectNode(), 0, 2, 1, 30, null));
        Task old = lifecycle.claim(task.id(), 0, "crashed").orElseThrow();
        jdbc.update("UPDATE forge_task SET lease_until = CURRENT_TIMESTAMP - INTERVAL '1 second' WHERE id = ?", task.id());
        assertThat(lifecycle.recoverExpired(100)).isEqualTo(1);
        jdbc.update("UPDATE forge_task SET available_at = CURRENT_TIMESTAMP WHERE id = ?", task.id());
        Task replacement = lifecycle.claim(task.id(), store.require(task.id()).token(), "replacement").orElseThrow();
        assertThat(lifecycle.finish(old, "{}", null)).isFalse();
        assertThat(lifecycle.finish(replacement, "{}", null)).isTrue();
        assertThat(store.attempts(task.id())).extracting(Attempt::status).containsExactly("LOST", "SUCCEEDED");
    }
}
