package io.taskforge;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.sql.Connection;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

@SpringBootTest(properties = {
        "spring.datasource.url=jdbc:h2:mem:lease-lock;MODE=PostgreSQL;DB_CLOSE_DELAY=-1;LOCK_TIMEOUT=10000",
        "taskforge.worker-enabled=false"})
class LeaseContentionTest {
    enum Operation { FINISH, HEARTBEAT, CLAIM }
    @Autowired JdbcTemplate jdbc;
    @Autowired TaskStore store;
    @Autowired TaskLifecycle lifecycle;
    @Autowired ObjectMapper json;
    @Autowired TransactionTemplate tx;

    @ParameterizedTest
    @EnumSource(Operation.class)
    void samplesTimeAfterLockWait(Operation operation) throws Exception {
        checkLockWait(store, lifecycle, json, jdbc, operation);
    }

    @Test
    void clockAdvancesInsideTransaction() {
        checkClock(store, tx, jdbc);
    }

    static void checkClock(TaskStore store, TransactionTemplate tx, JdbcTemplate jdbc) {
        tx.executeWithoutResult(status -> {
            jdbc.queryForObject("SELECT CURRENT_TIMESTAMP", java.sql.Timestamp.class);
            Instant first = store.now();
            await().atMost(Duration.ofSeconds(2)).until(() -> store.now().isAfter(first.plusMillis(100)));
        });
    }

    static void checkLockWait(TaskStore store, TaskLifecycle lifecycle, ObjectMapper json, JdbcTemplate jdbc,
                              Operation operation) throws Exception {
        Task pending = store.submit(new Submission(UUID.randomUUID().toString(), "echo",
                json.createObjectNode(), 0, 2, 1, 60, null));
        Task task = operation == Operation.CLAIM ? pending
                : lifecycle.claim(pending.id(), 0, "before-lock").orElseThrow();
        Instant expiry = store.now().plusSeconds(1);
        if (operation != Operation.CLAIM) {
            jdbc.update("UPDATE forge_task SET lease_until=? WHERE id=?", TaskStore.timestamp(expiry), task.id());
        }
        Instant originalLease = store.require(task.id()).leaseUntil();
        var callers = Executors.newSingleThreadExecutor();
        CountDownLatch entered = new CountDownLatch(1);
        try (Connection lock = jdbc.getDataSource().getConnection()) {
            lock.setAutoCommit(false);
            try (var statement = lock.prepareStatement("SELECT id FROM forge_task WHERE id=? FOR UPDATE")) {
                statement.setString(1, task.id());
                statement.executeQuery().close();
            }
            var tx = new TransactionTemplate(new DataSourceTransactionManager(jdbc.getDataSource()));
            var result = callers.submit(() -> tx.execute(status -> {
                // Force transaction time to precede the wait on both database engines.
                jdbc.queryForObject("SELECT CURRENT_TIMESTAMP", java.sql.Timestamp.class);
                entered.countDown();
                return switch (operation) {
                    case FINISH -> lifecycle.finish(task, "{}", null);
                    case HEARTBEAT -> lifecycle.heartbeat(task, 10);
                    case CLAIM -> lifecycle.claim(task.id(), 0, "after-lock").isPresent();
                };
            }));
            assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
            await().atMost(Duration.ofSeconds(5)).until(() -> store.now().isAfter(expiry.plusMillis(100)));
            assertThat(result.isDone()).isFalse();
            Instant released = store.now();
            lock.commit();
            assertThat(result.get(5, TimeUnit.SECONDS)).isEqualTo(operation == Operation.CLAIM);
            Task after = store.require(task.id());
            if (operation == Operation.CLAIM) {
                assertThat(after.updatedAt()).isAfterOrEqualTo(released.minusMillis(1));
                assertThat(after.leaseUntil()).isAfter(after.updatedAt());
            } else {
                assertThat(after.status()).isEqualTo("RUNNING");
                assertThat(after.leaseUntil()).isEqualTo(originalLease);
                assertThat(store.attempts(task.id())).extracting(Attempt::status).containsExactly("RUNNING");
            }
        } finally {
            callers.shutdownNow();
            lifecycle.cancel(task.id());
        }
    }
}
