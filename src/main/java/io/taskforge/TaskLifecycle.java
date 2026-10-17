package io.taskforge;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;

import static io.taskforge.TaskStore.timestamp;

@Service
public class TaskLifecycle {
    private final TaskStore store;
    private final TransactionTemplate tx;
    private final ForgeProperties config;

    public TaskLifecycle(TaskStore store, TransactionTemplate tx, ForgeProperties config) {
        this.store = store;
        this.tx = tx;
        this.config = config;
    }

    public Optional<Task> claim(String id, long expectedToken, String node) {
        return tx.execute(status -> {
            if (store.lock(id).isEmpty()) return Optional.empty();
            Instant now = store.now();
            int changed = store.jdbc().update("""
                    UPDATE forge_task SET status = 'RUNNING', attempts = attempts + 1,
                      token = token + 1, owner = ?, lease_until = ?, progress = 0,
                      error = NULL, updated_at = ?
                    WHERE id = ? AND token = ? AND status IN ('PENDING', 'RETRY')
                      AND available_at <= ? AND attempts < max_attempts
                    """, node, timestamp(now.plusSeconds(config.leaseSeconds())), timestamp(now),
                    id, expectedToken, timestamp(now));
            if (changed == 0) {
                return Optional.empty();
            }
            Task task = store.require(id);
            store.jdbc().update("""
                    INSERT INTO forge_attempt(task_id, number, node_id, token, status, started_at)
                    VALUES (?, ?, ?, ?, 'RUNNING', ?)
                    """, id, task.attempts(), node, task.token(), timestamp(now));
            return Optional.of(task);
        });
    }

    public boolean heartbeat(Task task, int progress) {
        if (progress < 0 || progress > 99) {
            throw new IllegalArgumentException("运行中进度必须为 0..99");
        }
        return Boolean.TRUE.equals(tx.execute(status -> {
            if (store.lock(task.id()).isEmpty()) return false;
            Instant now = store.now();
            return store.jdbc().update("""
                UPDATE forge_task SET lease_until = ?, progress = ?, updated_at = ?
                WHERE id = ? AND status = 'RUNNING' AND owner = ? AND token = ? AND lease_until > ?
                """, timestamp(now.plusSeconds(config.leaseSeconds())), progress, timestamp(now),
                task.id(), task.owner(), task.token(), timestamp(now)) == 1;
        }));
    }

    public boolean finish(Task task, String result, String error) {
        return Boolean.TRUE.equals(tx.execute(status -> {
            if (store.lock(task.id()).isEmpty()) return false;
            Instant now = store.now();
            boolean success = error == null;
            String next = success ? "SUCCEEDED" : task.attempts() < task.maxAttempts() ? "RETRY" : "FAILED";
            String message = abbreviate(error);
            int changed = store.jdbc().update("""
                    UPDATE forge_task SET status = ?, result = ?, error = ?, progress = ?,
                      owner = NULL, lease_until = NULL, available_at = ?, updated_at = ?
                    WHERE id = ? AND status = 'RUNNING' AND owner = ? AND token = ? AND lease_until > ?
                    """, next, result, message, success ? 100 : 0,
                    timestamp(now.plusSeconds(backoff(task))), timestamp(now), task.id(),
                    task.owner(), task.token(), timestamp(now));
            if (changed == 0) {
                return false;
            }
            closeAttempt(task, success ? "SUCCEEDED" : "FAILED", now, message);
            return true;
        }));
    }

    public Task cancel(String id) {
        return tx.execute(status -> {
            // Lock the row so attempt finalization cannot race cancellation bookkeeping.
            Task current = store.lock(id).orElseThrow(() -> new ForgeException(404, "任务不存在"));
            if (!current.terminal()) {
                Instant now = store.now();
                store.jdbc().update("""
                        UPDATE forge_task SET status = 'CANCELLED', token = token + 1,
                          owner = NULL, lease_until = NULL, updated_at = ? WHERE id = ?
                        """, timestamp(now), id);
                if (current.status().equals("RUNNING")) {
                    closeAttempt(current, "CANCELLED", now, "用户取消");
                }
            }
            return store.require(id);
        });
    }

    public int recoverExpired(int limit) {
        List<Task> expired = store.jdbc().query("""
                SELECT * FROM forge_task WHERE status = 'RUNNING' AND lease_until <= CURRENT_TIMESTAMP
                ORDER BY lease_until LIMIT ?
                """, store::map, limit);
        int recovered = 0;
        for (Task task : expired) {
            Boolean changed = tx.execute(status -> {
                if (store.lock(task.id()).isEmpty()) return false;
                Instant now = store.now();
                String next = task.attempts() < task.maxAttempts() ? "RETRY" : "FAILED";
                int count = store.jdbc().update("""
                        UPDATE forge_task SET status = ?, token = token + 1, owner = NULL,
                          lease_until = NULL, error = '执行租约过期', available_at = ?, updated_at = ?
                        WHERE id = ? AND status = 'RUNNING' AND token = ? AND lease_until <= ?
                        """, next, timestamp(now.plusSeconds(backoff(task))), timestamp(now),
                        task.id(), task.token(), timestamp(now));
                if (count == 1) {
                    closeAttempt(task, "LOST", now, "执行租约过期");
                }
                return count == 1;
            });
            if (Boolean.TRUE.equals(changed)) {
                recovered++;
            }
        }
        return recovered;
    }

    private void closeAttempt(Task task, String status, Instant now, String error) {
        store.jdbc().update("""
                UPDATE forge_attempt SET status = ?, finished_at = ?, error = ?
                WHERE task_id = ? AND number = ? AND token = ? AND status = 'RUNNING'
                """, status, timestamp(now), error, task.id(), task.attempts(), task.token());
    }

    static long backoff(Task task) {
        return Math.min(3600L, task.retryDelaySeconds() * (1L << Math.min(20, task.attempts() - 1)));
    }

    static String abbreviate(String error) {
        return error == null ? null : error.substring(0, Math.min(2000, error.length()));
    }
}
