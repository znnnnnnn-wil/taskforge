package io.taskforge;

import io.taskforge.persistence.ExecutionMapper;
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
    private final ExecutionMapper executions;

    public TaskLifecycle(TaskStore store, TransactionTemplate tx, ForgeProperties config, ExecutionMapper executions) {
        this.store = store;
        this.tx = tx;
        this.config = config;
        this.executions = executions;
    }

    public Optional<Task> claim(String id, long expectedToken, String node) {
        return tx.execute(status -> {
            if (store.lock(id).isEmpty()) return Optional.empty();
            Instant now = store.now();
            int changed = executions.claim(id, expectedToken, node,
                    timestamp(now.plusSeconds(config.leaseSeconds())), timestamp(now));
            if (changed == 0) {
                return Optional.empty();
            }
            Task task = store.require(id);
            executions.startAttempt(task, timestamp(now));
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
            return executions.heartbeat(task, timestamp(now.plusSeconds(config.leaseSeconds())),
                    progress, timestamp(now)) == 1;
        }));
    }

    public boolean finish(Task task, String result, String error) {
        return Boolean.TRUE.equals(tx.execute(status -> {
            if (store.lock(task.id()).isEmpty()) return false;
            Instant now = store.now();
            boolean success = error == null;
            String next = success ? "SUCCEEDED" : task.attempts() < task.maxAttempts() ? "RETRY" : "FAILED";
            String message = abbreviate(error);
            int changed = executions.finish(task, next, result, message, success ? 100 : 0,
                    timestamp(now.plusSeconds(backoff(task))), timestamp(now));
            if (changed == 0) {
                return false;
            }
            closeAttempt(task, success ? "SUCCEEDED" : "FAILED", now, message);
            if (next.equals("RETRY")) store.enqueue(store.require(task.id()));
            return true;
        }));
    }

    public Task cancel(String id) {
        return tx.execute(status -> {
            // Lock the row so attempt finalization cannot race cancellation bookkeeping.
            Task current = store.lock(id).orElseThrow(() -> new ForgeException(404, "任务不存在"));
            if (!current.terminal()) {
                Instant now = store.now();
                executions.cancel(id, timestamp(now));
                if (current.status().equals("RUNNING")) {
                    closeAttempt(current, "CANCELLED", now, "用户取消");
                }
            }
            return store.require(id);
        });
    }

    public int recoverExpired(int limit) {
        List<Task> expired = executions.expired(limit).stream().map(row -> row.view()).toList();
        int recovered = 0;
        for (Task task : expired) {
            Boolean changed = tx.execute(status -> {
                if (store.lock(task.id()).isEmpty()) return false;
                Instant now = store.now();
                String next = task.attempts() < task.maxAttempts() ? "RETRY" : "FAILED";
                int count = executions.recover(task, next,
                        timestamp(now.plusSeconds(backoff(task))), timestamp(now));
                if (count == 1) {
                    closeAttempt(task, "LOST", now, "执行租约过期");
                    if (next.equals("RETRY")) store.enqueue(store.require(task.id()));
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
        executions.closeAttempt(task, status, timestamp(now), error);
    }

    static long backoff(Task task) {
        return Math.min(3600L, task.retryDelaySeconds() * (1L << Math.min(20, task.attempts() - 1)));
    }

    static String abbreviate(String error) {
        return error == null ? null : error.substring(0, Math.min(2000, error.length()));
    }
}
