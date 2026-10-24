package io.taskforge.persistence;

import com.mybatisflex.annotation.Id;
import com.mybatisflex.annotation.Table;
import io.taskforge.Task;
import java.time.Instant;

@Table(value = "forge_task", camelToUnderline = true)
public class TaskRow {
    @Id public String id;
    public String requestKey;
    public String handler;
    public String payload;
    public String status;
    public int priority;
    public int maxAttempts;
    public int attempts;
    public int retryDelaySeconds;
    public int timeoutSeconds;
    public Instant availableAt;
    public String owner;
    public long token;
    public Instant leaseUntil;
    public String result;
    public String error;
    public int progress;
    public Instant createdAt;
    public Instant updatedAt;
    public String requestedTime;
    public String scheduleId;

    public Task view() {
        return new Task(id, requestKey, handler, payload, status, priority, maxAttempts, attempts,
                retryDelaySeconds, timeoutSeconds, availableAt, owner, token, leaseUntil, result,
                error, progress, createdAt, updatedAt);
    }
}
