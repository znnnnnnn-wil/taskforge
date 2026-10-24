package io.taskforge.persistence;

import com.mybatisflex.annotation.Id;
import com.mybatisflex.annotation.Table;
import io.taskforge.DispatchMessage;
import io.taskforge.OutboxStore;
import java.time.Instant;

@Table(value = "forge_outbox", camelToUnderline = true)
public class OutboxRow {
    @Id public String eventId;
    public String taskId;
    public long expectedToken;
    public int priority;
    public Instant availableAt;
    public Instant nextAttemptAt;
    public String owner;
    public Instant leaseUntil;
    public int attempts;
    public Instant sentAt;
    public String error;
    public Instant createdAt;

    public OutboxStore.Event event() {
        return new OutboxStore.Event(new DispatchMessage(eventId, taskId, expectedToken), priority, owner, attempts);
    }
}
