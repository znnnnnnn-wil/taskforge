package io.taskforge;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.transaction.support.TransactionTemplate;

import static io.taskforge.TaskStore.timestamp;

@Repository
public class OutboxStore {
    public record Event(DispatchMessage message, int priority, String owner, int attempts) { }
    private final JdbcTemplate jdbc;
    private final TransactionTemplate tx;
    private final boolean h2;
    @Value("${taskforge.rabbit.enabled:false}") private boolean enabled;

    public OutboxStore(JdbcTemplate jdbc, TransactionTemplate tx) {
        this.jdbc = jdbc;
        this.tx = tx;
        this.h2 = Boolean.TRUE.equals(jdbc.execute((ConnectionCallback<Boolean>) connection ->
                connection.getMetaData().getDatabaseProductName().equals("H2")));
    }

    /** Caller owns the task row in the same transaction as the state transition. */
    public void enqueue(Task task) {
        if (!TransactionSynchronizationManager.isActualTransactionActive()) {
            throw new IllegalStateException("任务与 Outbox 必须在同一事务中写入");
        }
        if (!List.of("PENDING", "RETRY").contains(task.status())) return;
        enqueue(task.id(), task.token(), task.priority(), task.availableAt());
    }

    private void enqueue(String id, long token, int priority, Instant availableAt) {
        jdbc.update("""
                INSERT INTO forge_outbox(event_id, task_id, expected_token, priority, available_at, next_attempt_at)
                SELECT ?, ?, ?, ?, ?, ? WHERE NOT EXISTS (
                  SELECT 1 FROM forge_outbox WHERE task_id = ? AND expected_token = ?)
                """, UUID.randomUUID().toString(), id, token, priority,
                timestamp(availableAt), timestamp(availableAt), id, token);
    }

    /** Reservation commits before any network I/O. Each reservation gets a new fencing owner. */
    public Optional<Event> reserve() {
        Instant now = now();
        List<String> candidates = jdbc.queryForList("""
                SELECT o.event_id FROM forge_outbox o JOIN forge_task t ON t.id = o.task_id
                WHERE o.sent_at IS NULL AND o.available_at <= ? AND o.next_attempt_at <= ?
                  AND (o.owner IS NULL OR o.lease_until <= ?) AND t.token = o.expected_token
                  AND t.status IN ('PENDING', 'RETRY') AND t.attempts < t.max_attempts
                  AND t.available_at <= ?
                ORDER BY o.priority DESC, o.available_at, o.event_id LIMIT 16
                """, String.class, timestamp(now), timestamp(now), timestamp(now), timestamp(now));
        for (String id : candidates) {
            Optional<Event> event = tx.execute(status -> {
                jdbc.queryForList("SELECT event_id FROM forge_outbox WHERE event_id = ? FOR UPDATE", String.class, id);
                Instant lockedAt = now();
                String owner = UUID.randomUUID().toString();
                int changed = jdbc.update("""
                        UPDATE forge_outbox SET owner = ?, lease_until = ?, attempts = attempts + 1
                        WHERE event_id = ? AND sent_at IS NULL AND next_attempt_at <= ? AND available_at <= ?
                          AND (owner IS NULL OR lease_until <= ?)
                        """, owner, timestamp(lockedAt.plusSeconds(30)), id, timestamp(lockedAt),
                        timestamp(lockedAt), timestamp(lockedAt));
                if (changed == 0) return Optional.empty();
                return jdbc.query("""
                        SELECT * FROM forge_outbox WHERE event_id = ?
                        """, (rs, row) -> new Event(new DispatchMessage(rs.getString("event_id"),
                        rs.getString("task_id"), rs.getLong("expected_token")), rs.getInt("priority"),
                        owner, rs.getInt("attempts")), id).stream().findFirst();
            });
            if (event != null && event.isPresent()) return event;
        }
        return Optional.empty();
    }

    public void sent(Event event) {
        jdbc.update("""
                UPDATE forge_outbox SET sent_at = ?, owner = NULL, lease_until = NULL, error = NULL
                WHERE event_id = ? AND owner = ?
                """, timestamp(now()), event.message().eventId(), event.owner());
    }

    public void failed(Event event, String error) {
        long delay = Math.min(60, 1L << Math.min(6, Math.max(0, event.attempts() - 1)));
        jdbc.update("""
                UPDATE forge_outbox SET owner = NULL, lease_until = NULL, next_attempt_at = ?, error = ?
                WHERE event_id = ? AND owner = ?
                """, timestamp(now().plusSeconds(delay)), TaskLifecycle.abbreviate(error),
                event.message().eventId(), event.owner());
    }

    @Scheduled(fixedDelay = 30000)
    public void repair() {
        if (!enabled) return;
        Instant cutoff = now().minusSeconds(60);
        List<String> ids = jdbc.queryForList("""
                SELECT t.id FROM forge_task t WHERE t.status IN ('PENDING', 'RETRY')
                  AND t.available_at <= ? AND t.attempts < t.max_attempts
                  AND NOT EXISTS (SELECT 1 FROM forge_outbox o WHERE o.task_id = t.id
                    AND o.expected_token = t.token AND (o.sent_at IS NULL OR o.sent_at > ?))
                ORDER BY t.available_at LIMIT 100
                """, String.class, timestamp(cutoff), timestamp(cutoff));
        for (String id : ids) repair(id, cutoff);
    }

    private void repair(String id, Instant cutoff) {
        tx.executeWithoutResult(status -> {
            List<RepairTask> tasks = jdbc.query("SELECT * FROM forge_task WHERE id = ? FOR UPDATE",
                    (rs, row) -> new RepairTask(rs.getString("status"), rs.getInt("priority"),
                            rs.getLong("token"), rs.getTimestamp("available_at").toInstant()), id);
            if (tasks.isEmpty()) return;
            RepairTask task = tasks.get(0);
            if (!List.of("PENDING", "RETRY").contains(task.status()) || task.availableAt().isAfter(cutoff)) return;
            enqueue(id, task.token(), task.priority(), task.availableAt());
            // Never steal an in-flight publish or alter a pending event's failure backoff.
            jdbc.update("""
                    UPDATE forge_outbox SET sent_at = NULL, next_attempt_at = ?, error = NULL
                    WHERE task_id = ? AND expected_token = ? AND owner IS NULL AND sent_at <= ?
                    """, timestamp(now()), id, task.token(), timestamp(cutoff));
        });
    }

    private record RepairTask(String status, int priority, long token, Instant availableAt) { }

    private Instant now() {
        return h2 ? Instant.now() : jdbc.queryForObject("SELECT clock_timestamp()", Timestamp.class).toInstant();
    }
}
