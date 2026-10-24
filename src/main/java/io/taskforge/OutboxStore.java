package io.taskforge;

import io.taskforge.persistence.OutboxMapper;
import io.taskforge.persistence.OutboxRow;
import io.taskforge.persistence.TaskRow;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.transaction.support.TransactionTemplate;

import static io.taskforge.TaskStore.timestamp;

@Repository
public class OutboxStore {
    public record Event(DispatchMessage message, int priority, String owner, int attempts) { }
    private final OutboxMapper mapper;
    private final TransactionTemplate tx;
    private final DatabaseClock clock;
    @Value("${taskforge.rabbit.enabled:false}") private boolean enabled;

    public OutboxStore(OutboxMapper mapper, TransactionTemplate tx, DatabaseClock clock) {
        this.mapper = mapper;
        this.tx = tx;
        this.clock = clock;
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
        mapper.enqueue(UUID.randomUUID().toString(), id, token, priority, timestamp(availableAt));
    }

    /** Reservation commits before any network I/O. Each reservation gets a new fencing owner. */
    public Optional<Event> reserve() {
        List<String> candidates = mapper.candidates(timestamp(clock.now()));
        for (String id : candidates) {
            Optional<Event> event = tx.execute(status -> {
                mapper.lock(id);
                Instant lockedAt = clock.now();
                String owner = UUID.randomUUID().toString();
                int changed = mapper.reserve(id, owner, timestamp(lockedAt.plusSeconds(30)), timestamp(lockedAt));
                if (changed == 0) return Optional.empty();
                return Optional.ofNullable(mapper.selectOneById(id)).map(OutboxRow::event);
            });
            if (event != null && event.isPresent()) return event;
        }
        return Optional.empty();
    }

    public void sent(Event event) {
        mapper.sent(event.message().eventId(), event.owner(), timestamp(clock.now()));
    }

    public void failed(Event event, String error) {
        long delay = Math.min(60, 1L << Math.min(6, Math.max(0, event.attempts() - 1)));
        mapper.failed(event.message().eventId(), event.owner(), timestamp(clock.now().plusSeconds(delay)),
                TaskLifecycle.abbreviate(error));
    }

    @Scheduled(fixedDelay = 30000)
    public void repair() {
        if (!enabled) return;
        Instant cutoff = clock.now().minusSeconds(60);
        for (String id : mapper.repairCandidates(timestamp(cutoff))) repair(id, cutoff);
    }

    private void repair(String id, Instant cutoff) {
        tx.executeWithoutResult(status -> {
            TaskRow task = mapper.lockTask(id);
            if (task == null) return;
            if (!List.of("PENDING", "RETRY").contains(task.status) || task.availableAt.isAfter(cutoff)) return;
            enqueue(id, task.token, task.priority, task.availableAt);
            // Never steal an in-flight publish or alter a pending event's failure backoff.
            mapper.repair(id, task.token, timestamp(clock.now()), timestamp(cutoff));
        });
    }
}
