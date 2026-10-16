package io.taskforge;

import java.time.Instant;
import java.util.List;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class NodeRegistry {
    private final TaskStore tasks;
    private final ForgeProperties config;
    private final WorkerEngine worker;

    public NodeRegistry(TaskStore tasks, ForgeProperties config, WorkerEngine worker) {
        this.tasks = tasks;
        this.config = config;
        this.worker = worker;
    }

    @Scheduled(fixedDelay = 2000)
    public void heartbeat() {
        if (update() == 0) {
            try {
                tasks.jdbc().update("""
                        INSERT INTO forge_node(node_id, worker_enabled, capacity, active, last_seen)
                        VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
                        """, config.nodeId(), config.workerEnabled(), config.concurrency(), worker.activeCount());
            } catch (DuplicateKeyException concurrentlyRegistered) {
                update();
            }
        }
    }

    private int update() {
        return tasks.jdbc().update("""
                UPDATE forge_node SET worker_enabled = ?, capacity = ?, active = ?, last_seen = CURRENT_TIMESTAMP
                WHERE node_id = ?
                """, config.workerEnabled(), config.concurrency(), worker.activeCount(), config.nodeId());
    }

    public List<Node> list() {
        Instant cutoff = tasks.now().minusSeconds(10);
        return tasks.jdbc().query("SELECT * FROM forge_node ORDER BY last_seen DESC LIMIT 100", (rs, row) -> {
            Instant seen = TaskStore.instant(rs, "last_seen");
            return new Node(rs.getString("node_id"), rs.getBoolean("worker_enabled"),
                    rs.getInt("capacity"), rs.getInt("active"), seen, seen.isAfter(cutoff));
        });
    }

    public record Node(String nodeId, boolean workerEnabled, int capacity, int active,
                       Instant lastSeen, boolean online) { }
}
