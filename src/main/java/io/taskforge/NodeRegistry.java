package io.taskforge;

import com.mybatisflex.core.query.QueryWrapper;
import io.taskforge.persistence.NodeMapper;
import io.taskforge.persistence.NodeRow;
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
    private final NodeMapper mapper;

    public NodeRegistry(TaskStore tasks, ForgeProperties config, WorkerEngine worker, NodeMapper mapper) {
        this.tasks = tasks;
        this.config = config;
        this.worker = worker;
        this.mapper = mapper;
    }

    @Scheduled(fixedDelay = 2000)
    public void heartbeat() {
        if (update() == 0) {
            try {
                var row = new NodeRow();
                row.nodeId = config.nodeId();
                row.workerEnabled = config.workerEnabled();
                row.capacity = config.concurrency();
                row.active = worker.activeCount();
                mapper.insertSelectiveWithPk(row);
            } catch (DuplicateKeyException concurrentlyRegistered) {
                update();
            }
        }
    }

    private int update() {
        return mapper.heartbeat(config.nodeId(), config.workerEnabled(), config.concurrency(), worker.activeCount());
    }

    public List<Node> list() {
        Instant cutoff = tasks.now().minusSeconds(10);
        return mapper.selectListByQuery(QueryWrapper.create().orderBy("last_seen DESC").limit(100)).stream()
                .map(row -> new Node(row.nodeId, row.workerEnabled, row.capacity, row.active,
                        row.lastSeen, row.lastSeen.isAfter(cutoff))).toList();
    }

    public record Node(String nodeId, boolean workerEnabled, int capacity, int active,
                       Instant lastSeen, boolean online) { }
}
