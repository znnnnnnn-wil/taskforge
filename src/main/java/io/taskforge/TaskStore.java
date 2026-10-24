package io.taskforge;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mybatisflex.core.query.QueryWrapper;
import io.taskforge.persistence.TaskMapper;
import io.taskforge.persistence.TaskRow;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;

@Repository
public class TaskStore {
    private final TaskMapper mapper;
    private final ObjectMapper json;
    private final OutboxStore outbox;
    private final TransactionTemplate tx;
    private final DatabaseClock clock;

    public TaskStore(TaskMapper mapper, ObjectMapper json, OutboxStore outbox,
                     TransactionTemplate tx, DatabaseClock clock) {
        this.mapper = mapper;
        this.json = json;
        this.outbox = outbox;
        this.clock = clock;
        this.tx = new TransactionTemplate(tx.getTransactionManager());
        this.tx.setPropagationBehavior(TransactionDefinition.PROPAGATION_NESTED);
    }

    public Task submit(Submission request) {
        if (!request.payload().isObject() || request.payload().toString().length() > 32768) {
            throw new ForgeException(400, "payload 必须为不超过 32768 字符的 JSON 对象");
        }
        String id = UUID.randomUUID().toString();
        try {
            tx.executeWithoutResult(status -> insert(id, request));
        } catch (DuplicateKeyException duplicate) {
            TaskRow existing = mapper.selectOneByQuery(QueryWrapper.create().where("request_key = ?", request.requestKey()));
            if (existing == null || !sameRequest(existing, request)) {
                throw new ForgeException(409, "requestKey 已被不同内容的任务使用");
            }
            return existing.view();
        }
        return require(id);
    }

    void insert(String id, Submission request) {
        TaskRow row = new TaskRow();
        row.id = id;
        row.requestKey = request.requestKey();
        row.handler = request.handler();
        row.payload = request.payload().toString();
        row.status = "PENDING";
        row.priority = request.priority();
        row.maxAttempts = request.maxAttempts();
        row.retryDelaySeconds = request.retryDelaySeconds();
        row.timeoutSeconds = request.timeoutSeconds();
        row.availableAt = timestamp(request.availableAt() == null ? now() : request.availableAt()).toInstant();
        row.requestedTime = request.availableAt() == null ? null : request.availableAt().toString();
        mapper.insertSelectiveWithPk(row);
        outbox.enqueue(require(id));
    }

    void enqueue(Task task) { outbox.enqueue(task); }

    public Task require(String id) {
        TaskRow row = mapper.selectOneById(id);
        if (row == null) throw new ForgeException(404, "任务不存在");
        return row.view();
    }

    Optional<Task> lock(String id) {
        return Optional.ofNullable(mapper.lock(id)).map(TaskRow::view);
    }

    private boolean sameRequest(TaskRow old, Submission request) {
        try {
            return old.handler.equals(request.handler())
                    && json.readTree(old.payload).equals(request.payload())
                    && old.priority == request.priority()
                    && old.maxAttempts == request.maxAttempts()
                    && old.retryDelaySeconds == request.retryDelaySeconds()
                    && old.timeoutSeconds == request.timeoutSeconds()
                    && java.util.Objects.equals(request.availableAt() == null ? null : request.availableAt().toString(),
                            old.requestedTime);
        } catch (JsonProcessingException impossible) {
            throw new IllegalStateException("持久化的任务 JSON 损坏", impossible);
        }
    }

    public List<Task> list(String status, int limit, int offset) {
        if (limit < 1 || limit > 200 || offset < 0 || offset > 100000) {
            throw new ForgeException(400, "limit 范围 1..200，offset 范围 0..100000");
        }
        if (status != null && !List.of("PENDING", "RUNNING", "RETRY", "SUCCEEDED", "FAILED", "CANCELLED").contains(status)) {
            throw new ForgeException(400, "未知任务状态");
        }
        QueryWrapper query = QueryWrapper.create().orderBy("created_at DESC, id DESC").limit(offset, limit);
        if (status != null) query.where("status = ?", status);
        return mapper.selectListByQuery(query).stream().map(TaskRow::view).toList();
    }

    public List<Attempt> attempts(String id) {
        require(id);
        return mapper.attempts(id).stream().map(row -> row.view()).toList();
    }

    public List<Map<String, Object>> counts() { return mapper.counts(); }

    Instant now() { return clock.now(); }

    List<Task> due(int limit) { return mapper.due(limit).stream().map(TaskRow::view).toList(); }

    static Timestamp timestamp(Instant instant) {
        return instant == null ? null : Timestamp.from(instant.truncatedTo(java.time.temporal.ChronoUnit.MICROS));
    }
}
