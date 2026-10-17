package io.taskforge;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.stereotype.Repository;

@Repository
public class TaskStore {
    final JdbcTemplate jdbc;
    private final ObjectMapper json;
    private final boolean embeddedH2;

    public TaskStore(JdbcTemplate jdbc, ObjectMapper json) {
        this.jdbc = jdbc;
        this.json = json;
        this.embeddedH2 = Boolean.TRUE.equals(jdbc.execute((ConnectionCallback<Boolean>) connection -> {
            String database = connection.getMetaData().getDatabaseProductName();
            if (database.equals("H2")) {
                String url = connection.getMetaData().getURL();
                if (url.startsWith("jdbc:h2:tcp:") || url.startsWith("jdbc:h2:ssl:")) {
                    throw new IllegalArgumentException("H2 仅支持嵌入式开发；多节点请使用 PostgreSQL");
                }
                return true;
            }
            if (!database.equals("PostgreSQL")) throw new IllegalArgumentException("仅支持 H2 和 PostgreSQL");
            return false;
        }));
    }

    public JdbcTemplate jdbc() { return jdbc; }

    public Task submit(Submission request) {
        if (!request.payload().isObject() || request.payload().toString().length() > 32768) {
            throw new ForgeException(400, "payload 必须为不超过 32768 字符的 JSON 对象");
        }
        String id = UUID.randomUUID().toString();
        try {
            insert(id, request);
        } catch (DuplicateKeyException duplicate) {
            Task existing = byRequestKey(request.requestKey());
            if (!sameRequest(existing, request)) {
                throw new ForgeException(409, "requestKey 已被不同内容的任务使用");
            }
            return existing;
        }
        return require(id);
    }

    void insert(String id, Submission request) {
        jdbc.update("""
                INSERT INTO forge_task(id, request_key, handler, payload, status, priority,
                  max_attempts, retry_delay_seconds, timeout_seconds, available_at, requested_time)
                VALUES (?, ?, ?, ?, 'PENDING', ?, ?, ?, ?, COALESCE(?, CURRENT_TIMESTAMP), ?)
                """, id, request.requestKey(), request.handler(), request.payload().toString(),
                request.priority(), request.maxAttempts(), request.retryDelaySeconds(),
                request.timeoutSeconds(), timestamp(request.availableAt()),
                request.availableAt() == null ? null : request.availableAt().toString());
    }

    public Task require(String id) {
        List<Task> tasks = jdbc.query("SELECT * FROM forge_task WHERE id = ?", this::map, id);
        if (tasks.isEmpty()) {
            throw new ForgeException(404, "任务不存在");
        }
        return tasks.get(0);
    }

    java.util.Optional<Task> lock(String id) {
        return jdbc.query("SELECT * FROM forge_task WHERE id = ? FOR UPDATE", this::map, id).stream().findFirst();
    }

    private Task byRequestKey(String key) {
        return jdbc.queryForObject("SELECT * FROM forge_task WHERE request_key = ?", this::map, key);
    }

    private boolean sameRequest(Task old, Submission request) {
        try {
            return old.handler().equals(request.handler())
                    && json.readTree(old.payload()).equals(request.payload())
                    && old.priority() == request.priority()
                    && old.maxAttempts() == request.maxAttempts()
                    && old.retryDelaySeconds() == request.retryDelaySeconds()
                    && old.timeoutSeconds() == request.timeoutSeconds()
                    && java.util.Objects.equals(request.availableAt() == null ? null : request.availableAt().toString(),
                        jdbc.queryForObject("SELECT requested_time FROM forge_task WHERE id = ?", String.class, old.id()));
        } catch (JsonProcessingException impossible) {
            throw new IllegalStateException("持久化的任务 JSON 损坏", impossible);
        }
    }

    public List<Task> list(String status, int limit, int offset) {
        if (limit < 1 || limit > 200 || offset < 0 || offset > 100000) {
            throw new ForgeException(400, "limit 范围 1..200，offset 范围 0..100000");
        }
        if (status == null) {
            return jdbc.query("SELECT * FROM forge_task ORDER BY created_at DESC, id DESC LIMIT ? OFFSET ?",
                    this::map, limit, offset);
        }
        if (!List.of("PENDING", "RUNNING", "RETRY", "SUCCEEDED", "FAILED", "CANCELLED").contains(status)) {
            throw new ForgeException(400, "未知任务状态");
        }
        return jdbc.query("""
                SELECT * FROM forge_task WHERE status = ?
                ORDER BY created_at DESC, id DESC LIMIT ? OFFSET ?
                """, this::map, status, limit, offset);
    }

    public List<Attempt> attempts(String id) {
        require(id);
        return jdbc.query("SELECT * FROM forge_attempt WHERE task_id = ? ORDER BY number", (rs, row) ->
                new Attempt(rs.getString("task_id"), rs.getInt("number"), rs.getString("node_id"),
                        rs.getLong("token"), rs.getString("status"), instant(rs, "started_at"),
                        instant(rs, "finished_at"), rs.getString("error")), id);
    }

    public List<Map<String, Object>> counts() {
        return jdbc.queryForList("SELECT status, COUNT(*) AS total FROM forge_task GROUP BY status");
    }

    Instant now() {
        // Embedded H2 shares this JVM's clock. PostgreSQL CURRENT_TIMESTAMP is frozen at transaction start.
        return embeddedH2 ? Instant.now()
                : jdbc.queryForObject("SELECT clock_timestamp()", Timestamp.class).toInstant();
    }

    List<Task> due(int limit) {
        return jdbc.query("""
                SELECT * FROM forge_task WHERE status IN ('PENDING', 'RETRY')
                  AND available_at <= CURRENT_TIMESTAMP AND attempts < max_attempts
                ORDER BY priority DESC, available_at, id LIMIT ?
                """, this::map, limit);
    }

    Task map(ResultSet rs, int row) throws SQLException {
        return new Task(rs.getString("id"), rs.getString("request_key"), rs.getString("handler"),
                rs.getString("payload"), rs.getString("status"), rs.getInt("priority"),
                rs.getInt("max_attempts"), rs.getInt("attempts"), rs.getInt("retry_delay_seconds"),
                rs.getInt("timeout_seconds"), instant(rs, "available_at"), rs.getString("owner"),
                rs.getLong("token"), instant(rs, "lease_until"), rs.getString("result"),
                rs.getString("error"), rs.getInt("progress"), instant(rs, "created_at"),
                instant(rs, "updated_at"));
    }

    static Timestamp timestamp(Instant instant) {
        return instant == null ? null : Timestamp.from(instant);
    }

    static Instant instant(ResultSet rs, String name) throws SQLException {
        Timestamp value = rs.getTimestamp(name);
        return value == null ? null : value.toInstant();
    }
}
