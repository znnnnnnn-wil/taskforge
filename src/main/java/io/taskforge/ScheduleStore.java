package io.taskforge;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.ZoneId;
import java.util.List;
import java.util.UUID;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.scheduling.support.CronExpression;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;

import static io.taskforge.TaskStore.instant;
import static io.taskforge.TaskStore.timestamp;

@Service
public class ScheduleStore {
    private final TaskStore tasks;
    private final ObjectMapper json;
    private final HandlerRegistry handlers;
    private final TransactionTemplate tx;
    private final ForgeProperties config;

    public ScheduleStore(TaskStore tasks, ObjectMapper json, HandlerRegistry handlers,
                         TransactionTemplate tx, ForgeProperties config) {
        this.tasks = tasks;
        this.json = json;
        this.handlers = handlers;
        this.tx = tx;
        this.config = config;
    }

    public TaskSchedule create(ScheduleRequest request) {
        handlers.validate(request.task().handler(), request.task().payload());
        Instant next = next(request.cron(), request.zone(), tasks.now());
        String id = UUID.randomUUID().toString();
        try {
            tasks.jdbc().update("""
                    INSERT INTO forge_schedule(id, name, cron, zone, task_json, next_run)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """, id, request.name(), request.cron(), request.zone(),
                    json.writeValueAsString(request.task()), timestamp(next));
        } catch (JsonProcessingException impossible) {
            throw new IllegalStateException(impossible);
        }
        return require(id);
    }

    public TaskSchedule require(String id) {
        List<TaskSchedule> found = tasks.jdbc().query("SELECT * FROM forge_schedule WHERE id = ?", this::map, id);
        if (found.isEmpty()) throw new ForgeException(404, "周期任务不存在");
        return found.get(0);
    }

    public List<TaskSchedule> list(int limit, int offset) {
        if (limit < 1 || limit > 200 || offset < 0 || offset > 100000) {
            throw new ForgeException(400, "limit 范围 1..200，offset 范围 0..100000");
        }
        return tasks.jdbc().query("SELECT * FROM forge_schedule ORDER BY created_at DESC, id DESC LIMIT ? OFFSET ?",
                this::map, limit, offset);
    }

    public TaskSchedule enable(String id, boolean enabled) {
        return tx.execute(status -> {
            TaskSchedule schedule = require(id);
            tasks.jdbc().update("UPDATE forge_schedule SET enabled = ?, next_run = ? WHERE id = ?",
                    enabled, timestamp(next(schedule.cron(), schedule.zone(), tasks.now())), id);
            return require(id);
        });
    }

    public List<Task> instances(String id, int limit, int offset) {
        require(id);
        if (limit < 1 || limit > 200 || offset < 0 || offset > 100000) {
            throw new ForgeException(400, "分页参数超出范围");
        }
        return tasks.jdbc().query("""
                SELECT * FROM forge_task WHERE schedule_id = ?
                ORDER BY created_at DESC, id DESC LIMIT ? OFFSET ?
                """, tasks::map, id, limit, offset);
    }

    @Scheduled(fixedDelay = 1000)
    public void tick() {
        if (!config.workerEnabled()) return;
        List<TaskSchedule> due = tasks.jdbc().query("""
                SELECT * FROM forge_schedule WHERE enabled = TRUE AND next_run <= CURRENT_TIMESTAMP
                ORDER BY next_run LIMIT 100
                """, this::map);
        for (TaskSchedule schedule : due) fire(schedule);
    }

    public boolean fire(TaskSchedule schedule) {
        return Boolean.TRUE.equals(tx.execute(status -> {
            Instant now = tasks.now();
            if (schedule.nextRun().isAfter(now)) return false;
            Instant next = next(schedule.cron(), schedule.zone(), now);
            int changed = tasks.jdbc().update("""
                    UPDATE forge_schedule SET next_run = ?
                    WHERE id = ? AND enabled = TRUE AND next_run = ?
                    """, timestamp(next), schedule.id(), timestamp(schedule.nextRun()));
            if (changed == 0) return false;
            String id = UUID.randomUUID().toString();
            String key = "schedule:" + schedule.id() + ":" + schedule.nextRun().toEpochMilli();
            tasks.insert(id, schedule.task().submission(key, schedule.nextRun()));
            tasks.jdbc().update("UPDATE forge_task SET schedule_id = ? WHERE id = ?", schedule.id(), id);
            return true;
        }));
    }

    static Instant next(String expression, String zone, Instant after) {
        try {
            var value = CronExpression.parse(expression).next(after.atZone(ZoneId.of(zone)));
            if (value == null) throw new IllegalArgumentException("表达式没有下一次触发时间");
            return value.toInstant();
        } catch (IllegalArgumentException | java.time.DateTimeException invalid) {
            throw new ForgeException(400, "无效的六字段 cron 或时区: " + invalid.getMessage());
        }
    }

    private TaskSchedule map(ResultSet rs, int row) throws SQLException {
        try {
            return new TaskSchedule(rs.getString("id"), rs.getString("name"), rs.getString("cron"),
                    rs.getString("zone"), json.readValue(rs.getString("task_json"), TaskSpec.class),
                    rs.getBoolean("enabled"), instant(rs, "next_run"), instant(rs, "created_at"));
        } catch (JsonProcessingException corrupt) {
            throw new SQLException("持久化的周期任务配置损坏", corrupt);
        }
    }
}
