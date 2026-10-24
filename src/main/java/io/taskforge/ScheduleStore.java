package io.taskforge;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mybatisflex.core.query.QueryWrapper;
import io.taskforge.persistence.ScheduleMapper;
import io.taskforge.persistence.ScheduleRow;
import io.taskforge.persistence.TaskMapper;
import io.taskforge.persistence.TaskRow;
import java.time.Instant;
import java.time.ZoneId;
import java.util.List;
import java.util.UUID;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.scheduling.support.CronExpression;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;

import static io.taskforge.TaskStore.timestamp;

@Service
public class ScheduleStore {
    private final TaskStore tasks;
    private final ObjectMapper json;
    private final HandlerRegistry handlers;
    private final TransactionTemplate tx;
    private final ForgeProperties config;
    private final ScheduleMapper mapper;
    private final TaskMapper taskMapper;

    public ScheduleStore(TaskStore tasks, ObjectMapper json, HandlerRegistry handlers,
                         TransactionTemplate tx, ForgeProperties config,
                         ScheduleMapper mapper, TaskMapper taskMapper) {
        this.tasks = tasks;
        this.json = json;
        this.handlers = handlers;
        this.tx = tx;
        this.config = config;
        this.mapper = mapper;
        this.taskMapper = taskMapper;
    }

    public TaskSchedule create(ScheduleRequest request) {
        handlers.validate(request.task().handler(), request.task().payload());
        Instant next = next(request.cron(), request.zone(), tasks.now());
        String id = UUID.randomUUID().toString();
        try {
            var row = new ScheduleRow();
            row.id = id;
            row.name = request.name();
            row.cron = request.cron();
            row.zone = request.zone();
            row.taskJson = json.writeValueAsString(request.task());
            row.nextRun = timestamp(next).toInstant();
            mapper.insertSelectiveWithPk(row);
        } catch (JsonProcessingException impossible) {
            throw new IllegalStateException(impossible);
        }
        return require(id);
    }

    public TaskSchedule require(String id) {
        ScheduleRow row = mapper.selectOneById(id);
        if (row == null) throw new ForgeException(404, "周期任务不存在");
        return view(row);
    }

    public List<TaskSchedule> list(int limit, int offset) {
        if (limit < 1 || limit > 200 || offset < 0 || offset > 100000) {
            throw new ForgeException(400, "limit 范围 1..200，offset 范围 0..100000");
        }
        return mapper.selectListByQuery(QueryWrapper.create()
                .orderBy("created_at DESC", "id DESC").limit(offset, limit)).stream().map(this::view).toList();
    }

    public TaskSchedule enable(String id, boolean enabled) {
        return tx.execute(status -> {
            ScheduleRow row = mapper.lock(id);
            if (row == null) throw new ForgeException(404, "周期任务不存在");
            TaskSchedule schedule = view(row);
            if (schedule.enabled() == enabled) return schedule;
            if (enabled) {
                mapper.resume(id, timestamp(next(schedule.cron(), schedule.zone(), tasks.now())).toInstant());
            } else {
                mapper.pause(id);
            }
            return require(id);
        });
    }

    public List<Task> instances(String id, int limit, int offset) {
        require(id);
        if (limit < 1 || limit > 200 || offset < 0 || offset > 100000) {
            throw new ForgeException(400, "分页参数超出范围");
        }
        return taskMapper.selectListByQuery(QueryWrapper.create().where("schedule_id = ?", id)
                .orderBy("created_at DESC", "id DESC").limit(offset, limit)).stream().map(TaskRow::view).toList();
    }

    @Scheduled(fixedDelay = 1000)
    public void tick() {
        if (!config.workerEnabled()) return;
        List<ScheduleRow> due = mapper.selectListByQuery(QueryWrapper.create()
                .where("enabled = TRUE AND next_run <= CURRENT_TIMESTAMP").orderBy("next_run").limit(100));
        for (ScheduleRow row : due) fire(view(row));
    }

    public boolean fire(TaskSchedule schedule) {
        return Boolean.TRUE.equals(tx.execute(status -> {
            Instant now = tasks.now();
            if (schedule.nextRun().isAfter(now)) return false;
            Instant next = next(schedule.cron(), schedule.zone(), now);
            int changed = mapper.advance(schedule.id(), timestamp(schedule.nextRun()).toInstant(),
                    timestamp(next).toInstant());
            if (changed == 0) return false;
            String id = UUID.randomUUID().toString();
            String key = "schedule:" + schedule.id() + ":" + schedule.nextRun().toEpochMilli();
            tasks.insert(id, schedule.task().submission(key, schedule.nextRun()));
            taskMapper.assignSchedule(id, schedule.id());
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

    private TaskSchedule view(ScheduleRow row) {
        try {
            return new TaskSchedule(row.id, row.name, row.cron, row.zone,
                    json.readValue(row.taskJson, TaskSpec.class), Boolean.TRUE.equals(row.enabled),
                    row.nextRun, row.createdAt);
        } catch (JsonProcessingException corrupt) {
            throw new IllegalStateException("持久化的周期任务配置损坏", corrupt);
        }
    }
}
