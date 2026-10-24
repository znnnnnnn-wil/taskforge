package io.taskforge.persistence;

import com.mybatisflex.annotation.Id;
import com.mybatisflex.annotation.Table;
import java.time.Instant;

@Table(value = "forge_schedule", camelToUnderline = true)
public class ScheduleRow {
    @Id public String id;
    public String name;
    public String cron;
    public String zone;
    public String taskJson;
    public Boolean enabled;
    public Instant nextRun;
    public Instant createdAt;
}
