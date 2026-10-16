package io.taskforge;

import java.time.Instant;

public record TaskSchedule(String id, String name, String cron, String zone,
                           TaskSpec task, boolean enabled, Instant nextRun, Instant createdAt) {
}
