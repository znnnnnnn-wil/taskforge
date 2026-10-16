package io.taskforge;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest(properties = {
        "spring.datasource.url=jdbc:h2:mem:schedule;MODE=PostgreSQL;DB_CLOSE_DELAY=-1",
        "taskforge.worker-enabled=false"})
class ScheduleTest {
    @Autowired ScheduleStore schedules;
    @Autowired TaskStore tasks;
    @Autowired ObjectMapper json;

    @Test
    void materializesOnceAndSkipsMissedIntervals() {
        TaskSchedule schedule = schedules.create(new ScheduleRequest("日报", "*/5 * * * * *", "Asia/Shanghai",
                new TaskSpec("echo", json.createObjectNode(), 0, 3, 1, 60)));
        tasks.jdbc().update("UPDATE forge_schedule SET next_run = ? WHERE id = ?",
                TaskStore.timestamp(Instant.now().minusSeconds(3600)), schedule.id());
        TaskSchedule due = schedules.require(schedule.id());
        assertThat(schedules.fire(due)).isTrue();
        assertThat(schedules.fire(due)).isFalse();
        assertThat(schedules.instances(schedule.id(), 10, 0)).hasSize(1);
        assertThat(schedules.require(schedule.id()).nextRun()).isAfter(Instant.now().minusSeconds(1));
        schedules.enable(schedule.id(), false);
        assertThat(schedules.require(schedule.id()).enabled()).isFalse();
        assertThat(schedules.fire(due)).isFalse();
    }

    @Test
    void validatesCronAndZone() {
        assertThatThrownBy(() -> ScheduleStore.next("invalid", "UTC", Instant.now())).isInstanceOf(ForgeException.class);
        assertThatThrownBy(() -> ScheduleStore.next("* * * * * *", "missing", Instant.now())).isInstanceOf(ForgeException.class);
    }
}
