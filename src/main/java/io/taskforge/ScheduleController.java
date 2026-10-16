package io.taskforge;

import com.fasterxml.jackson.databind.node.ObjectNode;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import java.util.List;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/schedules")
public class ScheduleController {
    private final ScheduleStore schedules;
    private final TaskController views;

    public ScheduleController(ScheduleStore schedules, TaskController views) {
        this.schedules = schedules;
        this.views = views;
    }

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public TaskSchedule create(@Valid @RequestBody ScheduleRequest request) {
        return schedules.create(request);
    }

    @GetMapping
    public List<TaskSchedule> list(@RequestParam(defaultValue = "50") int limit,
                                   @RequestParam(defaultValue = "0") int offset) {
        return schedules.list(limit, offset);
    }

    @GetMapping("/{id}")
    public TaskSchedule get(@PathVariable String id) { return schedules.require(id); }

    @PutMapping("/{id}/enabled")
    public TaskSchedule enable(@PathVariable String id, @Valid @RequestBody Enabled request) {
        return schedules.enable(id, request.enabled());
    }

    @GetMapping("/{id}/tasks")
    public List<ObjectNode> instances(@PathVariable String id,
                                      @RequestParam(defaultValue = "50") int limit,
                                      @RequestParam(defaultValue = "0") int offset) {
        return schedules.instances(id, limit, offset).stream().map(views::view).toList();
    }

    public record Enabled(@NotNull Boolean enabled) { }
}
