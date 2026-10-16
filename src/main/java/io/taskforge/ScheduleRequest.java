package io.taskforge;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;

public record ScheduleRequest(@NotBlank @Size(max = 100) String name,
                              @NotBlank @Size(max = 100) String cron,
                              @NotBlank @Size(max = 80) String zone,
                              @NotNull @Valid TaskSpec task) {
}
