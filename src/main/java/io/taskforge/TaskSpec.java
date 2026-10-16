package io.taskforge;

import com.fasterxml.jackson.databind.JsonNode;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;

public record TaskSpec(
        @NotBlank @Pattern(regexp = "[a-z][a-z0-9-]{0,39}") String handler,
        @NotNull JsonNode payload,
        @Min(0) @Max(9) int priority,
        @Min(1) @Max(20) int maxAttempts,
        @Min(1) @Max(3600) int retryDelaySeconds,
        @Min(1) @Max(86400) int timeoutSeconds) {
    Submission submission(String key, java.time.Instant availableAt) {
        return new Submission(key, handler, payload, priority, maxAttempts,
                retryDelaySeconds, timeoutSeconds, availableAt);
    }
}
