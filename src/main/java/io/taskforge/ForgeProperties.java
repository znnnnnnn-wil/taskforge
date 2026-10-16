package io.taskforge;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

@Validated
@ConfigurationProperties("taskforge")
public record ForgeProperties(
        @NotBlank @Size(min = 16, max = 256) String apiKey,
        boolean workerEnabled,
        @NotBlank @Size(max = 100) String nodeId,
        @Min(1) @Max(128) int concurrency,
        @Min(6) @Max(3600) int leaseSeconds,
        @Min(50) @Max(60000) long pollMs) {
}
