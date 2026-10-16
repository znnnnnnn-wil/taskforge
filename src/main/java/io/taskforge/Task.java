package io.taskforge;

import java.time.Instant;

public record Task(
        String id, String requestKey, String handler, String payload,
        String status, int priority, int maxAttempts, int attempts,
        int retryDelaySeconds, int timeoutSeconds, Instant availableAt,
        String owner, long token, Instant leaseUntil, String result,
        String error, int progress, Instant createdAt, Instant updatedAt) {
    public boolean terminal() {
        return status.equals("SUCCEEDED") || status.equals("FAILED") || status.equals("CANCELLED");
    }
}
