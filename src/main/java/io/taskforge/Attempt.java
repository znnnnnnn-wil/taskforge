package io.taskforge;

import java.time.Instant;

public record Attempt(String taskId, int number, String nodeId, long token,
                      String status, Instant startedAt, Instant finishedAt, String error) {
}
