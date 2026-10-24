package io.taskforge.persistence;

import io.taskforge.Attempt;
import java.time.Instant;

public class AttemptRow {
    public String taskId;
    public int number;
    public String nodeId;
    public long token;
    public String status;
    public Instant startedAt;
    public Instant finishedAt;
    public String error;

    public Attempt view() {
        return new Attempt(taskId, number, nodeId, token, status, startedAt, finishedAt, error);
    }
}
