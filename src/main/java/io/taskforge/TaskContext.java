package io.taskforge;

import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public record TaskContext(String taskId, int attempt, long fencingToken,
                          AtomicInteger progress, AtomicBoolean stopped) {
    public void checkCancelled() {
        if (stopped.get() || Thread.currentThread().isInterrupted()) {
            throw new CancellationException("执行已停止或失去租约");
        }
    }

    public void reportProgress(int value) {
        checkCancelled();
        progress.accumulateAndGet(Math.max(0, Math.min(99, value)), Math::max);
    }
}
