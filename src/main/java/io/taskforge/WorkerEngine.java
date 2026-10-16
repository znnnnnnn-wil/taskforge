package io.taskforge;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import jakarta.annotation.PreDestroy;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class WorkerEngine {
    private static final Logger log = LoggerFactory.getLogger(WorkerEngine.class);
    private final TaskStore store;
    private final TaskLifecycle lifecycle;
    private final HandlerRegistry handlers;
    private final ForgeProperties config;
    private final ObjectMapper json;
    private final MeterRegistry metrics;
    private final ThreadPoolExecutor pool;
    private final Map<String, Active> running = new ConcurrentHashMap<>();
    private volatile boolean closing;

    public WorkerEngine(TaskStore store, TaskLifecycle lifecycle, HandlerRegistry handlers,
                        ForgeProperties config, ObjectMapper json, MeterRegistry metrics) {
        this.store = store;
        this.lifecycle = lifecycle;
        this.handlers = handlers;
        this.config = config;
        this.json = json;
        this.metrics = metrics;
        this.pool = new ThreadPoolExecutor(config.concurrency(), config.concurrency(), 0,
                TimeUnit.SECONDS, new LinkedBlockingQueue<>(config.concurrency()), r -> {
                    Thread thread = new Thread(r, "taskforge-executor");
                    thread.setDaemon(true);
                    return thread;
                });
        metrics.gauge("taskforge.worker.active", running, Map::size);
        metrics.gauge("taskforge.worker.capacity", config, ForgeProperties::concurrency);
    }

    @Scheduled(fixedDelayString = "${taskforge.poll-ms:500}")
    public synchronized void poll() {
        if (!config.workerEnabled() || closing) return;
        int capacity = config.concurrency() - running.size();
        if (capacity <= 0) return;
        for (Task candidate : store.due(capacity)) {
            if (closing) return;
            lifecycle.claim(candidate.id(), candidate.token(), config.nodeId()).ifPresent(this::dispatch);
        }
    }

    private void dispatch(Task task) {
        Active active = new Active(task);
        active.future = new FutureTask<>(() -> { execute(active); return null; }) {
            @Override public void run() {
                try { super.run(); }
                finally {
                    running.remove(task.id(), active);
                    try { poll(); }
                    catch (RuntimeException unavailable) { log.warn("任务完成后补充领取失败，等待定时轮询恢复"); }
                }
            }
        };
        running.put(task.id(), active);
        try {
            pool.execute(active.future);
            metrics.counter("taskforge.task.claimed").increment();
            metrics.timer("taskforge.task.scheduling.delay").record(
                    java.time.Duration.between(task.availableAt(), task.updatedAt()).isNegative()
                            ? java.time.Duration.ZERO
                            : java.time.Duration.between(task.availableAt(), task.updatedAt()));
        } catch (java.util.concurrent.RejectedExecutionException rejected) {
            running.remove(task.id(), active);
            lifecycle.finish(task, null, "执行器正在关闭");
        }
    }

    private void execute(Active active) {
        Timer.Sample sample = Timer.start(metrics);
        String outcome = "failed";
        try {
            TaskContext ctx = new TaskContext(active.task.id(), active.task.attempts(),
                    active.task.token(), active.progress, active.settled);
            var payload = json.readTree(active.task.payload());
            TaskHandler handler = handlers.require(active.task.handler());
            handler.validate(payload);
            String result = json.writeValueAsString(handler.execute(payload, ctx));
            if (result.length() > 65536) throw new IllegalStateException("任务结果超过 65536 字符");
            if (active.settled.compareAndSet(false, true)) {
                outcome = lifecycle.finish(active.task, result, null) ? "succeeded" : "stale";
            } else outcome = "stopped";
        } catch (Exception failure) {
            if (active.settled.compareAndSet(false, true)) {
                lifecycle.finish(active.task, null, failure.getClass().getSimpleName() + ": " + failure.getMessage());
            }
        } finally {
            sample.stop(metrics.timer("taskforge.task.execution", "outcome", outcome));
            log.info("task={} attempt={} node={} outcome={}", active.task.id(),
                    active.task.attempts(), config.nodeId(), outcome);
        }
    }

    @Scheduled(fixedDelay = 1000)
    public void maintain() {
        if (!config.workerEnabled() || closing) return;
        for (Active active : running.values()) {
            if (active.settled.get()) continue;
            boolean timeout = System.nanoTime() - active.startedNanos
                    >= TimeUnit.SECONDS.toNanos(active.task.timeoutSeconds());
            if (timeout && active.settled.compareAndSet(false, true)) {
                try { lifecycle.finish(active.task, null, "任务执行超时"); }
                finally { stop(active); }
            } else if (!timeout) {
                try {
                    if (!lifecycle.heartbeat(active.task, active.progress.get())) {
                        active.settled.set(true);
                        stop(active);
                    }
                } catch (RuntimeException databaseUnavailable) {
                    active.settled.set(true);
                    stop(active);
                    log.warn("task={} 续租失败，停止本地执行并等待持久化租约恢复", active.task.id());
                }
            }
        }
        int recovered = lifecycle.recoverExpired(100);
        if (recovered > 0) metrics.counter("taskforge.task.recovered").increment(recovered);
    }

    private void stop(Active active) {
        active.future.cancel(true);
        if (pool.remove(active.future)) running.remove(active.task.id(), active);
    }

    public int activeCount() { return running.size(); }

    @PreDestroy
    public void close() {
        closing = true;
        running.values().forEach(active -> { active.settled.set(true); stop(active); });
        pool.shutdownNow();
        try { pool.awaitTermination(10, TimeUnit.SECONDS); }
        catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); }
    }

    private static final class Active {
        final Task task;
        final long startedNanos = System.nanoTime();
        final AtomicInteger progress = new AtomicInteger();
        final AtomicBoolean settled = new AtomicBoolean();
        FutureTask<Void> future;
        Active(Task task) { this.task = task; }
    }
}
