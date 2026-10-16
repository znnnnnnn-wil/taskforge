package io.taskforge;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import jakarta.validation.Valid;
import java.util.List;
import java.util.Map;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api")
public class TaskController {
    private final TaskStore tasks;
    private final TaskLifecycle lifecycle;
    private final HandlerRegistry handlers;
    private final WorkerEngine worker;
    private final ForgeProperties config;
    private final ObjectMapper json;
    private final NodeRegistry nodes;

    public TaskController(TaskStore tasks, TaskLifecycle lifecycle, HandlerRegistry handlers,
                          WorkerEngine worker, ForgeProperties config, ObjectMapper json, NodeRegistry nodes) {
        this.tasks = tasks;
        this.lifecycle = lifecycle;
        this.handlers = handlers;
        this.worker = worker;
        this.config = config;
        this.json = json;
        this.nodes = nodes;
    }

    @PostMapping("/tasks")
    @ResponseStatus(HttpStatus.CREATED)
    public ObjectNode submit(@RequestBody @Valid Submission request) {
        handlers.validate(request.handler(), request.payload());
        return view(tasks.submit(request));
    }

    @GetMapping("/tasks")
    public List<ObjectNode> list(@RequestParam(required = false) String status,
                                @RequestParam(defaultValue = "50") int limit,
                                @RequestParam(defaultValue = "0") int offset) {
        return tasks.list(status, limit, offset).stream().map(this::view).toList();
    }

    @GetMapping("/tasks/{id}")
    public ObjectNode get(@PathVariable String id) { return view(tasks.require(id)); }

    @PostMapping("/tasks/{id}/cancel")
    public ObjectNode cancel(@PathVariable String id) { return view(lifecycle.cancel(id)); }

    @GetMapping("/tasks/{id}/attempts")
    public List<Attempt> attempts(@PathVariable String id) { return tasks.attempts(id); }

    @GetMapping("/overview")
    public Map<String, Object> overview() {
        return Map.of("nodeId", config.nodeId(), "workerEnabled", config.workerEnabled(),
                "active", worker.activeCount(), "concurrency", config.concurrency(),
                "counts", tasks.counts(), "handlers", handlers.names(), "nodes", nodes.list());
    }

    ObjectNode view(Task task) {
        ObjectNode node = json.valueToTree(task);
        try {
            node.set("payload", json.readTree(task.payload()));
            node.set("result", task.result() == null ? json.nullNode() : json.readTree(task.result()));
            return node;
        } catch (JsonProcessingException corrupt) {
            throw new IllegalStateException("持久化的任务内容损坏", corrupt);
        }
    }
}
