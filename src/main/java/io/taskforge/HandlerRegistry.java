package io.taskforge;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.springframework.stereotype.Component;

@Component
public class HandlerRegistry {
    private final Map<String, TaskHandler> handlers = new LinkedHashMap<>();

    public HandlerRegistry(List<TaskHandler> discovered) {
        for (TaskHandler handler : discovered) {
            if (handlers.putIfAbsent(handler.name(), handler) != null) {
                throw new IllegalArgumentException("重复处理器: " + handler.name());
            }
        }
    }

    public TaskHandler require(String name) {
        TaskHandler handler = handlers.get(name);
        if (handler == null) throw new ForgeException(400, "未知处理器: " + name);
        return handler;
    }

    public void validate(String name, JsonNode payload) {
        if (payload == null || !payload.isObject() || payload.toString().length() > 32768) {
            throw new ForgeException(400, "payload 必须为不超过 32768 字符的 JSON 对象");
        }
        require(name).validate(payload);
    }

    public List<String> names() {
        return List.copyOf(handlers.keySet());
    }
}
