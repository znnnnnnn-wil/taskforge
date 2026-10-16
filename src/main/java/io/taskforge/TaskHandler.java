package io.taskforge;

import com.fasterxml.jackson.databind.JsonNode;

public interface TaskHandler {
    String name();
    void validate(JsonNode payload);
    JsonNode execute(JsonNode payload, TaskContext context) throws Exception;
}
