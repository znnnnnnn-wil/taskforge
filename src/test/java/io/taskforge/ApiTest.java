package io.taskforge;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.autoconfigure.actuate.observability.AutoConfigureObservability;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

@AutoConfigureMockMvc
@AutoConfigureObservability
@SpringBootTest(properties = {
        "spring.datasource.url=jdbc:h2:mem:api;MODE=PostgreSQL;DB_CLOSE_DELAY=-1",
        "taskforge.worker-enabled=false"})
class ApiTest {
    private static final String KEY = "local-development-key";
    @Autowired MockMvc http;
    @Autowired ObjectMapper json;

    String body(String key) {
        return """
                {"requestKey":"%s","handler":"echo","payload":{"message":"你好"},
                 "priority":0,"maxAttempts":3,"retryDelaySeconds":1,"timeoutSeconds":30}
                """.formatted(key);
    }

    @Test
    void authenticationAndPublicHealth() throws Exception {
        http.perform(get("/api/tasks")).andExpect(status().isUnauthorized());
        http.perform(get("/api/tasks").header("X-API-Key", "wrong")).andExpect(status().isUnauthorized());
        http.perform(get("/actuator/health")).andExpect(status().isOk()).andExpect(jsonPath("$.status").value("UP"));
        http.perform(get("/")).andExpect(status().isOk());
        http.perform(get("/index.html")).andExpect(status().isOk()).andExpect(content().string(org.hamcrest.Matchers.containsString("TaskForge")));
    }

    @Test
    void submissionReplayDetailCancelAndAttemptHistory() throws Exception {
        String input = body(UUID.randomUUID().toString());
        var response = http.perform(post("/api/tasks").header("X-API-Key", KEY)
                        .contentType(MediaType.APPLICATION_JSON).content(input))
                .andExpect(status().isCreated()).andExpect(jsonPath("$.payload.message").value("你好"))
                .andReturn().getResponse();
        String id = json.readTree(response.getContentAsByteArray()).path("id").asText();
        http.perform(post("/api/tasks").header("X-API-Key", KEY).contentType(MediaType.APPLICATION_JSON).content(input))
                .andExpect(status().isCreated()).andExpect(jsonPath("$.id").value(id));
        http.perform(get("/api/tasks/" + id).header("X-API-Key", KEY)).andExpect(status().isOk());
        http.perform(post("/api/tasks/" + id + "/cancel").header("X-API-Key", KEY))
                .andExpect(status().isOk()).andExpect(jsonPath("$.status").value("CANCELLED"));
        http.perform(get("/api/tasks/" + id + "/attempts").header("X-API-Key", KEY))
                .andExpect(status().isOk()).andExpect(content().json("[]"));
    }

    @Test
    void rejectsInvalidConflictingAndOversizedInput() throws Exception {
        String key = UUID.randomUUID().toString();
        http.perform(post("/api/tasks").header("X-API-Key", KEY).contentType(MediaType.APPLICATION_JSON).content(body(key)))
                .andExpect(status().isCreated());
        http.perform(post("/api/tasks").header("X-API-Key", KEY).contentType(MediaType.APPLICATION_JSON)
                        .content(body(key).replace("你好", "changed"))).andExpect(status().isConflict());
        http.perform(post("/api/tasks").header("X-API-Key", KEY).contentType(MediaType.APPLICATION_JSON)
                        .content(body(key).replace("\"maxAttempts\":3", "\"maxAttempts\":0"))).andExpect(status().isBadRequest());
        http.perform(post("/api/tasks").header("X-API-Key", KEY).contentType(MediaType.APPLICATION_JSON)
                        .content(body(key).replace("\"echo\"", "\"missing\""))).andExpect(status().isBadRequest());
        http.perform(post("/api/tasks").header("X-API-Key", KEY).contentType(MediaType.APPLICATION_JSON)
                        .content("x".repeat(65537))).andExpect(status().isPayloadTooLarge());
        http.perform(get("/api/tasks?limit=201").header("X-API-Key", KEY)).andExpect(status().isBadRequest());
    }

    @Test
    void scheduleLifecycleAndStrictRequestParsing() throws Exception {
        String request = """
                {"name":"分钟统计","cron":"0 * * * * *","zone":"Asia/Shanghai",
                 "task":{"handler":"echo","payload":{},"priority":0,"maxAttempts":1,
                 "retryDelaySeconds":1,"timeoutSeconds":30}}
                """;
        var created = http.perform(post("/api/schedules").header("X-API-Key", KEY)
                .contentType(MediaType.APPLICATION_JSON).content(request)).andExpect(status().isCreated()).andReturn();
        String id = json.readTree(created.getResponse().getContentAsByteArray()).path("id").asText();
        String nextRun = json.readTree(created.getResponse().getContentAsByteArray()).path("nextRun").asText();
        http.perform(put("/api/schedules/" + id + "/enabled").header("X-API-Key", KEY)
                .contentType(MediaType.APPLICATION_JSON).content("{\"enabled\":true}"))
                .andExpect(status().isOk()).andExpect(jsonPath("$.nextRun").value(nextRun));
        http.perform(put("/api/schedules/" + id + "/enabled").header("X-API-Key", KEY)
                .contentType(MediaType.APPLICATION_JSON).content("{\"enabled\":false}"))
                .andExpect(status().isOk()).andExpect(jsonPath("$.enabled").value(false));
        http.perform(put("/api/schedules/" + id + "/enabled").header("X-API-Key", KEY)
                .contentType(MediaType.APPLICATION_JSON).content("{}")) .andExpect(status().isBadRequest());
        http.perform(post("/api/tasks").header("X-API-Key", KEY).contentType(MediaType.APPLICATION_JSON)
                .content("{\"unknown\":true}")).andExpect(status().isBadRequest());
    }

    @Test
    void prometheusRequiresAuthenticationAndExportsWorkerMetrics() throws Exception {
        http.perform(get("/actuator/prometheus")).andExpect(status().isUnauthorized());
        http.perform(get("/actuator/prometheus").header("X-API-Key", KEY))
                .andExpect(status().isOk()).andExpect(content().string(org.hamcrest.Matchers.containsString("taskforge_worker_capacity")));
    }

    @Test
    void alternateUrlFormsCannotBypassAuthentication() throws Exception {
        http.perform(get("/api;route=1/tasks")).andExpect(status().isUnauthorized());
        http.perform(get("/api/tasks/")).andExpect(status().isUnauthorized());
        http.perform(get("/actuator/prometheus;route=1")).andExpect(status().isUnauthorized());
    }
}
