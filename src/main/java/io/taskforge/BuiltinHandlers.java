package io.taskforge;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.HexFormat;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class BuiltinHandlers {
    @Bean
    TaskHandler echoHandler() {
        return new TaskHandler() {
            public String name() { return "echo"; }
            public void validate(JsonNode payload) { }
            public JsonNode execute(JsonNode payload, TaskContext ctx) {
                ctx.checkCancelled();
                return payload.deepCopy();
            }
        };
    }

    @Bean
    TaskHandler sha256Handler(ObjectMapper json) {
        return new TaskHandler() {
            public String name() { return "sha256"; }
            public void validate(JsonNode payload) {
                if (!payload.path("text").isTextual()) throw new ForgeException(400, "需要 text 字符串");
            }
            public JsonNode execute(JsonNode payload, TaskContext ctx) throws Exception {
                ctx.checkCancelled();
                byte[] digest = MessageDigest.getInstance("SHA-256")
                        .digest(payload.path("text").asText().getBytes(StandardCharsets.UTF_8));
                return json.createObjectNode().put("sha256", HexFormat.of().formatHex(digest));
            }
        };
    }

    @Bean
    TaskHandler reportHandler(ObjectMapper json) {
        return new TaskHandler() {
            public String name() { return "report"; }
            public void validate(JsonNode payload) {
                JsonNode values = payload.path("values");
                if (!values.isArray() || values.isEmpty() || values.size() > 5000) {
                    throw new ForgeException(400, "values 必须为包含 1..5000 个数值的数组");
                }
                values.forEach(value -> {
                    if (!value.isNumber()) throw new ForgeException(400, "values 只能包含数字");
                });
            }
            public JsonNode execute(JsonNode payload, TaskContext ctx) {
                BigDecimal sum = BigDecimal.ZERO;
                int count = 0;
                for (JsonNode value : payload.path("values")) {
                    ctx.reportProgress(count++ * 99 / payload.path("values").size());
                    sum = sum.add(value.decimalValue());
                }
                return json.createObjectNode().put("count", count).put("sum", sum)
                        .put("average", sum.divide(BigDecimal.valueOf(count), java.math.MathContext.DECIMAL64));
            }
        };
    }

    @Bean
    TaskHandler delayHandler(ObjectMapper json) {
        return new TaskHandler() {
            public String name() { return "delay"; }
            public void validate(JsonNode payload) {
                if (!payload.path("milliseconds").canConvertToInt()
                        || payload.path("milliseconds").asInt() < 1
                        || payload.path("milliseconds").asInt() > 3600000) {
                    throw new ForgeException(400, "milliseconds 必须为 1..3600000 的整数");
                }
            }
            public JsonNode execute(JsonNode payload, TaskContext ctx) throws Exception {
                long duration = payload.path("milliseconds").asLong();
                long start = System.nanoTime();
                while ((System.nanoTime() - start) / 1000000 < duration) {
                    ctx.reportProgress((int) ((System.nanoTime() - start) / 1000000 * 99 / duration));
                    Thread.sleep(Math.min(100, duration));
                }
                return json.createObjectNode().put("waitedMs", duration);
            }
        };
    }
}
