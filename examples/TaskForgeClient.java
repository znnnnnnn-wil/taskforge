import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.UUID;

/** Java 17 client example, runnable with: java examples/TaskForgeClient.java */
public final class TaskForgeClient {
    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(3)).build();
    private final URI base;
    private final String key;

    public TaskForgeClient(URI base, String key) {
        if (!java.util.Set.of("http", "https").contains(base.getScheme()) || base.getHost() == null) {
            throw new IllegalArgumentException("An HTTP(S) API origin is required");
        }
        this.base = base;
        this.key = java.util.Objects.requireNonNull(key, "TASKFORGE_API_KEY is required");
    }

    public String submit(String json) throws IOException, InterruptedException {
        return send("POST", "/api/tasks", json);
    }

    public String get(UUID id) throws IOException, InterruptedException {
        return send("GET", "/api/tasks/" + id, null);
    }

    public String cancel(UUID id) throws IOException, InterruptedException {
        return send("POST", "/api/tasks/" + id + "/cancel", null);
    }

    private String send(String method, String path, String body) throws IOException, InterruptedException {
        var request = HttpRequest.newBuilder(base.resolve(path)).timeout(Duration.ofSeconds(20))
                .header("X-API-Key", key).header("Content-Type", "application/json")
                .method(method, body == null ? HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofString(body)).build();
        var response = http.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() / 100 != 2) {
            throw new IOException("HTTP " + response.statusCode() + ": " + response.body());
        }
        return response.body();
    }

    public static void main(String[] args) throws Exception {
        var client = new TaskForgeClient(URI.create(System.getenv().getOrDefault("TASKFORGE_URL", "http://127.0.0.1:8080")),
                System.getenv("TASKFORGE_API_KEY"));
        System.out.println(client.submit("""
                {"requestKey":"%s","handler":"report","payload":{"values":[10,20,30]},
                 "priority":0,"maxAttempts":3,"retryDelaySeconds":1,"timeoutSeconds":30}
                """.formatted(UUID.randomUUID())));
    }
}
