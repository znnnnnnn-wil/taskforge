package io.taskforge;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ReadListener;
import jakarta.servlet.ServletException;
import jakarta.servlet.ServletInputStream;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletRequestWrapper;
import jakarta.servlet.http.HttpServletResponse;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Map;
import org.springframework.stereotype.Component;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.filter.OncePerRequestFilter;

@Component
public class ApiSecurity extends OncePerRequestFilter {
    private final byte[] expected;
    private final ObjectMapper json;

    public ApiSecurity(ForgeProperties config, ObjectMapper json,
                       @Value("${server.address:127.0.0.1}") String address) {
        if (config.apiKey().equals("local-development-key")
                && !address.equals("127.0.0.1") && !address.equals("::1") && !address.equals("localhost")) {
            throw new IllegalArgumentException("非回环监听必须设置独立的 TASKFORGE_API_KEY");
        }
        this.expected = config.apiKey().getBytes(StandardCharsets.UTF_8);
        this.json = json;
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response,
                                    FilterChain chain) throws ServletException, IOException {
        response.setHeader("X-Content-Type-Options", "nosniff");
        response.setHeader("Referrer-Policy", "no-referrer");
        response.setHeader("X-Frame-Options", "DENY");
        response.setHeader("Content-Security-Policy", "default-src 'self'; frame-ancestors 'none'; base-uri 'self'; form-action 'self'");
        String path = request.getRequestURI().substring(request.getContextPath().length());
        // Only exact static assets and health are public. Raw URL variants must never bypass authentication.
        if (java.util.Set.of("/", "/index.html", "/app.css", "/app.js", "/favicon.ico", "/actuator/health")
                .contains(path)) {
            chain.doFilter(request, response);
            return;
        }
        response.setHeader("Cache-Control", "no-store");
        String key = request.getHeader("X-API-Key");
        if (key == null || !MessageDigest.isEqual(expected, key.getBytes(StandardCharsets.UTF_8))) {
            reject(response, 401, "需要有效的 X-API-Key");
            return;
        }
        if (request.getMethod().equals("POST") || request.getMethod().equals("PUT")) {
            byte[] body = request.getInputStream().readNBytes(65537);
            if (body.length > 65536) {
                reject(response, 413, "请求体不能超过 65536 字节");
                return;
            }
            request = new HttpServletRequestWrapper(request) {
                @Override public ServletInputStream getInputStream() {
                    ByteArrayInputStream input = new ByteArrayInputStream(body);
                    return new ServletInputStream() {
                        public int read() { return input.read(); }
                        public int read(byte[] bytes, int offset, int length) { return input.read(bytes, offset, length); }
                        public boolean isFinished() { return input.available() == 0; }
                        public boolean isReady() { return true; }
                        public void setReadListener(ReadListener listener) { throw new UnsupportedOperationException(); }
                    };
                }
            };
        }
        chain.doFilter(request, response);
    }

    private void reject(HttpServletResponse response, int status, String message) throws IOException {
        response.setStatus(status);
        response.setContentType("application/json;charset=UTF-8");
        json.writeValue(response.getOutputStream(), Map.of("status", status, "message", message));
    }
}
