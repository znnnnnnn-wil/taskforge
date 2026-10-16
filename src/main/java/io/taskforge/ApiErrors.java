package io.taskforge;

import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;

@RestControllerAdvice
public class ApiErrors {
    private static final Logger log = LoggerFactory.getLogger(ApiErrors.class);

    @ExceptionHandler(ForgeException.class)
    ResponseEntity<?> domain(ForgeException error) {
        return response(error.status(), error.getMessage());
    }

    @ExceptionHandler(MethodArgumentNotValidException.class)
    ResponseEntity<?> validation(MethodArgumentNotValidException error) {
        String fields = error.getBindingResult().getFieldErrors().stream()
                .map(field -> field.getField() + ": " + field.getDefaultMessage())
                .distinct().sorted().collect(java.util.stream.Collectors.joining("; "));
        return response(400, fields);
    }

    @ExceptionHandler({HttpMessageNotReadableException.class, MethodArgumentTypeMismatchException.class})
    ResponseEntity<?> malformed(Exception error) { return response(400, "请求 JSON、日期或参数格式无效"); }

    @ExceptionHandler(org.springframework.dao.DataAccessException.class)
    ResponseEntity<?> unavailable(Exception error) {
        log.error("数据库操作失败", error);
        return response(503, "数据库暂不可用，请使用相同 requestKey 重试提交");
    }

    static ResponseEntity<?> response(int status, String message) {
        return ResponseEntity.status(status).body(Map.of("status", status, "message", message));
    }
}
