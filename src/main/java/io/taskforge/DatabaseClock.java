package io.taskforge;

import io.taskforge.persistence.ClockMapper;
import java.time.Instant;
import javax.sql.DataSource;
import org.springframework.stereotype.Component;

@Component
public class DatabaseClock {
    private final ClockMapper mapper;
    private final boolean h2;

    public DatabaseClock(DataSource source, ClockMapper mapper) throws java.sql.SQLException {
        this.mapper = mapper;
        try (var connection = source.getConnection()) {
            String product = connection.getMetaData().getDatabaseProductName();
            h2 = product.equals("H2");
            String url = connection.getMetaData().getURL();
            if (h2 && (url.startsWith("jdbc:h2:tcp:") || url.startsWith("jdbc:h2:ssl:"))) {
                throw new IllegalArgumentException("H2 仅支持嵌入式开发；多节点请使用 PostgreSQL");
            }
            if (!h2 && !product.equals("PostgreSQL")) {
                throw new IllegalArgumentException("仅支持 H2 和 PostgreSQL");
            }
        }
    }

    public Instant now() {
        return h2 ? Instant.now() : mapper.postgresNow().toInstant();
    }
}
