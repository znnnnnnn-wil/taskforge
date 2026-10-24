package io.taskforge;

import javax.sql.DataSource;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.support.JdbcTransactionManager;

@Configuration
public class PersistenceConfiguration {
    @Bean
    JdbcTransactionManager transactionManager(DataSource source) {
        var manager = new JdbcTransactionManager(source);
        manager.setNestedTransactionAllowed(true);
        return manager;
    }
}
