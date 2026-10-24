package io.taskforge;

import com.mybatisflex.core.datasource.FlexDataSource;
import com.mybatisflex.spring.boot.SqlSessionFactoryBeanCustomizer;
import com.zaxxer.hikari.HikariDataSource;
import javax.sql.DataSource;
import org.mybatis.spring.transaction.SpringManagedTransactionFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.support.JdbcTransactionManager;

@Configuration
@EnableConfigurationProperties(DataSourceProperties.class)
public class PersistenceConfiguration {
    @Bean(destroyMethod = "close")
    @ConfigurationProperties("spring.datasource.hikari")
    HikariDataSource taskforgePool(DataSourceProperties properties) {
        return properties.initializeDataSourceBuilder().type(HikariDataSource.class).build();
    }

    @Bean
    @Primary
    FlexDataSource dataSource(@Qualifier("taskforgePool") HikariDataSource pool) {
        return new FlexDataSource("taskforge", pool);
    }

    @Bean
    SqlSessionFactoryBeanCustomizer springManagedTransactions() {
        return factory -> factory.setTransactionFactory(new SpringManagedTransactionFactory());
    }

    @Bean
    JdbcTransactionManager transactionManager(DataSource source) {
        var manager = new JdbcTransactionManager(source);
        manager.setNestedTransactionAllowed(true);
        return manager;
    }
}
