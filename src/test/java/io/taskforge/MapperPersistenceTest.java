package io.taskforge;

import static org.assertj.core.api.Assertions.assertThat;

import io.taskforge.persistence.TaskMapper;
import io.taskforge.persistence.TaskRow;
import java.time.Instant;
import java.util.UUID;
import javax.sql.DataSource;
import org.apache.ibatis.session.LocalCacheScope;
import org.apache.ibatis.session.SqlSessionFactory;
import org.mybatis.spring.transaction.SpringManagedTransactionFactory;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.support.TransactionTemplate;

@SpringBootTest(properties = {
        "spring.datasource.url=jdbc:h2:mem:flex-mapping;MODE=PostgreSQL;DB_CLOSE_DELAY=-1",
        "taskforge.worker-enabled=false"})
class MapperPersistenceTest {
    @Autowired TaskMapper mapper;
    @Autowired JdbcTemplate jdbc;
    @Autowired TransactionTemplate tx;
    @Autowired DataSource source;
    @Autowired SqlSessionFactory sessions;

    @Test
    void mapperAndSpringShareTransactionResourcesWithoutSessionCache() {
        var configuration = sessions.getConfiguration();
        assertThat(configuration.getEnvironment().getDataSource()).isSameAs(source);
        assertThat(configuration.getEnvironment().getTransactionFactory())
                .isInstanceOf(SpringManagedTransactionFactory.class);
        assertThat(configuration.getLocalCacheScope()).isEqualTo(LocalCacheScope.STATEMENT);
        assertThat(configuration.isCacheEnabled()).isFalse();
        var manager = (org.springframework.jdbc.datasource.DataSourceTransactionManager) tx.getTransactionManager();
        assertThat(manager.getDataSource()).isSameAs(source);
        assertThat(manager.isNestedTransactionAllowed()).isTrue();
    }

    @Test
    void selectiveInsertPreservesDefaultsAndTimestampPrecision() {
        TaskRow task = row();
        mapper.insertSelectiveWithPk(task);
        TaskRow saved = mapper.selectOneById(task.id);
        assertThat(saved.id).isEqualTo(task.id);
        assertThat(saved.requestKey).isEqualTo(task.requestKey);
        assertThat(saved.availableAt).isEqualTo(task.availableAt);
        assertThat(saved.createdAt).isNotNull();
        assertThat(saved.attempts).isZero();
        assertThat(saved.owner).isNull();
    }

    @Test
    void repeatedReadsInsideTransactionSeeDatabaseChanges() {
        TaskRow task = row();
        mapper.insertSelectiveWithPk(task);
        tx.executeWithoutResult(status -> {
            assertThat(mapper.selectOneById(task.id).status).isEqualTo("PENDING");
            jdbc.update("UPDATE forge_task SET status='CANCELLED' WHERE id=?", task.id);
            assertThat(mapper.selectOneById(task.id).status).isEqualTo("CANCELLED");
        });
    }

    @Test
    void mapperWritesRollbackWithSpringTransaction() {
        TaskRow task = row();
        tx.executeWithoutResult(status -> {
            mapper.insertSelectiveWithPk(task);
            assertThat(mapper.selectOneById(task.id)).isNotNull();
            status.setRollbackOnly();
        });
        assertThat(mapper.selectOneById(task.id)).isNull();
    }

    private TaskRow row() {
        TaskRow task = new TaskRow();
        task.id = UUID.randomUUID().toString();
        task.requestKey = task.id;
        task.handler = "echo";
        task.payload = "{}";
        task.status = "PENDING";
        task.maxAttempts = 1;
        task.retryDelaySeconds = 1;
        task.timeoutSeconds = 60;
        task.availableAt = TaskStore.timestamp(Instant.now()).toInstant();
        return task;
    }
}
