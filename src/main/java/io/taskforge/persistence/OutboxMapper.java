package io.taskforge.persistence;

import com.mybatisflex.core.BaseMapper;
import com.mybatisflex.annotation.UseDataSource;
import java.sql.Timestamp;
import java.util.List;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Options;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;

@Mapper
@UseDataSource("taskforge")
public interface OutboxMapper extends BaseMapper<OutboxRow> {
    @Insert("""
            INSERT INTO forge_outbox(event_id, task_id, expected_token, priority, available_at, next_attempt_at)
            SELECT #{eventId}, #{taskId}, #{token}, #{priority}, #{availableAt}, #{availableAt} WHERE NOT EXISTS (
              SELECT 1 FROM forge_outbox WHERE task_id = #{taskId} AND expected_token = #{token})
            """)
    int enqueue(@Param("eventId") String eventId, @Param("taskId") String taskId,
                @Param("token") long token, @Param("priority") int priority,
                @Param("availableAt") Timestamp availableAt);

    @Select("""
            SELECT o.event_id FROM forge_outbox o JOIN forge_task t ON t.id = o.task_id
            WHERE o.sent_at IS NULL AND o.available_at <= #{now} AND o.next_attempt_at <= #{now}
              AND (o.owner IS NULL OR o.lease_until <= #{now}) AND t.token = o.expected_token
              AND t.status IN ('PENDING', 'RETRY') AND t.attempts < t.max_attempts
              AND t.available_at <= #{now}
            ORDER BY o.priority DESC, o.available_at, o.event_id LIMIT 16
            """)
    List<String> candidates(@Param("now") Timestamp now);

    @Select("SELECT event_id FROM forge_outbox WHERE event_id = #{id} FOR UPDATE")
    @Options(useCache = false, flushCache = Options.FlushCachePolicy.TRUE)
    String lock(@Param("id") String id);

    @Update("""
            UPDATE forge_outbox SET owner = #{owner}, lease_until = #{leaseUntil}, attempts = attempts + 1
            WHERE event_id = #{id} AND sent_at IS NULL AND next_attempt_at <= #{now} AND available_at <= #{now}
              AND (owner IS NULL OR lease_until <= #{now})
            """)
    int reserve(@Param("id") String id, @Param("owner") String owner,
                @Param("leaseUntil") Timestamp leaseUntil, @Param("now") Timestamp now);

    @Update("""
            UPDATE forge_outbox SET sent_at = #{now}, owner = NULL, lease_until = NULL, error = NULL
            WHERE event_id = #{id} AND owner = #{owner}
            """)
    int sent(@Param("id") String id, @Param("owner") String owner, @Param("now") Timestamp now);

    @Update("""
            UPDATE forge_outbox SET owner = NULL, lease_until = NULL, next_attempt_at = #{nextAttempt}, error = #{error}
            WHERE event_id = #{id} AND owner = #{owner}
            """)
    int failed(@Param("id") String id, @Param("owner") String owner,
               @Param("nextAttempt") Timestamp nextAttempt, @Param("error") String error);

    @Select("""
            SELECT t.id FROM forge_task t WHERE t.status IN ('PENDING', 'RETRY')
              AND t.available_at <= #{cutoff} AND t.attempts < t.max_attempts
              AND NOT EXISTS (SELECT 1 FROM forge_outbox o WHERE o.task_id = t.id
                AND o.expected_token = t.token AND (o.sent_at IS NULL OR o.sent_at > #{cutoff}))
            ORDER BY t.available_at LIMIT 100
            """)
    List<String> repairCandidates(@Param("cutoff") Timestamp cutoff);

    @Select("SELECT * FROM forge_task WHERE id = #{id} FOR UPDATE")
    @Options(useCache = false, flushCache = Options.FlushCachePolicy.TRUE)
    TaskRow lockTask(@Param("id") String id);

    @Update("""
            UPDATE forge_outbox SET sent_at = NULL, next_attempt_at = #{now}, error = NULL
            WHERE task_id = #{taskId} AND expected_token = #{token} AND owner IS NULL AND sent_at <= #{cutoff}
            """)
    int repair(@Param("taskId") String taskId, @Param("token") long token,
               @Param("now") Timestamp now, @Param("cutoff") Timestamp cutoff);
}
