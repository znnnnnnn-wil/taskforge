package io.taskforge.persistence;

import io.taskforge.Task;
import java.sql.Timestamp;
import java.util.List;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Options;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;

/** Conditional transitions remain explicit SQL; callers hold the task row lock. */
@Mapper
public interface ExecutionMapper {
    @Update("""
            UPDATE forge_task SET status = 'RUNNING', attempts = attempts + 1,
              token = token + 1, owner = #{node}, lease_until = #{leaseUntil}, progress = 0,
              error = NULL, updated_at = #{now}
            WHERE id = #{id} AND token = #{expectedToken} AND status IN ('PENDING', 'RETRY')
              AND available_at <= #{now} AND attempts < max_attempts
            """)
    int claim(@Param("id") String id, @Param("expectedToken") long expectedToken,
              @Param("node") String node, @Param("leaseUntil") Timestamp leaseUntil,
              @Param("now") Timestamp now);

    @Insert("""
            INSERT INTO forge_attempt(task_id, number, node_id, token, status, started_at)
            VALUES (#{task.id}, #{task.attempts}, #{task.owner}, #{task.token}, 'RUNNING', #{now})
            """)
    int startAttempt(@Param("task") Task task, @Param("now") Timestamp now);

    @Update("""
            UPDATE forge_task SET lease_until = #{leaseUntil}, progress = #{progress}, updated_at = #{now}
            WHERE id = #{task.id} AND status = 'RUNNING' AND owner = #{task.owner}
              AND token = #{task.token} AND lease_until > #{now}
            """)
    int heartbeat(@Param("task") Task task, @Param("leaseUntil") Timestamp leaseUntil,
                  @Param("progress") int progress, @Param("now") Timestamp now);

    @Update("""
            UPDATE forge_task SET status = #{next}, result = #{result,jdbcType=VARCHAR},
              error = #{error,jdbcType=VARCHAR}, progress = #{progress},
              owner = NULL, lease_until = NULL, available_at = #{availableAt}, updated_at = #{now}
            WHERE id = #{task.id} AND status = 'RUNNING' AND owner = #{task.owner}
              AND token = #{task.token} AND lease_until > #{now}
            """)
    int finish(@Param("task") Task task, @Param("next") String next, @Param("result") String result,
               @Param("error") String error, @Param("progress") int progress,
               @Param("availableAt") Timestamp availableAt, @Param("now") Timestamp now);

    @Update("""
            UPDATE forge_task SET status = 'CANCELLED', token = token + 1,
              owner = NULL, lease_until = NULL, updated_at = #{now} WHERE id = #{id}
            """)
    int cancel(@Param("id") String id, @Param("now") Timestamp now);

    @Select("""
            SELECT * FROM forge_task WHERE status = 'RUNNING' AND lease_until <= CURRENT_TIMESTAMP
            ORDER BY lease_until LIMIT #{limit}
            """)
    @Options(useCache = false)
    List<TaskRow> expired(@Param("limit") int limit);

    @Update("""
            UPDATE forge_task SET status = #{next}, token = token + 1, owner = NULL,
              lease_until = NULL, error = '执行租约过期', available_at = #{availableAt}, updated_at = #{now}
            WHERE id = #{task.id} AND status = 'RUNNING' AND token = #{task.token} AND lease_until <= #{now}
            """)
    int recover(@Param("task") Task task, @Param("next") String next,
                @Param("availableAt") Timestamp availableAt, @Param("now") Timestamp now);

    @Update("""
            UPDATE forge_attempt SET status = #{status}, finished_at = #{now}, error = #{error,jdbcType=VARCHAR}
            WHERE task_id = #{task.id} AND number = #{task.attempts}
              AND token = #{task.token} AND status = 'RUNNING'
            """)
    int closeAttempt(@Param("task") Task task, @Param("status") String status,
                     @Param("now") Timestamp now, @Param("error") String error);
}
