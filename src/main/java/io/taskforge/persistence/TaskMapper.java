package io.taskforge.persistence;

import com.mybatisflex.core.BaseMapper;
import com.mybatisflex.annotation.UseDataSource;
import java.util.List;
import java.util.Map;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;

@Mapper
@UseDataSource("taskforge")
public interface TaskMapper extends BaseMapper<TaskRow> {
    @Select("SELECT * FROM forge_task WHERE id = #{id} FOR UPDATE")
    TaskRow lock(@Param("id") String id);

    @Select("""
            SELECT * FROM forge_task WHERE status IN ('PENDING', 'RETRY')
              AND available_at <= CURRENT_TIMESTAMP AND attempts < max_attempts
            ORDER BY priority DESC, available_at, id LIMIT #{limit}
            """)
    List<TaskRow> due(@Param("limit") int limit);

    @Select("SELECT * FROM forge_attempt WHERE task_id = #{id} ORDER BY number")
    List<AttemptRow> attempts(@Param("id") String id);

    @Select("SELECT status AS \"status\", COUNT(*) AS \"total\" FROM forge_task GROUP BY status")
    List<Map<String, Object>> counts();

    @Update("UPDATE forge_task SET schedule_id = #{schedule} WHERE id = #{id}")
    int assignSchedule(@Param("id") String id, @Param("schedule") String schedule);
}
