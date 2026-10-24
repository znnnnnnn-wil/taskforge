package io.taskforge.persistence;

import com.mybatisflex.core.BaseMapper;
import com.mybatisflex.annotation.UseDataSource;
import java.time.Instant;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;

@Mapper
@UseDataSource("taskforge")
public interface ScheduleMapper extends BaseMapper<ScheduleRow> {
    @Select("SELECT * FROM forge_schedule WHERE id = #{id} FOR UPDATE")
    ScheduleRow lock(@Param("id") String id);

    @Update("UPDATE forge_schedule SET enabled = TRUE, next_run = #{next} WHERE id = #{id}")
    int resume(@Param("id") String id, @Param("next") Instant next);

    @Update("UPDATE forge_schedule SET enabled = FALSE WHERE id = #{id}")
    int pause(@Param("id") String id);

    @Update("""
            UPDATE forge_schedule SET next_run = #{next}
            WHERE id = #{id} AND enabled = TRUE AND next_run = #{expected}
            """)
    int advance(@Param("id") String id, @Param("expected") Instant expected, @Param("next") Instant next);
}
