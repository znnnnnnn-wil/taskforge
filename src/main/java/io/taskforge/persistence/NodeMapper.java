package io.taskforge.persistence;

import com.mybatisflex.core.BaseMapper;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Update;

@Mapper
public interface NodeMapper extends BaseMapper<NodeRow> {
    @Update("""
            UPDATE forge_node SET worker_enabled = #{enabled}, capacity = #{capacity},
              active = #{active}, last_seen = CURRENT_TIMESTAMP WHERE node_id = #{node}
            """)
    int heartbeat(@Param("node") String node, @Param("enabled") boolean enabled,
                  @Param("capacity") int capacity, @Param("active") int active);
}
