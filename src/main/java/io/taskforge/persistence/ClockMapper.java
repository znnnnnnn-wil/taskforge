package io.taskforge.persistence;

import java.sql.Timestamp;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Options;
import org.apache.ibatis.annotations.Select;

@Mapper
public interface ClockMapper {
    @Select("SELECT clock_timestamp()")
    @Options(useCache = false, flushCache = Options.FlushCachePolicy.TRUE)
    Timestamp postgresNow();
}
