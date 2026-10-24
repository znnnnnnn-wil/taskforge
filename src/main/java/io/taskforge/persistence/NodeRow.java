package io.taskforge.persistence;

import com.mybatisflex.annotation.Column;
import com.mybatisflex.annotation.Id;
import com.mybatisflex.annotation.Table;
import java.time.Instant;

@Table(value = "forge_node", camelToUnderline = true)
public class NodeRow {
    @Id public String nodeId;
    public boolean workerEnabled;
    public int capacity;
    public int active;
    @Column(onInsertValue = "CURRENT_TIMESTAMP") public Instant lastSeen;
}
