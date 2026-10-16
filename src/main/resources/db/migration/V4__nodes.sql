CREATE TABLE forge_node (
    node_id VARCHAR(100) PRIMARY KEY,
    worker_enabled BOOLEAN NOT NULL,
    capacity INTEGER NOT NULL,
    active INTEGER NOT NULL,
    last_seen TIMESTAMP WITH TIME ZONE NOT NULL
);
