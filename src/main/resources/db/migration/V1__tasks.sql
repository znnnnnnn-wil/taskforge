CREATE TABLE forge_task (
    id VARCHAR(36) PRIMARY KEY,
    request_key VARCHAR(120) NOT NULL UNIQUE,
    handler VARCHAR(40) NOT NULL,
    payload TEXT NOT NULL,
    status VARCHAR(16) NOT NULL,
    priority INTEGER NOT NULL CHECK (priority BETWEEN 0 AND 9),
    max_attempts INTEGER NOT NULL CHECK (max_attempts BETWEEN 1 AND 20),
    attempts INTEGER NOT NULL DEFAULT 0,
    retry_delay_seconds INTEGER NOT NULL,
    timeout_seconds INTEGER NOT NULL,
    available_at TIMESTAMP WITH TIME ZONE NOT NULL,
    owner VARCHAR(100),
    token BIGINT NOT NULL DEFAULT 0,
    lease_until TIMESTAMP WITH TIME ZONE,
    result TEXT,
    error VARCHAR(2000),
    progress INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CHECK (status IN ('PENDING', 'RUNNING', 'RETRY', 'SUCCEEDED', 'FAILED', 'CANCELLED'))
);
CREATE INDEX idx_forge_due ON forge_task(status, available_at, priority);
CREATE INDEX idx_forge_lease ON forge_task(status, lease_until);
CREATE INDEX idx_forge_created ON forge_task(created_at, id);
CREATE TABLE forge_attempt (
    task_id VARCHAR(36) NOT NULL REFERENCES forge_task(id),
    number INTEGER NOT NULL,
    node_id VARCHAR(100) NOT NULL,
    token BIGINT NOT NULL,
    status VARCHAR(16) NOT NULL,
    started_at TIMESTAMP WITH TIME ZONE NOT NULL,
    finished_at TIMESTAMP WITH TIME ZONE,
    error VARCHAR(2000),
    PRIMARY KEY (task_id, number)
);
