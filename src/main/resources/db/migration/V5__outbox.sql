CREATE TABLE forge_outbox (
    event_id VARCHAR(36) PRIMARY KEY,
    task_id VARCHAR(36) NOT NULL REFERENCES forge_task(id),
    expected_token BIGINT NOT NULL,
    priority INTEGER NOT NULL CHECK (priority BETWEEN 0 AND 9),
    available_at TIMESTAMP WITH TIME ZONE NOT NULL,
    next_attempt_at TIMESTAMP WITH TIME ZONE NOT NULL,
    owner VARCHAR(36),
    lease_until TIMESTAMP WITH TIME ZONE,
    attempts INTEGER NOT NULL DEFAULT 0,
    sent_at TIMESTAMP WITH TIME ZONE,
    error VARCHAR(2000),
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (task_id, expected_token)
);
CREATE INDEX idx_outbox_due ON forge_outbox(sent_at, next_attempt_at, available_at);
INSERT INTO forge_outbox(event_id, task_id, expected_token, priority, available_at, next_attempt_at)
SELECT id, id, token, priority, available_at, available_at FROM forge_task
WHERE status IN ('PENDING', 'RETRY') AND attempts < max_attempts;
