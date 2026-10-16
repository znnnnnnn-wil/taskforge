CREATE TABLE forge_schedule (
    id VARCHAR(36) PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    cron VARCHAR(100) NOT NULL,
    zone VARCHAR(80) NOT NULL,
    task_json TEXT NOT NULL,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    next_run TIMESTAMP WITH TIME ZONE NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX idx_schedule_due ON forge_schedule(enabled, next_run);
ALTER TABLE forge_task ADD COLUMN schedule_id VARCHAR(36) REFERENCES forge_schedule(id);
CREATE INDEX idx_task_schedule ON forge_task(schedule_id, created_at);
