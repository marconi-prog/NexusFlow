CREATE TABLE IF NOT EXISTS events (
    id                    BIGSERIAL PRIMARY KEY,
    type                  TEXT        NOT NULL,
    source                TEXT        NOT NULL DEFAULT '',
    actor                 TEXT        NOT NULL DEFAULT '',
    occurred_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    process_instance_key  TEXT        NOT NULL DEFAULT '',
    process_definition_id TEXT        NOT NULL DEFAULT '',
    job_key               TEXT        NOT NULL DEFAULT '',
    job_type              TEXT        NOT NULL DEFAULT '',
    incident_key          TEXT        NOT NULL DEFAULT '',
    duration_ms           BIGINT      NOT NULL DEFAULT 0,
    data                  JSONB
);

CREATE INDEX IF NOT EXISTS events_occurred_at_idx ON events (occurred_at DESC);
CREATE INDEX IF NOT EXISTS events_instance_idx    ON events (process_instance_key, id) WHERE process_instance_key <> '';
CREATE INDEX IF NOT EXISTS events_definition_idx  ON events (process_definition_id, occurred_at DESC) WHERE process_definition_id <> '';
CREATE INDEX IF NOT EXISTS events_job_type_idx    ON events (job_type, occurred_at DESC) WHERE job_type <> '';
CREATE INDEX IF NOT EXISTS events_type_idx        ON events (type, occurred_at DESC);

CREATE TABLE IF NOT EXISTS idempotency_keys (
    scope        TEXT        NOT NULL,
    key          TEXT        NOT NULL,
    request_hash TEXT        NOT NULL,
    status_code  INT         NOT NULL DEFAULT 0,
    response     BYTEA,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (scope, key)
);

CREATE INDEX IF NOT EXISTS idempotency_created_idx ON idempotency_keys (created_at);

CREATE TABLE IF NOT EXISTS process_instances (
    process_instance_key  TEXT PRIMARY KEY,
    business_key          TEXT        NOT NULL DEFAULT '',
    process_definition_id TEXT        NOT NULL,
    tenant_id             TEXT        NOT NULL DEFAULT '',
    started_by            TEXT        NOT NULL DEFAULT '',
    started_at            TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS process_instances_bk_idx ON process_instances (business_key) WHERE business_key <> '';
