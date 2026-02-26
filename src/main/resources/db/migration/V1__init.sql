CREATE TABLE orders (
  id UUID PRIMARY KEY,
  order_key VARCHAR(512) NOT NULL,
  payload TEXT NOT NULL,
  source_topic VARCHAR(512) NOT NULL,
  source_partition INT NOT NULL,
  source_offset BIGINT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (source_topic, source_partition, source_offset)
);

CREATE TABLE outbox_events (
  id UUID PRIMARY KEY,
  destination_topic VARCHAR(512) NOT NULL,
  destination_key VARCHAR(512) NOT NULL,
  payload TEXT NOT NULL,
  source_topic VARCHAR(512) NOT NULL,
  source_partition INT NOT NULL,
  source_offset BIGINT NOT NULL,
  status VARCHAR(32) NOT NULL,
  attempts INT NOT NULL DEFAULT 0,
  next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  last_error TEXT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  published_at TIMESTAMPTZ NULL,
  UNIQUE (source_topic, source_partition, source_offset)
);

CREATE INDEX outbox_events_pending_idx
  ON outbox_events (status, next_attempt_at, source_partition, source_offset);
