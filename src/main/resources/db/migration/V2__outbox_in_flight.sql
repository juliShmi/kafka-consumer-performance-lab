ALTER TABLE outbox_events
  ADD COLUMN locked_at TIMESTAMPTZ NULL;

CREATE INDEX outbox_events_in_flight_idx
  ON outbox_events (status, locked_at, source_partition, source_offset);
