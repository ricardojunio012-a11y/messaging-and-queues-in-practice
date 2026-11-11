-- Orders per minute and per region, in Flink SQL

SET 'table.local-time-zone' = 'UTC';
SET 'execution.checkpointing.interval' = '5s';

-- The source: the orders topic. The event time comes from the ts field (milliseconds),
-- and the watermark stays 30 seconds behind the latest event time seen: it is the
-- tolerance for late events.
CREATE TABLE orders (
  region     STRING,
  total      DOUBLE,
  ts         BIGINT,
  event_time AS TO_TIMESTAMP_LTZ(ts, 3),
  WATERMARK FOR event_time AS event_time - INTERVAL '30' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'orders',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id' = 'flink',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json'
);

-- The destination: the windows-flink topic
CREATE TABLE windows (
  `window` STRING,
  region   STRING,
  orders   BIGINT
) WITH (
  'connector' = 'kafka',
  'topic' = 'windows-flink',
  'properties.bootstrap.servers' = 'kafka:9092',
  'format' = 'json'
);

-- Fixed 1-minute window by event time. Each window is emitted once, when the
-- watermark passes its end.
INSERT INTO windows
SELECT DATE_FORMAT(window_start, 'HH:mm'), region, COUNT(*)
FROM TABLE(TUMBLE(TABLE orders, DESCRIPTOR(event_time), INTERVAL '1' MINUTE))
GROUP BY window_start, window_end, region;
