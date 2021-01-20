CREATE KEYSPACE IF NOT EXISTS pss_cassandra  WITH REPLICATION={'class': 'SimpleStrategy', 'replication_factor': 1};
USE pss_cassandra;

CREATE TABLE IF NOT EXISTS event_start (
  session_id uuid,
  country varchar,
  ts timestamp,
  player_id varchar,
  PRIMARY KEY (player_id, ts)
)
WITH CLUSTERING ORDER BY (ts DESC);

CREATE TABLE IF NOT EXISTS event_end (
  session_id uuid,
  ts timestamp,
  player_id varchar,
  PRIMARY KEY (player_id, ts)
)
WITH CLUSTERING ORDER BY (ts DESC);
