-- Schéma Cassandra : une table par requête (modélisation orientée requêtes)
-- Création : docker compose exec cassandra cqlsh -f /schema/schema.cql

CREATE KEYSPACE IF NOT EXISTS ids
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

-- Q1 : trafic d'une journée, fenêtre par fenêtre (écrit par le streaming)
CREATE TABLE IF NOT EXISTS ids.traffic_by_window (
  day             date,
  window_start    timestamp,
  window_end      timestamp,
  flows           int,
  packets         bigint,
  bytes           bigint,
  packets_per_sec double,
  distinct_ports  int,
  short_flows     int,
  PRIMARY KEY ((day), window_start)
) WITH CLUSTERING ORDER BY (window_start DESC)
  AND default_time_to_live = 2592000;   -- 30 jours

-- Q2 : alertes d'un type pendant une journée
CREATE TABLE IF NOT EXISTS ids.alerts_by_type (
  alert_type   text,
  day          date,
  alert_time   timestamp,
  alert_id     text,          -- déterministe : type|port|début de fenêtre (pas de doublon si rejeu)
  dst_port     int,
  severity     text,
  metric_value double,
  threshold    double,
  window_start timestamp,
  window_end   timestamp,
  PRIMARY KEY ((alert_type, day), alert_time, alert_id)
) WITH CLUSTERING ORDER BY (alert_time DESC, alert_id ASC);

-- Q3 : alertes visant un port pendant une journée (mêmes alertes, autre clé = dénormalisation)
CREATE TABLE IF NOT EXISTS ids.alerts_by_port (
  dst_port     int,
  day          date,
  alert_time   timestamp,
  alert_id     text,
  alert_type   text,
  severity     text,
  metric_value double,
  threshold    double,
  PRIMARY KEY ((dst_port, day), alert_time, alert_id)
) WITH CLUSTERING ORDER BY (alert_time DESC, alert_id ASC);

-- Q4 : répartition des flux par étiquette et par jour (écrit par le batch)
CREATE TABLE IF NOT EXISTS ids.daily_stats (
  day     date,
  label   text,
  flows   bigint,
  PRIMARY KEY ((day), label)
);
