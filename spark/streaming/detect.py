"""
Détection d'intrusions en temps réel avec Spark Structured Streaming.

Lit les flux réseau rejoués dans Kafka (topic network-flows), calcule des indicateurs
sur des fenêtres de temps glissantes et applique des règles à seuils :

  - DDoS / DoS   : trop de connexions ou de paquets/s vers un même port de destination ;
  - Force brute  : trop de connexions courtes vers FTP (21) ou SSH (22) ;
  - Scan de ports: trop de ports distincts visés par une même adresse source.

Trois requêtes de streaming lisent le même topic :
  1. "ports"   : fenêtre glissante x port de destination -> règles DDoS/DoS et force brute
  2. "sources" : fenêtre glissante x adresse source      -> règle scan de ports
  3. "trafic"  : fenêtre fixe de 1 s                      -> trafic par seconde (tableau de bord)

L'étiquette "label" du jeu de données n'est JAMAIS utilisée par les règles : elle est
seulement recopiée (nombre de flux d'attaque, types d'attaque) pour l'évaluation.

Fiabilité : offsets Kafka et état des fenêtres sauvegardés dans des checkpoints ;
chaque document Elasticsearch a un identifiant déterministe (fenêtre + clé), donc
rejouer un micro-batch après une panne remplace les documents au lieu de les dupliquer.

Lancement (Kafka, Spark et Elasticsearch démarrés avec docker compose) :
  docker compose exec spark-master spark-submit --master spark://spark-master:7077 \
      spark/streaming/detect.py
Options utiles :
  --sink console        affiche les alertes au lieu de les envoyer à Elasticsearch
  --starting-offsets earliest   relit tout le topic au premier lancement
  --reset               efface les checkpoints (repartir de zéro)
"""

import argparse
import os
import shutil
from datetime import datetime, timezone

import yaml
from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

# Message JSON envoyé par producer/producer.py
FLOW_SCHEMA = T.StructType([
    T.StructField("timestamp", T.StringType()),        # heure d'origine (2018)
    T.StructField("event_time", T.StringType()),       # heure du rejeu (utilisée pour les fenêtres)
    T.StructField("dst_port", T.IntegerType()),
    T.StructField("protocol", T.IntegerType()),
    T.StructField("flow_duration", T.DoubleType()),    # microsecondes
    T.StructField("tot_fwd_pkts", T.DoubleType()),
    T.StructField("tot_bwd_pkts", T.DoubleType()),
    T.StructField("totlen_fwd_pkts", T.DoubleType()),
    T.StructField("totlen_bwd_pkts", T.DoubleType()),
    T.StructField("src_ip", T.StringType()),
    T.StructField("dst_ip", T.StringType()),
    T.StructField("label", T.StringType()),            # évaluation uniquement
])

ISO = "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"   # format de date compris par Elasticsearch


# ---------------------------------------------------------------- configuration
def load_config(path: str) -> dict:
    with open(path, encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    scale = cfg.get("replay_rate", 1000) / cfg.get("reference_rate", 1000)
    r = cfg["rules"]
    # seuils effectifs, mis à l'échelle du débit de rejeu
    cfg["thr"] = {
        "pps": r["ddos_dos"]["max_packets_per_second"] * scale,
        "conn": r["ddos_dos"]["max_connections_per_window"] * scale,
        "short": r["brute_force"]["max_short_connections_per_window"] * scale,
        "ports": r["port_scan"]["max_distinct_ports"] * scale,
    }
    cfg["window_seconds"] = int(cfg["window"]["duration"].split()[0])
    return cfg


# ---------------------------------------------------------------- sorties
class Sink:
    """Envoie les documents vers Elasticsearch, ou les affiche (mode console)."""

    def __init__(self, cfg: dict, mode: str):
        self.mode, self.es_cfg = mode, cfg["elasticsearch"]
        self.seen_alerts = set()   # mode console : chaque alerte n'est affichée qu'une fois
        if mode == "es":
            from elasticsearch import Elasticsearch, helpers
            self.helpers = helpers
            self.es = Elasticsearch(f"http://{self.es_cfg['host']}:{self.es_cfg['port']}",
                                    request_timeout=30, retry_on_timeout=True, max_retries=5)
            self.create_indices()

    def create_indices(self):
        common = {"window_start": {"type": "date"}, "window_end": {"type": "date"},
                  "processed_at": {"type": "date"}, "last_event_time": {"type": "date"},
                  "dst_port": {"type": "integer"}, "src_ip": {"type": "keyword"},
                  "attack_labels": {"type": "keyword"}, "alert_type": {"type": "keyword"},
                  "severity": {"type": "keyword"}, "metric": {"type": "keyword"},
                  "alert_id": {"type": "keyword"}}
        for key in ("traffic_index", "ports_index", "alerts_index"):
            name = self.es_cfg[key]
            if not self.es.indices.exists(index=name):
                self.es.indices.create(index=name, mappings={"properties": common},
                                       settings={"number_of_replicas": 0, "refresh_interval": "1s"})
                print(f"[es] index {name} créé")

    def send(self, index_key: str, docs: list[dict], id_field: str):
        if not docs:
            return
        if self.mode == "console":
            if index_key == "alerts_index":
                for d in docs:
                    if d["alert_id"] in self.seen_alerts:
                        continue
                    self.seen_alerts.add(d["alert_id"])
                    print(f"  ALERTE {d['alert_type']:<11} {d['severity']:<8} "
                          f"fenêtre {d['window_start'][11:19]}-{d['window_end'][11:19]} "
                          f"{'port ' + str(d['dst_port']) if d.get('dst_port') is not None else 'source ' + str(d.get('src_ip'))}"
                          f"  {d['metric']}={d['metric_value']:,.0f} (seuil {d['threshold']:,.0f})"
                          f"  latence {d['latency_s']:.1f} s"
                          f"  [vérité terrain : {d['attack_flows']} flux d'attaque {d['attack_labels']}]")
            return
        actions = ({"_index": self.es_cfg[index_key], "_id": d[id_field], "_source": d} for d in docs)
        self.helpers.bulk(self.es, actions, chunk_size=1000)


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def latency(processed_at: str, last_event: str) -> float:
    return round((datetime.fromisoformat(processed_at)
                  - datetime.fromisoformat(last_event.replace("Z", "+00:00"))).total_seconds(), 3)


def with_window_strings(df: DataFrame) -> DataFrame:
    return (df.withColumn("window_start", F.date_format("window.start", ISO))
              .withColumn("window_end", F.date_format("window.end", ISO))
              .withColumn("last_event_time", F.date_format("last_event", ISO)))


def make_alert(cfg, rule, row, key, metric, value, threshold, processed_at):
    alert_type = {"ddos_dos": "DDoS/DoS", "brute_force": "BruteForce", "port_scan": "PortScan"}[rule]
    return {
        "alert_id": f"{rule}|{key}|{row['window_start']}",   # déterministe : pas de doublon au rejeu
        "alert_type": alert_type,
        "severity": cfg["rules"][rule]["severity"],
        "alert_time": row["window_end"],
        "window_start": row["window_start"], "window_end": row["window_end"],
        "dst_port": row["dst_port"] if "dst_port" in row else None,
        "src_ip": row["src_ip"] if "src_ip" in row else None,
        "metric": metric, "metric_value": float(value), "threshold": float(threshold),
        "flows": row["flows"], "attack_flows": row["attack_flows"],
        "attack_labels": row["attack_labels"],
        "processed_at": processed_at,
        "last_event_time": row["last_event_time"],
        "latency_s": latency(processed_at, row["last_event_time"]),
    }


# ---------------------------------------------------------------- traitement des micro-batchs
def process_ports(cfg, sink):
    """Règles DDoS/DoS et force brute sur les agrégats (fenêtre, port de destination)."""
    thr, rules = cfg["thr"], cfg["rules"]
    bf_ports = rules["brute_force"]["target_ports"]
    top_n = cfg["streaming"]["top_ports_per_window"]
    win_s = cfg["window_seconds"]

    def handler(batch: DataFrame, batch_id: int):
        df = with_window_strings(batch).withColumn("packets_per_sec", F.col("packets") / win_s)
        rank = F.row_number().over(Window.partitionBy("window_start").orderBy(F.desc("flows")))
        # On ne rapatrie que les lignes utiles : ports les plus actifs, candidats aux règles,
        # et ports qui reçoivent des flux d'attaque (nécessaires pour l'évaluation)
        rows = (df.withColumn("rank", rank)
                  .filter((F.col("rank") <= top_n) | (F.col("attack_flows") > 0)
                          | (F.col("flows") >= thr["conn"]) | (F.col("packets_per_sec") >= thr["pps"])
                          | (F.col("dst_port").isin(bf_ports) & (F.col("short_flows") >= thr["short"])))
                  .drop("window", "last_event", "rank")
                  .collect())
        processed_at = now_iso()
        docs, alerts = [], []
        for r in rows:
            row = r.asDict()
            alert = None
            # La force brute est prioritaire sur ses ports : sinon elle déclencherait aussi la règle DoS
            if (rules["brute_force"]["enabled"] and row["dst_port"] in bf_ports
                    and row["short_flows"] >= thr["short"]):
                alert = make_alert(cfg, "brute_force", row, row["dst_port"], "short_connections",
                                   row["short_flows"], thr["short"], processed_at)
            elif rules["ddos_dos"]["enabled"] and row["packets_per_sec"] >= thr["pps"]:
                alert = make_alert(cfg, "ddos_dos", row, row["dst_port"], "packets_per_second",
                                   row["packets_per_sec"], thr["pps"], processed_at)
            elif rules["ddos_dos"]["enabled"] and row["flows"] >= thr["conn"]:
                alert = make_alert(cfg, "ddos_dos", row, row["dst_port"], "connections",
                                   row["flows"], thr["conn"], processed_at)
            if alert:
                alerts.append(alert)
            row.update({
                "doc_id": f"{row['window_start']}|{row['dst_port']}",
                "flows_per_sec": row["flows"] / win_s,
                "alert_type": alert["alert_type"] if alert else None,
                "processed_at": processed_at,
                "latency_s": latency(processed_at, row["last_event_time"]),
            })
            docs.append(row)
        sink.send("ports_index", docs, "doc_id")
        sink.send("alerts_index", alerts, "alert_id")
        if alerts or batch_id % 10 == 0:
            print(f"[ports] batch {batch_id} : {len(docs)} lignes, {len(alerts)} alerte(s)")

    return handler


def process_sources(cfg, sink):
    """Règle scan de ports sur les agrégats (fenêtre, adresse source)."""
    thr = cfg["thr"]

    def handler(batch: DataFrame, batch_id: int):
        if not cfg["rules"]["port_scan"]["enabled"]:
            return
        rows = (with_window_strings(batch.filter(F.col("distinct_ports") >= thr["ports"]))
                .drop("window", "last_event").collect())
        processed_at = now_iso()
        alerts = [make_alert(cfg, "port_scan", r.asDict(), r["src_ip"], "distinct_ports",
                             r["distinct_ports"], thr["ports"], processed_at) for r in rows]
        sink.send("alerts_index", alerts, "alert_id")
        if alerts:
            print(f"[sources] batch {batch_id} : {len(alerts)} alerte(s) scan de ports")

    return handler


def process_traffic(cfg, sink):
    """Trafic global seconde par seconde (courbe du tableau de bord, mesure du débit et de la latence)."""

    def handler(batch: DataFrame, batch_id: int):
        rows = (with_window_strings(batch).drop("window", "last_event").collect())
        processed_at = now_iso()
        docs = []
        for r in rows:
            d = r.asDict()
            d.update({"doc_id": d["window_start"], "processed_at": processed_at,
                      "latency_s": latency(processed_at, d["last_event_time"])})
            docs.append(d)
        sink.send("traffic_index", docs, "doc_id")
        if docs and batch_id % 10 == 0:
            last = max(docs, key=lambda d: d["window_start"])
            print(f"[trafic] batch {batch_id} : {last['flows']:,} flux/s, "
                  f"latence {last['latency_s']:.1f} s")

    return handler


# ---------------------------------------------------------------- programme principal
def main():
    p = argparse.ArgumentParser()
    p.add_argument("--config", default="/opt/project/config/detection_rules.yaml")
    p.add_argument("--sink", choices=["es", "console"], default="es")
    p.add_argument("--starting-offsets", default="latest", choices=["latest", "earliest"],
                   help="où commencer au PREMIER lancement (ensuite : reprise au checkpoint)")
    p.add_argument("--reset", action="store_true", help="efface les checkpoints avant de démarrer")
    args = p.parse_args()

    cfg = load_config(args.config)
    st, win, k = cfg["streaming"], cfg["window"], cfg["kafka"]
    print(f"[config] seuils effectifs (débit {cfg['replay_rate']} flux/s) : {cfg['thr']}")

    checkpoints = os.path.join(st["checkpoint_dir"], args.sink)
    if args.reset:
        shutil.rmtree(checkpoints, ignore_errors=True)

    spark = (SparkSession.builder.appName("IDS-streaming-detect")
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.shuffle.partitions", st["shuffle_partitions"])
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    sink = Sink(cfg, args.sink)

    raw = (spark.readStream.format("kafka")
           .option("kafka.bootstrap.servers", k["bootstrap_servers"])
           .option("subscribe", k["flows_topic"])
           .option("startingOffsets", args.starting_offsets)
           .option("maxOffsetsPerTrigger", st["max_offsets_per_trigger"])
           .load())

    short_us = cfg["rules"]["brute_force"]["short_flow_duration_us"]
    flows = (raw.select(F.from_json(F.col("value").cast("string"), FLOW_SCHEMA).alias("f"))
                .select("f.*")
                .withColumn("event_time", F.col("event_time").cast("timestamp"))
                .filter(F.col("event_time").isNotNull() & F.col("dst_port").isNotNull())
                .withColumn("packets", F.coalesce(F.col("tot_fwd_pkts"), F.lit(0.0))
                            + F.coalesce(F.col("tot_bwd_pkts"), F.lit(0.0)))
                .withColumn("bytes", F.coalesce(F.col("totlen_fwd_pkts"), F.lit(0.0))
                            + F.coalesce(F.col("totlen_bwd_pkts"), F.lit(0.0)))
                .withColumn("is_short", (F.col("flow_duration") < short_us).cast("int"))
                .withColumn("is_attack", (F.col("label") != "Benign").cast("int"))
                .withColumn("attack_label", F.when(F.col("is_attack") == 1, F.col("label")))
                .withWatermark("event_time", win["watermark"]))

    # Indicateurs communs ; "attack_*" = vérité terrain, pour l'évaluation seulement
    def common_aggs():
        return [F.count(F.lit(1)).alias("flows"),
                F.sum("is_attack").alias("attack_flows"),
                F.collect_set("attack_label").alias("attack_labels"),
                F.max("event_time").alias("last_event")]

    sliding = F.window("event_time", win["duration"], win["slide"])
    by_port = (flows.groupBy(sliding, "dst_port")
                    .agg(*common_aggs(),
                         F.sum("packets").alias("packets"),
                         F.sum("bytes").alias("bytes"),
                         F.sum("is_short").alias("short_flows")))
    by_source = (flows.filter(F.col("src_ip").isNotNull())
                      .groupBy(sliding, "src_ip")
                      .agg(*common_aggs(), F.approx_count_distinct("dst_port").alias("distinct_ports")))
    per_second = (flows.groupBy(F.window("event_time", "1 second"))
                       .agg(*common_aggs(),
                            F.sum("packets").alias("packets"),
                            F.sum("bytes").alias("bytes"),
                            F.approx_count_distinct("dst_port").alias("distinct_ports")))

    queries = [("ports", by_port, process_ports(cfg, sink)),
               ("sources", by_source, process_sources(cfg, sink)),
               ("trafic", per_second, process_traffic(cfg, sink))]
    for name, df, handler in queries:
        (df.writeStream.queryName(name)
           .outputMode("update")      # résultats dès chaque micro-batch : faible latence
           .foreachBatch(handler)
           .option("checkpointLocation", os.path.join(checkpoints, name))
           .trigger(processingTime=st["trigger"])
           .start())
    print(f"[detect] {len(queries)} requêtes démarrées, sortie = {args.sink}, "
          f"checkpoints = {checkpoints}")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
