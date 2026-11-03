"""
Producteur Kafka : rejoue les flux réseau nettoyés (Parquet) dans un topic Kafka,
dans l'ordre chronologique, à un débit réglable (flux par seconde).

Économe en mémoire (adapté aux ~16 millions de flux) : le Parquet est lu jour
par jour, seulement les colonnes utiles à la détection et à l'évaluation, puis
envoyé par paquets.

Exemples (depuis ta machine, Kafka lancé avec docker compose) :
  python producer/producer.py --rate 1000
  python producer/producer.py --rate 2000 --date 2018-02-14 --limit 100000
  python producer/producer.py --dry-run --limit 3      # test sans Kafka
"""

import argparse
import itertools
import json
import os
import time
from datetime import datetime, timezone

import pyarrow.compute as pc
import pyarrow.parquet as pq

# Colonnes envoyées : celles des règles de détection + l'étiquette (évaluation uniquement)
SEND_COLS = ["timestamp", "dst_port", "protocol", "flow_duration",
             "tot_fwd_pkts", "tot_bwd_pkts", "totlen_fwd_pkts", "totlen_bwd_pkts",
             "flow_byts_s", "flow_pkts_s",
             "syn_flag_cnt", "fin_flag_cnt", "rst_flag_cnt", "ack_flag_cnt",
             "src_ip", "src_port", "dst_ip", "label"]


def list_dates(path: str) -> list[str]:
    return sorted(d.split("=", 1)[1] for d in os.listdir(path) if d.startswith("date="))


def iter_flows(path: str, dates: list[str], chunk: int = 5000):
    """Génère les flux un par un, jour après jour, triés par heure."""
    for d in dates:
        part = os.path.join(path, f"date={d}")
        available = set(pq.read_schema(next(
            os.path.join(part, f) for f in os.listdir(part) if f.endswith(".parquet"))).names)
        cols = [c for c in SEND_COLS if c in available]
        table = pq.read_table(part, columns=cols).sort_by("timestamp")
        ts = pc.utf8_slice_codeunits(                       # "2018-02-14 10:15:32"
            pc.strftime(table["timestamp"], format="%Y-%m-%d %H:%M:%S"), 0, 19)
        table = table.set_column(table.schema.get_field_index("timestamp"), "timestamp", ts)
        print(f"[producer] jour {d} : {table.num_rows:,} flux")
        for batch in table.to_batches(max_chunksize=chunk):
            for r in batch.to_pylist():
                yield {k: v for k, v in r.items() if v is not None}


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--input", default="data/processed/flows")
    p.add_argument("--bootstrap", default="localhost:9092")
    p.add_argument("--topic", default="network-flows")
    p.add_argument("--rate", type=int, default=1000, help="flux par seconde")
    p.add_argument("--date", help="ne rejouer qu'un jour, ex. 2018-02-14")
    p.add_argument("--limit", type=int, help="nombre maximum de flux")
    p.add_argument("--dry-run", action="store_true", help="affiche au lieu d'envoyer")
    args = p.parse_args()

    dates = [args.date] if args.date else list_dates(args.input)
    print(f"[producer] {len(dates)} jour(s) à rejouer : {', '.join(dates)}")
    flows = iter_flows(args.input, dates)
    if args.limit:
        flows = itertools.islice(flows, args.limit)

    if args.dry_run:
        for r in itertools.islice(flows, 5):
            r["event_time"] = datetime.now(timezone.utc).isoformat()
            print(json.dumps(r))
        return

    from kafka import KafkaProducer
    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: str(k).encode("utf-8"),
        acks="all",            # fiabilité : message confirmé par le broker
        linger_ms=20,
        batch_size=64 * 1024,
    )

    tick = 0.1                                  # on envoie par paquets de 100 ms
    per_tick = max(1, int(args.rate * tick))
    sent, start, last_log = 0, time.time(), time.time()
    try:
        while True:
            t_batch = time.time()
            batch = list(itertools.islice(flows, per_tick))
            if not batch:
                break
            now = datetime.now(timezone.utc).isoformat()
            for r in batch:
                r["event_time"] = now           # heure de rejeu (utilisée par le streaming)
                producer.send(args.topic, key=r["dst_port"], value=r)
            sent += len(batch)
            elapsed = time.time() - t_batch
            if elapsed < tick:
                time.sleep(tick - elapsed)
            if time.time() - last_log >= 5:     # log toutes les 5 s
                last_log = time.time()
                print(f"[producer] {sent:,} flux envoyés — débit réel {sent / (last_log - start):,.0f} flux/s")
    except KeyboardInterrupt:
        print("\n[producer] arrêt demandé")
    finally:
        producer.flush()
        producer.close()
        total = time.time() - start
        print(f"[producer] terminé : {sent:,} flux en {total:.1f} s "
              f"({sent / max(total, 1e-9):,.0f} flux/s)")


if __name__ == "__main__":
    main()
