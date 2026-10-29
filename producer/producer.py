"""
Producteur Kafka : rejoue les flux réseau nettoyés (Parquet) dans un topic Kafka,
dans l'ordre chronologique, à un débit réglable (flux par seconde).

Exemples (depuis ta machine, Kafka lancé avec docker compose) :
  python producer/producer.py --rate 1000
  python producer/producer.py --rate 2000 --date 2018-02-14 --limit 100000
  python producer/producer.py --dry-run --limit 3      # test sans Kafka
"""

import argparse
import json
import time
from datetime import datetime, timezone

import pandas as pd


def load_flows(path: str, date: str | None, limit: int | None) -> pd.DataFrame:
    filters = [("date", "=", date)] if date else None
    df = pd.read_parquet(path, filters=filters)
    df = df.sort_values("timestamp", kind="stable").reset_index(drop=True)
    if limit:
        df = df.head(limit)
    df["timestamp"] = df["timestamp"].astype(str)
    df["date"] = df["date"].astype(str)
    return df


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

    print(f"[producer] chargement de {args.input} ...")
    df = load_flows(args.input, args.date, args.limit)
    # on retire les champs vides (ex. IP absentes dans certains fichiers) -> JSON valide
    records = [{k: v for k, v in r.items() if pd.notna(v)} for r in df.to_dict(orient="records")]
    print(f"[producer] {len(records):,} flux à envoyer à {args.rate} flux/s "
          f"(~{len(records) / args.rate:.0f} s)")

    if args.dry_run:
        for r in records[:5]:
            r["event_time"] = datetime.now(timezone.utc).isoformat()
            print(json.dumps(r)[:300], "...")
        return

    from kafka import KafkaProducer
    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: str(k).encode("utf-8"),
        acks="all",            # fiabilité : confirmé par le broker
        linger_ms=20,
        batch_size=64 * 1024,
    )

    tick = 0.1                                  # on envoie par paquets de 100 ms
    per_tick = max(1, int(args.rate * tick))
    sent, start = 0, time.time()
    try:
        for i in range(0, len(records), per_tick):
            t_batch = time.time()
            now = datetime.now(timezone.utc).isoformat()
            for r in records[i:i + per_tick]:
                r["event_time"] = now           # heure de rejeu (utilisée par le streaming)
                producer.send(args.topic, key=r["dst_port"], value=r)
            sent += len(records[i:i + per_tick])
            elapsed = time.time() - t_batch
            if elapsed < tick:
                time.sleep(tick - elapsed)
            if sent % (args.rate * 5) < per_tick:   # log toutes les ~5 s
                real = sent / (time.time() - start)
                print(f"[producer] {sent:,} flux envoyés — débit réel {real:,.0f} flux/s")
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
