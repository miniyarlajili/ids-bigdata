"""
Traitement batch de l'historique CSE-CIC-IDS2018 avec Spark.

Conçu pour un poste avec PEU D'ESPACE DISQUE : le travail est découpé pour que
Spark n'ait jamais besoin de trier les ~16 millions de flux d'un seul coup.

  Phase A - Conversion (fichier par fichier, sans tri) :
      chaque CSV est lu, typé et écrit en Parquet de travail (_staging),
      partitionné par jour. Aucun fichier temporaire de tri.
  Phase B - Bilan du nettoyage (lecture du Parquet, très rapide) :
      lignes d'en-tête répétées, valeurs vides/NaN/infinies, colonnes constantes.
  Phase C - Dédoublonnage (jour par jour) :
      les doublons sont forcément dans le même jour (même horodatage), donc on
      trie un jour à la fois : l'espace temporaire nécessaire est celui d'un jour.
  Phase D - Statistiques globales (CSV + graphiques PNG dans reports/).

Lancement (dans le conteneur Spark) :
  spark-submit --master spark://spark-master:7077 \
      spark/batch/clean_and_stats.py --input data/raw --output data/processed/flows
"""

import argparse
import glob
import json
import os
import re
import shutil
import time

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Colonnes textuelles (toutes les autres sont numériques)
TEXT_COLS = {"timestamp", "label", "flow_id", "src_ip", "dst_ip"}
INT_COLS = {"dst_port", "src_port", "protocol"}
REQUIRED = ["timestamp", "label", "dst_port", "protocol"]
FLAGS = ["_hdr", "_bad"]


def valid():
    """Ligne gardée : ni en-tête répétée, ni valeur invalide."""
    return (~F.col("_hdr")) & (~F.col("_bad"))


def to_snake(name: str) -> str:
    """'Flow Byts/s' -> 'flow_byts_s' (noms compatibles Parquet / Elasticsearch / Cassandra)."""
    name = re.sub(r"[^0-9a-zA-Z]+", "_", name.strip()).strip("_")
    return name.lower()


def numeric_cols(columns):
    return [c for c in columns if c not in TEXT_COLS and c not in INT_COLS
            and c not in FLAGS and c != "date"]


def typed_with_flags(spark: SparkSession, path: str) -> DataFrame:
    """Lit un CSV, renomme et type les colonnes, ajoute les drapeaux _hdr / _bad."""
    df = spark.read.option("header", True).csv(path)          # tout en texte au départ
    df = df.toDF(*[to_snake(c) for c in df.columns])
    df = df.withColumn("_hdr", F.col("label") == "Label")     # en-tête répétée dans le fichier
    for c in df.columns:
        if c in INT_COLS:
            df = df.withColumn(c, F.col(c).cast("int"))
        elif c not in TEXT_COLS and c not in FLAGS:
            df = df.withColumn(c, F.col(c).cast("double"))
    df = df.withColumn("timestamp", F.to_timestamp("timestamp", "dd/MM/yyyy HH:mm:ss"))

    bad = F.lit(False)                                          # vide, NaN ou infini
    for c in numeric_cols(df.columns):
        bad = bad | F.col(c).isNull() | F.isnan(c) | F.col(c).isin(float("inf"), float("-inf"))
    for c in REQUIRED:
        bad = bad | F.col(c).isNull()
    return df.withColumn("_bad", bad).withColumn("date", F.to_date("timestamp"))


def phase_a_convert(spark, files, staging, delete_raw):
    """Chaque CSV va dans son propre sous-dossier source_file=<nom>.
    Un fichier déjà converti (marqueur _SUCCESS de Spark) n'est pas refait :
    on peut relancer le script après une interruption sans tout recommencer."""
    print("\n=== Phase A : conversion CSV -> Parquet de travail (fichier par fichier) ===")
    for i, f in enumerate(files, 1):
        stem = os.path.splitext(os.path.basename(f))[0]
        out = os.path.join(staging, f"source_file={stem}")
        if os.path.exists(os.path.join(out, "_SUCCESS")):
            print(f"   [{i}/{len(files)}] {stem} déjà converti")
        else:
            t = time.time()
            shutil.rmtree(out, ignore_errors=True)               # reste d'une conversion interrompue
            (typed_with_flags(spark, f)
                .write.mode("overwrite").partitionBy("date").parquet(out))
            print(f"   [{i}/{len(files)}] {stem} converti en {time.time() - t:,.0f} s")
        if delete_raw:
            os.remove(f)                                         # re-téléchargeable : scripts/download_all.ps1
            print(f"         CSV supprimé pour libérer l'espace disque")


def phase_b_audit(spark, staging, report):
    print("\n=== Phase B : bilan du nettoyage ===")
    st = spark.read.option("mergeSchema", True).parquet(staging)
    num = numeric_cols(st.columns)
    aggs = [F.count(F.lit(1)).alias("raw"),
            F.sum(F.col("_hdr").cast("int")).alias("hdr"),
            F.sum(((~F.col("_hdr")) & F.col("_bad")).cast("int")).alias("invalid")]
    for c in num:
        aggs += [F.min(F.when(valid(), F.col(c))).alias(f"{c}__min"),
                 F.max(F.when(valid(), F.col(c))).alias(f"{c}__max")]
    r = st.agg(*aggs).first().asDict()
    report["rows_raw"] = r["raw"]
    report["rows_after_header_removal"] = r["raw"] - (r["hdr"] or 0)
    report["rows_after_invalid_removal"] = report["rows_after_header_removal"] - (r["invalid"] or 0)
    constant = [c for c in num if r[f"{c}__min"] == r[f"{c}__max"]]
    report["dropped_constant_columns"] = constant
    print(f"   {r['raw']:,} lignes lues, {r['hdr'] or 0:,} en-têtes répétées, "
          f"{r['invalid'] or 0:,} lignes invalides, {len(constant)} colonnes constantes")
    return st, constant


def phase_c_dedup(st, constant, output, report):
    print("\n=== Phase C : suppression des doublons (jour par jour) ===")
    shutil.rmtree(output, ignore_errors=True)
    dates = sorted(r["date"] for r in st.filter(valid() & F.col("date").isNotNull())
                   .select("date").distinct().collect())
    for d in dates:
        t = time.time()
        day = (st.filter((F.col("date") == F.lit(d)) & valid())
                 .drop(*FLAGS, "source_file", *constant)
                 .dropDuplicates()
                 .withColumn("is_attack", (F.col("label") != "Benign").cast("int")))
        (day.coalesce(8)                                     # ~8 fichiers par jour, sans nouveau tri
            .write.mode("overwrite").partitionBy("date").parquet(output))
        print(f"   {d} traité en {time.time() - t:,.0f} s")


def save_stats(df: DataFrame, reports_dir: str) -> None:
    os.makedirs(reports_dir, exist_ok=True)

    per_day = (df.groupBy("date")
                 .agg(F.count("*").alias("flows"), F.sum("is_attack").alias("attacks"))
                 .orderBy("date").toPandas())
    per_label = (df.groupBy("label").count()
                   .withColumn("percent", F.round(
                       F.col("count") * 100 / F.sum("count").over(Window.partitionBy()), 2))
                   .orderBy(F.desc("count")).toPandas())
    top_ports_all = (df.groupBy("dst_port").count().orderBy(F.desc("count")).limit(10).toPandas())
    top_ports_attacks = (df.filter("is_attack = 1").groupBy("dst_port", "label").count()
                           .orderBy(F.desc("count")).limit(10).toPandas())

    per_day.to_csv(f"{reports_dir}/volume_par_jour.csv", index=False)
    per_label.to_csv(f"{reports_dir}/repartition_labels.csv", index=False)
    top_ports_all.to_csv(f"{reports_dir}/top_ports.csv", index=False)
    top_ports_attacks.to_csv(f"{reports_dir}/top_ports_attaques.csv", index=False)

    print("\n=== Volume par jour ===\n", per_day.to_string(index=False))
    print("\n=== Répartition des étiquettes ===\n", per_label.to_string(index=False))
    print("\n=== 10 ports les plus visés (tout trafic) ===\n", top_ports_all.to_string(index=False))
    print("\n=== 10 couples port/attaque les plus fréquents ===\n", top_ports_attacks.to_string(index=False))

    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt

        fig, ax = plt.subplots(figsize=(9, 5))
        p = per_label.sort_values("count")
        ax.barh(p["label"], p["count"], color=["#2a9d8f" if l == "Benign" else "#e76f51" for l in p["label"]])
        ax.set_xscale("log")
        ax.set_title("Répartition des flux par étiquette (échelle log)")
        ax.set_xlabel("Nombre de flux")
        fig.tight_layout(); fig.savefig(f"{reports_dir}/repartition_labels.png", dpi=120); plt.close(fig)

        fig, ax = plt.subplots(figsize=(10, 4))
        d = per_day.copy(); d["date"] = d["date"].astype(str)
        ax.bar(d["date"], d["flows"] - d["attacks"], label="Normal", color="#2a9d8f")
        ax.bar(d["date"], d["attacks"], bottom=d["flows"] - d["attacks"], label="Attaque", color="#e76f51")
        ax.set_title("Volume de flux par jour"); ax.legend(); plt.xticks(rotation=30)
        fig.tight_layout(); fig.savefig(f"{reports_dir}/volume_par_jour.png", dpi=120); plt.close(fig)

        fig, ax = plt.subplots(figsize=(9, 4))
        t = top_ports_all.sort_values("count")
        ax.barh(t["dst_port"].astype(str), t["count"], color="#264653")
        ax.set_title("10 ports de destination les plus visés"); ax.set_xlabel("Nombre de flux")
        fig.tight_layout(); fig.savefig(f"{reports_dir}/top_ports.png", dpi=120); plt.close(fig)
        print(f"\n[stats] graphiques enregistrés dans {reports_dir}/")
    except ImportError:
        print("[stats] matplotlib absent : graphiques non générés")


def dir_mb(path):
    return round(sum(os.path.getsize(os.path.join(dp, f))
                     for dp, _, fs in os.walk(path) for f in fs) / 1e6, 1)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default="data/raw")
    parser.add_argument("--output", default="data/processed/flows")
    parser.add_argument("--staging", default="data/processed/_staging")
    parser.add_argument("--reports", default="reports")
    parser.add_argument("--keep-staging", action="store_true", help="ne pas effacer le Parquet de travail")
    parser.add_argument("--delete-raw", action="store_true",
                        help="supprimer chaque CSV dès sa conversion (manque d'espace disque)")
    args = parser.parse_args()
    # chemins absolus : les executors (autre conteneur) n'ont pas le même dossier courant
    for k in ("input", "output", "staging", "reports"):
        setattr(args, k, os.path.abspath(getattr(args, k)))

    files = sorted(glob.glob(os.path.join(args.input, "*.csv")))
    # Taille des CSV mémorisée dans le dossier de travail (ils peuvent être supprimés ensuite)
    sizes_path = os.path.join(args.staging, "_csv_sizes.json")
    sizes = json.load(open(sizes_path)) if os.path.exists(sizes_path) else {}
    sizes.update({os.path.basename(f): os.path.getsize(f) for f in files})
    os.makedirs(args.staging, exist_ok=True)
    json.dump(sizes, open(sizes_path, "w"), indent=2)
    if not files and not glob.glob(os.path.join(args.staging, "source_file=*")):
        raise SystemExit(f"Aucun fichier CSV trouvé dans {args.input}")
    print(f"[load] {len(files)} fichier(s) CSV à traiter, {sum(map(os.path.getsize, files)) / 1e6:,.0f} Mo :")
    for f in files:
        print(f"   - {os.path.basename(f)}  ({os.path.getsize(f) / 1e6:,.0f} Mo)")

    spark = (SparkSession.builder.appName("IDS-batch-clean-and-stats")
             .config("spark.sql.session.timeZone", "UTC")
             # une date mal formée devient NULL (ligne écartée) au lieu de faire échouer le job
             .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
             # fichiers temporaires de tri compressés plus fortement (moins d'espace disque)
             .config("spark.io.compression.codec", "zstd")
             # chaque jour réécrit seulement sa propre partition
             .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
             # tolère les longues pauses mémoire de l'executor au lieu de le déclarer perdu
             .config("spark.network.timeout", "600s")
             .config("spark.executor.heartbeatInterval", "60s")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    t0 = time.time()
    report, durations = {}, {}

    phase_a_convert(spark, files, args.staging, args.delete_raw)
    durations["A_conversion"] = round(time.time() - t0, 1)
    report["size_staging_mb"] = dir_mb(args.staging)

    t = time.time()
    st, constant = phase_b_audit(spark, args.staging, report)
    durations["B_bilan"] = round(time.time() - t, 1)

    t = time.time()
    phase_c_dedup(st, constant, args.output, report)
    durations["C_doublons"] = round(time.time() - t, 1)

    t = time.time()
    clean_df = spark.read.parquet(args.output)
    report["rows_after_dedup"] = clean_df.count()
    report["columns_final"] = len(clean_df.columns)
    save_stats(clean_df, args.reports)
    durations["D_statistiques"] = round(time.time() - t, 1)

    if not args.keep_staging:
        shutil.rmtree(args.staging, ignore_errors=True)   # libère l'espace du Parquet de travail

    report["size_csv_mb"] = round(sum(sizes.values()) / 1e6, 1)
    report["size_parquet_mb"] = dir_mb(args.output)
    durations["total"] = round(time.time() - t0, 1)
    report["duration_seconds"] = durations
    with open(os.path.join(args.reports, "rapport_nettoyage.json"), "w") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)
    print("\n=== Rapport de nettoyage ===")
    print(json.dumps(report, indent=2, ensure_ascii=False))
    spark.stop()


if __name__ == "__main__":
    main()
