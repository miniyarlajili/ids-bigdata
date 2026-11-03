"""
Traitement batch de l'historique CSE-CIC-IDS2018 avec Spark.

Étapes :
  1. Charger tous les CSV de data/raw/ (fichiers à 80 ou 84 colonnes)
  2. Nettoyer : lignes d'en-tête répétées, valeurs vides / NaN / infinies,
     doublons, colonnes constantes (inutiles)
  3. Enregistrer les données propres en Parquet (partitionné par jour)
  4. Produire les statistiques globales (CSV + graphiques PNG dans reports/)

Optimisé pour le volume complet (~16 millions de flux) :
  - les comptages du nettoyage et la recherche des colonnes constantes se font
    en UNE seule passe d'agrégation ;
  - les statistiques sont calculées sur le Parquet (colonnes compressées),
    pas en relisant les CSV.

Lancement (dans le conteneur Spark) :
  spark-submit --master spark://spark-master:7077 \
      spark/batch/clean_and_stats.py --input data/raw --output data/processed/flows
"""

import argparse
import glob
import json
import os
import re
import time

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Colonnes textuelles (toutes les autres sont numériques)
TEXT_COLS = {"timestamp", "label", "flow_id", "src_ip", "dst_ip"}
INT_COLS = {"dst_port", "src_port", "protocol"}
REQUIRED = ["timestamp", "label", "dst_port", "protocol"]


def to_snake(name: str) -> str:
    """'Flow Byts/s' -> 'flow_byts_s' (noms compatibles Parquet / Elasticsearch / Cassandra)."""
    name = re.sub(r"[^0-9a-zA-Z]+", "_", name.strip()).strip("_")
    return name.lower()


def load_csvs(spark: SparkSession, input_dir: str) -> DataFrame:
    files = sorted(glob.glob(os.path.join(input_dir, "*.csv")))
    if not files:
        raise SystemExit(f"Aucun fichier CSV trouvé dans {input_dir}")
    total_mb = sum(os.path.getsize(f) for f in files) / 1e6
    print(f"[load] {len(files)} fichier(s), {total_mb:,.0f} Mo :")
    df_all = None
    for f in files:
        print(f"   - {os.path.basename(f)}  ({os.path.getsize(f) / 1e6:,.0f} Mo)")
        df = spark.read.option("header", True).csv(f)  # tout en texte au départ
        df = df.toDF(*[to_snake(c) for c in df.columns])
        # Certains fichiers ont 84 colonnes (avec IP), d'autres 80 :
        df_all = df if df_all is None else df_all.unionByName(df, allowMissingColumns=True)
    return df_all


def clean(df: DataFrame, report: dict) -> DataFrame:
    # Ligne d'en-tête répétée au milieu de certains fichiers
    df = df.withColumn("_hdr", F.col("label") == "Label")

    # Typage des colonnes
    for c in df.columns:
        if c in INT_COLS:
            df = df.withColumn(c, F.col(c).cast("int"))
        elif c not in TEXT_COLS and c != "_hdr":
            df = df.withColumn(c, F.col(c).cast("double"))
    df = df.withColumn("timestamp", F.to_timestamp("timestamp", "dd/MM/yyyy HH:mm:ss"))

    # Valeurs vides, NaN ou infinies ("Infinity" dans les CSV)
    num_cols = [c for c in df.columns if c not in TEXT_COLS and c not in INT_COLS and c != "_hdr"]
    bad = F.lit(False)
    for c in num_cols:
        bad = bad | F.col(c).isNull() | F.isnan(c) | F.col(c).isin(float("inf"), float("-inf"))
    for c in REQUIRED:
        bad = bad | F.col(c).isNull()
    df = df.withColumn("_bad", bad)
    valid = (~F.col("_hdr")) & (~F.col("_bad"))

    # UNE passe : comptages + min/max de chaque colonne (pour repérer les colonnes constantes)
    aggs = [F.count(F.lit(1)).alias("raw"),
            F.sum(F.col("_hdr").cast("int")).alias("hdr"),
            F.sum(((~F.col("_hdr")) & F.col("_bad")).cast("int")).alias("invalid")]
    for c in num_cols:
        aggs += [F.min(F.when(valid, F.col(c))).alias(f"{c}__min"),
                 F.max(F.when(valid, F.col(c))).alias(f"{c}__max")]
    r = df.agg(*aggs).first().asDict()

    report["rows_raw"] = r["raw"]
    report["rows_after_header_removal"] = r["raw"] - (r["hdr"] or 0)
    report["rows_after_invalid_removal"] = report["rows_after_header_removal"] - (r["invalid"] or 0)
    constant = [c for c in num_cols if r[f"{c}__min"] == r[f"{c}__max"]]
    report["dropped_constant_columns"] = constant

    df = (df.filter(valid).drop("_hdr", "_bad", *constant)
            .dropDuplicates()
            .withColumn("is_attack", (F.col("label") != "Benign").cast("int"))
            .withColumn("date", F.to_date("timestamp")))
    return df


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

    # Graphiques (pour le rapport / la présentation)
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


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default="data/raw")
    parser.add_argument("--output", default="data/processed/flows")
    parser.add_argument("--reports", default="reports")
    args = parser.parse_args()
    # chemins absolus : les executors (autre conteneur) n'ont pas le même dossier courant
    args.input, args.output, args.reports = map(os.path.abspath, (args.input, args.output, args.reports))

    spark = (SparkSession.builder.appName("IDS-batch-clean-and-stats")
             .config("spark.sql.session.timeZone", "UTC")
             # une date mal formée devient NULL (ligne écartée) au lieu de faire échouer le job
             .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    t0 = time.time()

    report = {}
    df = clean(load_csvs(spark, args.input), report)

    # ~8 fichiers par jour : évite un fichier géant (jour du 20/02) et des milliers de petits fichiers
    (df.withColumn("_salt", (F.rand(seed=42) * 8).cast("int"))
       .repartition("date", "_salt").drop("_salt")
       .write.mode("overwrite").partitionBy("date").parquet(args.output))
    print(f"[parquet] données propres écrites dans {args.output}")
    t_write = time.time()

    # Les statistiques sont calculées sur le Parquet (beaucoup plus rapide que les CSV)
    clean_df = spark.read.parquet(args.output)
    report["rows_after_dedup"] = clean_df.count()
    report["columns_final"] = len(clean_df.columns)
    save_stats(clean_df, args.reports)

    def dir_mb(path):
        return round(sum(os.path.getsize(os.path.join(dp, f))
                         for dp, _, fs in os.walk(path) for f in fs) / 1e6, 1)
    report["size_csv_mb"] = round(sum(os.path.getsize(f) for f in glob.glob(os.path.join(args.input, "*.csv"))) / 1e6, 1)
    report["size_parquet_mb"] = dir_mb(args.output)
    report["duration_seconds"] = {
        "nettoyage_et_ecriture_parquet": round(t_write - t0, 1),
        "statistiques": round(time.time() - t_write, 1),
        "total": round(time.time() - t0, 1),
    }
    with open(os.path.join(args.reports, "rapport_nettoyage.json"), "w") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)
    print("\n=== Rapport de nettoyage ===")
    print(json.dumps(report, indent=2, ensure_ascii=False))
    spark.stop()


if __name__ == "__main__":
    main()
