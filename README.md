# Analyse Big Data pour la détection en temps réel des cyberattaques dans les réseaux informatiques

Pipeline : **CSE-CIC-IDS2018 → Kafka → Spark Structured Streaming → Cassandra + Redis + Elasticsearch → Kibana**,
avec un traitement batch Spark de l'historique (nettoyage, Parquet, statistiques, calibrage des seuils).

| Brique | Rôle |
|---|---|
| Kafka | Ingestion du trafic (`network-flows`) et diffusion des alertes (`ids-alerts`) |
| Spark | Batch (historique) et Structured Streaming (détection sur fenêtres de 10 s) |
| **Cassandra** (NoSQL colonnes) | Historique : métriques par fenêtre, alertes — `config/cassandra/schema.cql` |
| **Redis** (NoSQL clé-valeur) | État temps réel : dernières alertes, compteurs, seuils actifs, anti-doublons |
| Elasticsearch + Kibana | Recherche des alertes et tableau de bord en direct |

## Structure du projet

```
ids-bigdata/
├── docker-compose.yml          # tout l'environnement en une commande
├── config/detection_rules.yaml # seuils des règles (modifiables sans toucher au code)
├── spark/
│   ├── Dockerfile              # image Spark 3.5.3 + pandas/matplotlib
│   ├── batch/clean_and_stats.py   # phase 1 : nettoyage + Parquet + statistiques
│   └── streaming/              # phase 2 : détection temps réel (à venir)
├── producer/producer.py        # rejeu du trafic dans Kafka (débit réglable)
├── scripts/download_sample.sh  # téléchargement d'un jour du dataset
├── data/raw/                   # CSV bruts (non versionnés)
├── data/processed/             # Parquet propres (non versionnés)
├── reports/                    # statistiques et graphiques générés
└── kibana/                     # export du tableau de bord (à venir)
```

## Avancement

| Étape (projet final) | Contenu | État |
|---|---|---|
| 1. Conception | Architecture, justification des choix, modélisation des données, points critiques | ✅ dossier de conception |
| 2. Déploiement | Nettoyage batch + Parquet + statistiques | ✅ code prêt |
| 2. Déploiement | Producteur Kafka (rejeu chronologique, débit réglable) | ✅ code prêt |
| 2. Déploiement | Spark Structured Streaming + règles DDoS / scan / brute force | ⏳ |
| 3. Intégration NoSQL | Cassandra + Redis (services et schéma prêts), écriture depuis Spark | ⏳ |
| 4. Tests | Performance (débit, latence) et tolérance aux pannes (RTO) | ⏳ |

---

# Guide pas à pas — séance 1

## Étape 0 — Prérequis (15 min)

- **Docker Desktop** installé et lancé. Dans *Settings → Resources*, donner **au moins 6 Go de RAM**.
- **Git** et un compte **GitHub**.
- **Python 3.10+** (pour le producteur Kafka uniquement).
- ~5 Go d'espace disque libre.

Vérifier :
```bash
docker --version
docker compose version
git --version
python --version
```

## Étape 1 — Créer le dépôt GitHub (10 min)

1. Sur GitHub : *New repository* → nom `ids-bigdata` → *Create* (sans README).
2. Dans le dossier du projet :
```bash
git init
git add .
git commit -m "Initialisation : structure du projet, docker-compose, script batch Spark"
git branch -M main
git remote add origin https://github.com/<ton-compte>/ids-bigdata.git
git push -u origin main
```

## Étape 2 — Télécharger un échantillon du dataset (10–20 min)

On développe d'abord sur **un seul jour** (mercredi 14/02/2018, ~1 million de flux, attaques
FTP-BruteForce et SSH-Bruteforce). Le volume complet viendra quand le pipeline sera stable.

**Linux / macOS / Git Bash :**
```bash
bash scripts/download_sample.sh
```

**Windows PowerShell :**
```powershell
mkdir data\raw -Force
curl.exe -L -o data\raw\Wednesday-14-02-2018_TrafficForML_CICFlowMeter.csv "https://cse-cic-ids2018.s3.ca-central-1.amazonaws.com/Processed%20Traffic%20Data%20for%20ML%20Algorithms/Wednesday-14-02-2018_TrafficForML_CICFlowMeter.csv"
```

Si le téléchargement échoue : le même fichier existe sur Kaggle (dataset « IDS 2018 Intrusion CSVs
(CSE-CIC-IDS2018) »). Le déposer dans `data/raw/`.

Liste des fichiers disponibles : `aws s3 ls --no-sign-request "s3://cse-cic-ids2018/Processed Traffic Data for ML Algorithms/"`

## Étape 3 — Lancer Spark avec Docker (10 min)

```bash
docker compose build spark-master
docker compose up -d spark-master spark-worker
docker compose ps
```

Ouvrir **http://localhost:8080** : l'interface du master Spark doit afficher **1 worker** (ALIVE).
📸 *Capture d'écran pour l'enseignante.*

## Étape 4 — Exécuter le traitement batch (5–10 min de calcul)

```bash
docker compose exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  --driver-memory 1g --executor-memory 1500m \
  spark/batch/clean_and_stats.py \
  --input data/raw --output data/processed/flows --reports reports
```
(Sous PowerShell, tout écrire sur une seule ligne sans les `\`.)

Pendant le calcul, ouvrir **http://localhost:4040** pour voir les jobs et les étapes Spark.
📸 *Capture d'écran.*

Le script :
1. charge tous les CSV de `data/raw/` (gère les fichiers à 80 et à 84 colonnes) ;
2. supprime les en-têtes répétés, les valeurs vides/NaN/infinies, les doublons et les colonnes constantes ;
3. écrit les données propres en **Parquet partitionné par jour** dans `data/processed/flows/` ;
4. produit dans `reports/` :
   - `rapport_nettoyage.json` : nombre de lignes à chaque étape, colonnes supprimées, durée ;
   - `volume_par_jour.csv/.png`, `repartition_labels.csv/.png`, `top_ports.csv/.png`, `top_ports_attaques.csv`.

Comparer la taille CSV vs Parquet (argument « Big Data » pour le rapport) :
```bash
du -sh data/raw data/processed/flows
```

## Étape 5 — Versionner les résultats

```bash
git add reports/ README.md
git commit -m "Phase 1 : nettoyage batch Spark, export Parquet et statistiques"
git push
```

## Étape 6 (bonus) — Rejouer le trafic dans Kafka

```bash
docker compose up -d kafka kafka-ui
pip install -r requirements.txt
python producer/producer.py --rate 1000 --limit 60000
```
Ouvrir **http://localhost:8090** → *Topics* → `network-flows` → *Messages* : les flux arrivent en direct.
📸 *Capture d'écran.*

Le débit réel s'affiche toutes les 5 secondes (objectif du cahier des charges : ≥ 1 000 flux/s).

## Étape 7 — Démarrer les bases NoSQL (Cassandra + Redis)

```bash
docker compose up -d cassandra redis
docker compose ps            # attendre que cassandra soit "healthy" (1 à 2 min)
```

Créer le schéma Cassandra (keyspace `ids` et ses 4 tables) :
```bash
docker compose exec cassandra cqlsh -f /schema/schema.cql
docker compose exec cassandra cqlsh -e "DESCRIBE TABLES" -k ids
```

Tester Redis :
```bash
docker compose exec redis redis-cli PING                 # réponse : PONG
docker compose exec redis redis-cli HSET ids:thresholds brute_force 50
docker compose exec redis redis-cli HGETALL ids:thresholds
```
📸 *Captures d'écran : les tables Cassandra et la réponse de Redis.*

---

## Dépannage

| Problème | Solution |
|---|---|
| `port is already allocated` | Un autre programme utilise le port (8080, 9092…). Le fermer ou changer le port de gauche dans `docker-compose.yml`. |
| Le job Spark s'arrête avec `OutOfMemory` | Augmenter la RAM de Docker Desktop, ou réduire `--executor-memory`. |
| Aucun worker sur http://localhost:8080 | `docker compose logs spark-worker` puis `docker compose restart spark-worker`. |
| `Aucun fichier CSV trouvé` | Vérifier que le CSV est bien dans `data/raw/` à la racine du projet. |
| `NoBrokersAvailable` (producteur) | Kafka met ~20 s à démarrer : attendre puis relancer. Vérifier `docker compose logs kafka`. |
| Script `.sh` qui échoue sous Windows | Utiliser la commande PowerShell de l'étape 2. |

Arrêter tout : `docker compose down` (les données de `data/` sont conservées).

## Source des données

CSE-CIC-IDS2018 — Canadian Institute for Cybersecurity & CSE, bucket public `s3://cse-cic-ids2018` (région ca-central-1).
