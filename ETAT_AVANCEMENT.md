# État d'avancement du projet — comparaison avec le cahier des charges

*Analyse du 14/11/2026 (semaine 4 sur 7 depuis le 23/10/2026).*

## 1. Résumé

| Objectif du cahier des charges | État | Avancement |
|---|---|---|
| 1. Batch Spark sur l'historique complet (~16 M flux) | ✅ Fait et exécuté | 100 % |
| 2. Simulation du trafic dans Kafka (≥ 1 000 flux/s) | 🟡 Code testé (dry-run OK), débit réel avec Kafka à mesurer | 80 % |
| 3. Détection temps réel avec Spark Structured Streaming | ❌ Pas commencé (`spark/streaming/` vide) | 0 % |
| 4. Tableau de bord Kibana | ❌ Services déclarés dans docker-compose, rien d'autre | 5 % |
| 5. Évaluation (détection, fausses alertes, débit, latence) | ❌ Pas commencé | 0 % |

**Avancement global estimé : environ 35 %.** Le socle (infrastructure, données, batch) est solide.
Le cœur du système, Spark Streaming, reste à écrire, et tout ce qui suit en dépend.

---

## 2. Ce qui est fait

### 2.1 Organisation et déploiement
- Dépôt GitHub en place (`miniyarlajili/ids-bigdata`), 5 commits, tout est poussé.
- `docker-compose.yml` complet : Kafka 3.8 (KRaft, sans Zookeeper, 3 partitions), Kafka UI,
  Spark 3.5.3 (1 master + 1 worker), Cassandra 4.1, Redis 7.2, Elasticsearch et Kibana 8.15.3.
- Image Spark personnalisée (`spark/Dockerfile`) déjà construite (`ids-spark:3.5.3`).
- `.gitignore` correct : les données ne sont pas versionnées.
- README détaillé avec un guide pas à pas et une section de dépannage.

### 2.2 Données (section 4 du cahier)
- Scripts de téléchargement : un jour (`scripts/download_sample.sh`) ou le jeu complet (`scripts/download_all.ps1`, avec reprise et vérification de l'espace disque).
- Jeu complet téléchargé puis converti. Les CSV bruts ont été supprimés après conversion (`--delete-raw`).

### 2.3 Traitement batch (objectif 1) : terminé
`spark/batch/clean_and_stats.py` a tourné sur **tout le jeu de données** :

| Étape | Lignes |
|---|---|
| Lignes brutes | 16 233 002 |
| Après suppression des en-têtes répétés | 16 232 943 (identique au cahier des charges ✅) |
| Après suppression des valeurs vides / NaN / infinies | 16 137 183 |
| Après suppression des doublons | 15 727 888 |
| Après rejet des 14 flux datés de 1970 | **15 727 874** |

- 8 colonnes constantes supprimées, 78 colonnes conservées.
- **CSV 6,9 Go → Parquet 2,0 Go** (environ 3,4 fois plus petit), partitionné par jour. Cet argument « Big Data » est à mettre dans le rapport.
- Durée totale : 16 min 27 s.
- Statistiques produites dans `reports/` : volume par jour, répartition des attaques, ports les plus visés, ports visés par type d'attaque, avec leurs graphiques PNG.

### 2.4 Ingestion Kafka (objectif 2) : code prêt
`producer/producer.py` :
- rejoue le Parquet **dans l'ordre chronologique**, jour par jour (économe en mémoire) ;
- **débit réglable** (`--rate`), avec affichage du débit réel toutes les 5 s ;
- options `--date`, `--limit` et `--dry-run` ;
- `acks="all"` pour ne perdre aucun message (fiabilité) ;
- ajoute `event_time` (l'heure du rejeu) pour le streaming et garde `label` pour l'évaluation.

### 2.5 Configuration des règles (exigence « Paramétrage »)
`config/detection_rules.yaml` : fenêtre de 10 s qui glisse de 5 s, watermark, seuils DDoS/DoS, scan de ports et force brute, gravité, anti-doublons, et paramètres de connexion à Kafka, Cassandra, Redis et Elasticsearch.

### 2.6 Bonus hors cahier des charges
Cassandra (schéma de 4 tables orienté requêtes, `config/cassandra/schema.cql`) et Redis sont prêts. Ils ne sont pas demandés par ce cahier des charges, mais le README les justifie par l'exigence « au moins 2 bases NoSQL ». Aucune donnée n'y est encore écrite.

---

## 3. Ce qui reste à faire

| Besoin du cahier des charges | Reste à faire |
|---|---|
| Lire Kafka avec Spark Structured Streaming | Tout |
| Indicateurs par fenêtre glissante (flux, paquets/s, ports distincts, connexions courtes) | Tout |
| Règles DDoS/DoS, scan de ports, force brute | Tout |
| Alerte avec type, heure et gravité | Tout |
| Envoi des flux et des alertes vers Elasticsearch | Tout |
| Tableau de bord Kibana : trafic/s, alertes par type, top ports et destinations, dernières alertes | Tout, plus l'export `.ndjson` dans `kibana/` |
| Points de contrôle (checkpoints) et reprise après redémarrage | Tout |
| Évaluation : taux de détection et fausses alertes par type d'attaque | Tout |
| Mesure du débit (≥ 1 000 flux/s) et de la latence | Tout |
| Rapport et démonstration | Tout |

---

## 4. Problèmes et points d'attention

> **Corrections du 14/11/2026 :**
> - ✅ Point 1 : le batch rejette les dates hors de la période de capture et le producteur les ignore. Les 14 lignes ont été déplacées
>   vers `data/processed/_rejected/` et les statistiques recalculées (nouvelle option `--stats-only`).
> - ✅ Point 2 : Elasticsearch et Kibana sont passés en 8.17.0 (images déjà présentes sur le poste). Les ports sont réglables
>   avec `ES_PORT` et `KIBANA_PORT` (voir le dépannage du README). L'autre projet reste à arrêter avant le lancement.
> - ✅ Point 6 : dépendances installées, producteur testé en `--dry-run`, accents corrigés dans la console Windows.
> - ✅ Point 7 : `kafka-ui` fixé en `v0.7.2`.
> - ⏳ Points 3, 4 et 5 : ils seront traités pendant le développement du streaming et de l'évaluation (semaines 4 à 6).

1. **14 flux datés de 1970** (partitions `date=1970-01-*`) : ce sont des horodatages invalides. Il faut les filtrer
   (par exemple `F.year("timestamp") >= 2018` dans le batch) ou au moins les ignorer dans le producteur.
   Sinon ils apparaissent dans `volume_par_jour` et seront rejoués en premier.

2. **Conflit de ports** : un autre projet (`bigdata-neuf-2026`) utilise déjà Elasticsearch (9200) et Kibana (5601).
   Avant `docker compose up`, lancer `docker stop bigdata-neuf-2026-kibana-1 bigdata-neuf-2026-elasticsearch-1`.
   Par ailleurs, les images 8.17.0 sont déjà présentes sur le poste alors que le compose demande 8.15.3 (environ 4 Go à retélécharger).
   Passer le compose en `8.17.0` évite ce téléchargement.

3. **Les seuils ne sont pas encore calibrés.** Le producteur remplace l'heure d'origine par l'heure du rejeu.
   À 1 000 flux/s, le trafic est donc fortement accéléré : le 14/02 compte environ 820 000 flux sur une journée, soit en moyenne
   une dizaine de flux par seconde à l'origine. Les seuils absolus du YAML (5 000 paquets/s, 2 000 connexions par fenêtre)
   dépendent donc du débit de rejeu. Pour les fixer, regarder les fenêtres **normales**
   (99e centile du trafic « Benign » par port et par fenêtre), éventuellement à l'aide d'un petit script batch.
   C'est la réponse directe à « limiter les fausses alertes ».

4. **Scan de ports** : CSE-CIC-IDS2018 ne contient **aucune étiquette « PortScan »**. La règle peut fonctionner,
   mais elle ne pourra pas être évaluée avec les étiquettes. Il faut le dire dans le rapport (limites).
   Sans adresse IP source, le nombre de ports distincts ne peut se calculer que pour l'ensemble du trafic. Les IP ne sont
   présentes que dans certains fichiers (en particulier celui du 20/02).

5. **Préparer l'évaluation dès le streaming.** Les étiquettes sont attachées aux flux, les alertes aux fenêtres.
   Dans chaque fenêtre, calculer aussi `nb_flux_attaque` et le label majoritaire à partir de `label`,
   **sans les utiliser dans les règles**. Ensuite :
   fenêtre attaquée et alerte = vrai positif ; fenêtre normale et alerte = fausse alerte ; fenêtre attaquée sans alerte = attaque manquée.

6. **Producteur non testé sur ce poste** : `pyarrow` n'est pas installé dans le Python 3.13 local.
   Lancer `pip install -r requirements.txt`, puis `python producer/producer.py --dry-run --limit 3`.

7. **Reproductibilité** : `provectuslabs/kafka-ui:latest` n'a pas de version fixe. La fixer (par exemple `v0.7.2`).

8. **Mémoire** : Kafka, Spark, Elasticsearch, Kibana, Cassandra et Redis sur un seul poste, c'est lourd. Pour la démonstration,
   si la RAM manque, arrêter Cassandra (le cahier des charges ne l'exige pas).

---

## 5. Plan pour avancer (semaines 4 à 7)

### Semaine 4 (14–20/11) : Spark Streaming minimal, priorité absolue
1. Installer les dépendances locales et tester le producteur :
   `docker compose up -d kafka kafka-ui`, puis `python producer/producer.py --rate 1000 --limit 60000 --date 2018-02-14`.
   Vérifier dans Kafka UI (http://localhost:8090) que le débit atteint bien 1 000 flux/s.
2. Créer `spark/streaming/detect.py` :
   - `readStream.format("kafka")` sur `network-flows` (package `org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3`) ;
   - `from_json` avec un schéma explicite, puis `event_time` converti en timestamp ;
   - `withWatermark` + `groupBy(window(event_time, "10 seconds", "5 seconds"), dst_port)` ;
   - indicateurs : `count`, somme des paquets / 10 s, `approx_count_distinct(dst_port)`, nombre de flux avec `flow_duration` inférieure au seuil ;
   - lecture des seuils dans `config/detection_rules.yaml` (PyYAML est déjà dans l'image).
   - D'abord vers la console (`format("console")`) pour valider.
3. Commencer par la règle **force brute** : le 14/02 contient FTP-BruteForce et SSH-Bruteforce, et c'est le jour le plus simple.

### Semaine 5 (21–27/11) : règles, Elasticsearch et Kibana
4. Ajouter les règles DDoS/DoS et scan de ports, ainsi que les champs type, heure, gravité et `alert_id` déterministe.
5. Écrire avec `foreachBatch` vers Elasticsearch (bulk `elasticsearch-py`, index `ids-traffic` et `ids-alerts`)
   avec `checkpointLocation` (exigence de fiabilité). Si le temps le permet, écrire aussi vers Cassandra et Redis dans le même `foreachBatch`.
6. Créer le tableau de bord Kibana (trafic par seconde, alertes par type, top ports, dernières alertes, rafraîchissement toutes les 5 s),
   puis l'exporter : *Stack Management → Saved Objects → Export* → `kibana/dashboard.ndjson`.

### Semaine 6 (28/11–04/12) : calibrage et évaluation
7. Calibrer les seuils (point 3 ci-dessus) en rejouant les jours 14/02 (force brute), 16/02 (DoS) et 20/02 ou 21/02 (DDoS).
8. Écrire `evaluation/evaluate.py` : alertes et fenêtres comparées aux étiquettes, puis tableau par type d'attaque
   (taux de détection, fausses alertes, précision).
9. Tests de performance : débit maximal (rejouer à 1 000, 2 000 puis 5 000 flux/s) et latence
   (`@timestamp` d'indexation moins `event_time`). Test de redémarrage : arrêter Spark en plein flux, le relancer
   et vérifier qu'aucun flux n'est perdu grâce au checkpoint.

### Semaine 7 (05–11/12) : finalisation
10. Corriger les points 1, 2 et 7 de la section 4. Vérifier que `docker compose up -d` lance tout d'un coup.
11. Rapport : contexte, problématique, architecture, réalisation (avec les chiffres du batch), évaluation, limites
    (étiquetage, absence de PortScan, IP manquantes, attaques discrètes comme l'infiltration) et perspectives (ML, cluster).
12. Préparer et répéter la démonstration : lancer le producteur sur le 14/02, puis montrer les alertes force brute dans Kibana.
