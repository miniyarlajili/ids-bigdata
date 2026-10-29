#!/usr/bin/env bash
# Télécharge UN jour du jeu CSE-CIC-IDS2018 (échantillon de développement).
# Par défaut : mercredi 14/02/2018 (~1 million de flux, attaques FTP/SSH brute force, ~350 Mo).
# Usage : bash scripts/download_sample.sh [NomDuFichier.csv]

set -e
FILE="${1:-Wednesday-14-02-2018_TrafficForML_CICFlowMeter.csv}"
PREFIX="Processed Traffic Data for ML Algorithms"
mkdir -p data/raw

if command -v aws >/dev/null 2>&1; then
  echo "[download] via AWS CLI (aucun compte nécessaire)"
  aws s3 cp --no-sign-request --region ca-central-1 \
    "s3://cse-cic-ids2018/${PREFIX}/${FILE}" "data/raw/${FILE}"
else
  echo "[download] via HTTPS (curl)"
  curl -L --fail -o "data/raw/${FILE}" \
    "https://cse-cic-ids2018.s3.ca-central-1.amazonaws.com/Processed%20Traffic%20Data%20for%20ML%20Algorithms/${FILE}"
fi

echo "[download] terminé :"
ls -lh "data/raw/${FILE}"
echo "Nombre de lignes :"; wc -l "data/raw/${FILE}"
