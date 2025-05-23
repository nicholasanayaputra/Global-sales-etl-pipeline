#!/bin/bash
set -e

TRANSACTIONS_TYPE=$1   # misal: "transactions" atau "customers"
START=$2               # angka awal
END=$3                 # angka akhir

URL_PREFIX="https://github.com/nicholasanayaputra/dataset/releases/download"

for NUMBER in $(seq $START $END); do
  FILE_NAME="${TRANSACTIONS_TYPE}_${NUMBER}.csv.gz"
  URL="${URL_PREFIX}/${TRANSACTIONS_TYPE}/${FILE_NAME}"

  LOCAL_PREFIX="data/raw/${TRANSACTIONS_TYPE}/${NUMBER}"
  LOCAL_PATH="${LOCAL_PREFIX}/${FILE_NAME}"

  echo "Downloading ${URL} to ${LOCAL_PATH}"
  mkdir -p "${LOCAL_PREFIX}"
  curl -L "${URL}" -o "${LOCAL_PATH}"
done
