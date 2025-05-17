
set -e

TRANSACTIONS_TYPE=$1 # "transactions"
NUMBER=$1

URL_PREFIX="https://github.com/nicholasanayaputra/dataset/releases/download"

for MONTH in {1..11}; do
  FMONTH=`printf "%02d" ${MONTH}`

  URL="${URL_PREFIX}/${TRANSACTIONS_TYPE}/${TRANSACTIONS_TYPE}_RETAIL_${NUMBER}.csv.gz"

  LOCAL_PREFIX="data/raw/${TRANSACTIONS_TYPE}/${NUMBER}"
  LOCAL_FILE="${TRANSACTIONS_TYPE}_tripdata_${NUMBER}.csv.gz"
  LOCAL_PATH="${LOCAL_PREFIX}/${LOCAL_FILE}"

  echo "downloading ${URL} to ${LOCAL_PATH}"
  mkdir -p ${LOCAL_PREFIX}
  curl -L ${URL} -o ${LOCAL_PATH}

done