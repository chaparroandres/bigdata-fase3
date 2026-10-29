#!/usr/bin/env bash
# Descarga los casos positivos de COVID-19 en Colombia desde datos.gov.co (API SODA).
# Uso: ./scripts/download_data.sh [NUMERO_DE_REGISTROS]   (por defecto 1000000)
set -euo pipefail

MAX="${1:-1000000}"
PAGE=50000
OUT="data/casos_covid.csv"
BASE="https://www.datos.gov.co/resource/gt2j-8ykr.csv"

mkdir -p data
rm -f "$OUT"

for ((offset = 0; offset < MAX; offset += PAGE)); do
  url="${BASE}?\$limit=${PAGE}&\$offset=${offset}&\$order=%3Aid"
  echo "Descargando registros ${offset} a $((offset + PAGE))..."
  if [ "$offset" -eq 0 ]; then
    curl -sSfL "$url" > "$OUT"
  else
    # Se omite el encabezado de las páginas siguientes
    curl -sSfL "$url" | tail -n +2 >> "$OUT"
  fi
done

echo "Archivo generado: $OUT"
echo "Líneas: $(wc -l < "$OUT")"
ls -lh "$OUT"
