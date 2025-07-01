#!/usr/bin/env bash
# Uso: ./find_usgs.sh <lat> <lon>   (es. 38.033 -76.335)
LAT=$1; LON=$2; BOX=0.3             # raggio ±0.3° ≈ 30 km

W=$(awk -v l=$LON -v b=$BOX 'BEGIN{printf "%.3f", l-b}')
E=$(awk -v l=$LON -v b=$BOX 'BEGIN{printf "%.3f", l+b}')
S=$(awk -v a=$LAT -v b=$BOX 'BEGIN{printf "%.3f", a-b}')
N=$(awk -v a=$LAT -v b=$BOX 'BEGIN{printf "%.3f", a+b}')

URL="https://waterservices.usgs.gov/nwis/site/?format=rdb&bBox=${W},${S},${E},${N}&parameterCd=00400,63675&hasDataTypeCd=iv&siteStatus=active"

curl -s "$URL" | awk 'BEGIN{FS="\t"}
/^[0-9]/{
  if(index($0,"00400") && index($0,"63675"))
    printf "%s\t%s\t%s\n", $1, $5, $6   # site_id  lat  lon
}'
