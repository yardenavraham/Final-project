import json
import logging
from datetime import datetime, timezone

import requests
from kafka import KafkaProducer
from kafka.errors import KafkaError

# ---------------- Config ----------------
GIS_URL = "https://gisn.tel-aviv.gov.il/GisOpenData/service.asmx/GetLayer"
LAYER_CODE = 835
PROJECTION = "itm"  # or "wgs84" for lat/lon

KAFKA_BROKER = "course-kafka:9092"
KAFKA_TOPIC = "telofun_raw_2"

HTTP_TIMEOUT = 60
RUN_ID = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("telofun_gis_to_kafka")


# ------------- Data Fetch -------------
def fetch_telofun_rows():
    resp = requests.get(
        GIS_URL,
        params={
            "layerCode": LAYER_CODE,
            "layerWhere": "",
            "xmin": "", "ymin": "", "xmax": "", "ymax": "",
            "projection": PROJECTION,
        },
        timeout=HTTP_TIMEOUT,
        headers={"User-Agent": "telofun-gis-producer/1.0"}
    )
    resp.raise_for_status()

    # Try to parse JSON
    try:
        data = resp.json()
    except ValueError:
        snippet = resp.text[:400].replace("\n", " ")
        raise RuntimeError(f"Unexpected non-JSON response. First 400 chars: {snippet}")

    # Handle ASP.NET pattern: {"d": "<json string>"}
    if isinstance(data, dict) and "d" in data:
        try:
            data = json.loads(data["d"])
        except Exception:
            data = data["d"]

    # Normalize to list of records
    if isinstance(data, dict):
        for key in ("Rows", "rows", "features", "data", "items", "Stations"):
            if key in data and isinstance(data[key], list):
                return data[key]
        return [data]
    if isinstance(data, list):
        return data

    return [{"payload": data}]


# ------------- Utilities -------------
def build_value(rec: dict) -> dict:
    now = datetime.now(timezone.utc)
    return {
        "_meta_source": "telaviv_gis_getlayer",
        "_meta_layer_code": LAYER_CODE,
        "_meta_projection": PROJECTION,
        "_meta_ingest_ts": now.isoformat(),
        "_meta_ingest_epoch": int(now.timestamp()),
        "_meta_run_id": RUN_ID,
        "payload": rec
    }


def pick_key(rec: dict):
    # Prefer StationID if available, then common alternatives
    for k in ("StationID", "station_id", "OBJECTID", "ID", "_id", "stationId"):
        if k in rec:
            return rec[k]
    # Some GIS payloads use nested attributes
    attrs = rec.get("Attributes")
    if isinstance(attrs, dict):
        for k in ("StationID", "station_id", "OBJECTID", "ID", "_id", "stationId"):
            if k in attrs:
                return attrs[k]
    return None


# ------------- Main -------------
def main():
    rows = fetch_telofun_rows()
    log.info(f"Fetched {len(rows)} records from GIS layer {LAYER_CODE} (projection={PROJECTION}); run_id={RUN_ID}")

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        acks="all",
        retries=5,
        compression_type="gzip",
        linger_ms=50,
        batch_size=64_000,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        key_serializer=lambda k: (str(k).encode("utf-8") if k is not None else None),
    )

    sent = 0
    for rec in rows:
        key = pick_key(rec)
        value = build_value(rec)
        try:
            producer.send(KAFKA_TOPIC, key=key, value=value)
            sent += 1
        except KafkaError as ke:
            log.error(f"Kafka send error: {ke}")

    producer.flush()
    producer.close()
    log.info(f"Done. Sent {sent} messages to topic '{KAFKA_TOPIC}'. run_id={RUN_ID}")


if __name__ == "__main__":
    main()
