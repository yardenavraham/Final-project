import json
import requests
from kafka import KafkaProducer

# API + Kafka basics
GIS_URL = "https://gisn.tel-aviv.gov.il/GisOpenData/service.asmx/GetLayer"
LAYER_CODE = 835
PROJECTION = "itm"   # or "wgs84"
KAFKA_BROKER = "course-kafka:9092"
KAFKA_TOPIC = "telofun_raw"
HTTP_TIMEOUT = 60

def fetch_rows():
    r = requests.get(
        GIS_URL,
        params={
            "layerCode": LAYER_CODE,
            "layerWhere": "",
            "xmin": "", "ymin": "", "xmax": "", "ymax": "",
            "projection": PROJECTION,
        },
        timeout=HTTP_TIMEOUT,
    )
    r.raise_for_status()
    data = r.json()

    # Unwrap ASP.NET format: {"d": "...json..."}
    if isinstance(data, dict) and "d" in data:
        try:
            data = json.loads(data["d"])
        except Exception:
            pass

    # Return the list of records
    if isinstance(data, dict):
        for key in ("Rows", "rows", "features", "data", "items", "Stations"):
            if key in data and isinstance(data[key], list):
                return data[key]
        return [data]
    if isinstance(data, list):
        return data
    return []

def main():
    rows = fetch_rows()
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
    )
    for rec in rows:
        producer.send(KAFKA_TOPIC, value=rec)
    producer.flush()
    producer.close()
    print(f"sent {len(rows)} messages")

if __name__ == "__main__":
    main()
