import json
from pathlib import Path
import pandas as pd
import boto3
import io
import pyarrow as pa
import pyarrow.parquet as pq

# 1. Read stations
stations = json.loads(Path("../apps/api/seed_data/stations_seoul.json").read_text(encoding="utf-8"))

# 2. Build station_master dataframe
rows = []
for s in stations:
    rows.append({
        "sta_id": str(s["sta_id"]),
        "sta_no": str(s["sta_id"]), # mock sta_no
        "sta_nm": s["sta_nm"],
        "hold_cnt": s["hold_cnt"],
        "lat": s["lat"],
        "lon": s["lon"],
        "grid_id": "mock_grid",
    })
df = pd.DataFrame(rows)

# 3. Connect S3
import os
s3 = boto3.client(
    "s3",
    endpoint_url=os.environ.get("MINIO_URL", "http://minio:9000"),
    aws_access_key_id="minioadmin",
    aws_secret_access_key="minioadmin",
)
bucket = "local-dev"

# 4. Upload station_master.parquet
buf = io.BytesIO()
df.to_parquet(buf, index=False)
s3.put_object(Bucket=bucket, Key="silver/station/station_master.parquet", Body=buf.getvalue())
print("Uploaded station_master.parquet")

# 5. Upload categories json
categories = sorted(df["sta_id"].tolist())
cat_json = json.dumps(categories).encode("utf-8")
s3.put_object(Bucket=bucket, Key="models/lightgbm/rental_station_categories.json", Body=cat_json)
s3.put_object(Bucket=bucket, Key="models/lightgbm/return_station_categories.json", Body=cat_json)
print("Uploaded categories JSON")
