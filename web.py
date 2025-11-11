from flask import Flask, jsonify
from google.cloud import storage
from datetime import datetime, timezone
import os, glob

import territory_checks  # your script

app = Flask(__name__)

CURATED_BUCKET = os.environ["CURATED_BUCKET"]  # gs bucket for final excel
GCS_PREFIX     = os.getenv("GCS_PREFIX", "territory-checks/weekly")

def upload_to_gcs(local_path: str, bucket: str, dest_path: str) -> str:
    client = storage.Client()
    blob = client.bucket(bucket).blob(dest_path)
    blob.cache_control = "no-cache"
    blob.upload_from_filename(local_path)
    return f"gs://{bucket}/{dest_path}"

@app.route("/", methods=["GET", "POST"])
def run_now():
    # run your existing job (writes Excel to OUT_DIR=/tmp)
    territory_checks.run()

    # pick the newest Excel created by the job
    candidates = sorted(glob.glob("/tmp/Territory_Checks_*.xlsx"))
    if not candidates:
        return jsonify({"status":"no_output","message":"No Excel produced"}), 200

    latest = candidates[-1]

    # name in GCS: territory-checks/weekly/YYYY/MM/DD/<filename>
    now = datetime.now(timezone.utc)
    dest = f"{GCS_PREFIX}/{now:%Y/%m/%d}/{os.path.basename(latest)}"
    uri = upload_to_gcs(latest, CURATED_BUCKET, dest)

    return jsonify({"status":"ok","excel_uri":uri,"local_file":latest}), 200
