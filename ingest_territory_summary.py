# ingest_territory_summary.py
import os, io, re, sys
from datetime import datetime, timezone, timedelta
from urllib.parse import urlparse
import pandas as pd

from google.cloud import storage, bigquery

RAW_PREFIX = os.getenv("RAW_PREFIX", "gs://sfs-raw-us/territory-checks/weekly")

PROJECT   = os.getenv("GCP_PROJECT", "sfs-data-lake")
DATASET   = os.getenv("BQ_DATASET", "sfs_silver")
TABLE     = os.getenv("BQ_TABLE",   "territory_summary")

# --- helpers ---------------------------------------------------------------

def _list_latest_xlsx(gcs_prefix:str) -> str:
    """
    Return the GCS URI of the newest Territory_Checks_*.xlsx under the prefix.
    """
    u = urlparse(gcs_prefix)
    bucket = u.netloc
    prefix = u.path.lstrip("/")

    client = storage.Client()
    blobs = list(client.list_blobs(bucket, prefix=prefix))
    xlsx = [b for b in blobs if b.name.lower().endswith(".xlsx")]
    if not xlsx:
        raise RuntimeError(f"No .xlsx files under {gcs_prefix}")

    newest = max(xlsx, key=lambda b: b.updated)
    return f"gs://{bucket}/{newest.name}"

def _read_excel_from_gcs(gcs_uri:str) -> dict:
    """
    Read Excel bytes to pandas; return dict of sheet_name -> DataFrame.
    """
    u = urlparse(gcs_uri)
    bucket = u.netloc
    blob_path = u.path.lstrip("/")

    storage_client = storage.Client()
    blob = storage_client.bucket(bucket).blob(blob_path)
    data = blob.download_as_bytes()

    xl = pd.ExcelFile(io.BytesIO(data))
    sheets = {s: xl.parse(s, header=None) for s in xl.sheet_names}
    return sheets

def _infer_week(window_str:str):
    """
    From header like '11.02-11.08.25' extract week_start/week_end as DATEs.
    """
    # supports 'MM.DD-MM.DD.YY' or 'M.DD-M.D.YY'
    m = re.search(r'(\d{1,2})\.(\d{1,2})-(\d{1,2})\.(\d{1,2})\.(\d{2})', window_str)
    if not m:
        return None, None
    m1, d1, m2, d2, yy = map(int, m.groups())
    year = 2000 + yy
    start = datetime(year, m1, d1).date()
    end   = datetime(year, m2, d2).date()
    return start, end

def _extract_summary_from_sheet(df: pd.DataFrame, sheet_name:str) -> pd.DataFrame:
    """
    Locate the small 2-column summary block (Broker, Count) on the right side.
    Heuristic: first column with all strings & the adjacent numeric column.
    """
    # try to find a column that contains 'Broker' header in any cell
    broker_col_idx, count_col_idx = None, None
    for c in range(df.shape[1]-1):
        col_vals = df[c].dropna().astype(str).str.strip().tolist()
        if not col_vals:
            continue
        if any(v.lower().startswith("broker") for v in col_vals[:5]):
            broker_col_idx = c
            count_col_idx  = c + 1
            break

    if broker_col_idx is None:
        # fallback: find the first column where the next one is numeric-ish
        for c in range(df.shape[1]-1):
            left  = df[c].dropna().astype(str)
            right = pd.to_numeric(df[c+1], errors="coerce")
            if len(left) > 0 and right.notna().sum() >= 1:
                broker_col_idx, count_col_idx = c, c+1
                break

    if broker_col_idx is None:
        return pd.DataFrame(columns=["broker","count"])

    block = df[[broker_col_idx, count_col_idx]].copy()
    block.columns = ["broker","count"]
    block["broker"] = block["broker"].astype(str).str.strip()

    # drop headers/total rows and obviously bad rows
    block = block[block["broker"].str.len() > 0]
    block = block[~block["broker"].str.lower().str.startswith("total")]
    block["count"] = pd.to_numeric(block["count"], errors="coerce")
    block = block[block["count"].notna()].reset_index(drop=True)
    block["brand"] = sheet_name.strip()
    return block[["brand","broker","count"]]

def main():
    gcs_uri  = os.getenv("FILE_URI") or _list_latest_xlsx(RAW_PREFIX)
    sheets   = _read_excel_from_gcs(gcs_uri)

    # try to find the header cell that carries the week window (e.g., 11.02-11.08.25)
    week_header = None
    for df in sheets.values():
        flat = df.astype(str).values.ravel().tolist()
        for cell in flat:
            if re.search(r'\d{1,2}\.\d{1,2}-\d{1,2}\.\d{1,2}\.\d{2}', cell):
                week_header = cell
                break
        if week_header:
            break

    week_start, week_end = (None, None)
    if week_header:
        week_start, week_end = _infer_week(week_header)

    parts = []
    for name, df in sheets.items():
        # ignore "Audit" or "Unmapped" sheets
        if name.lower() in ("audit", "unmapped", "unmap", "unmapped "):
            continue
        summary = _extract_summary_from_sheet(df, name)
        if not summary.empty:
            parts.append(summary)

    if not parts:
        print("No summary blocks found; nothing to load.")
        return

    out = pd.concat(parts, ignore_index=True)
    out["week_start"] = week_start
    out["week_end"]   = week_end
    out["file_gcs"]   = gcs_uri

    bq = bigquery.Client(project=PROJECT)
    table_id = f"{PROJECT}.{DATASET}.{TABLE}"

    job = bq.load_table_from_dataframe(
        out,
        table_id,
        job_config=bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND
        ),
    )
    job.result()
    print(f"Loaded {len(out)} rows to {table_id} from {gcs_uri}")

if __name__ == "__main__":
    main()
