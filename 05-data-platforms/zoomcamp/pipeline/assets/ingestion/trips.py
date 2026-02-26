"""@bruin
name: ingestion.trips

type: python
image: python:3.12.10

# TODO: Set the connection.
connection: duckdb-default

materialization:
  type: table
  strategy: append
  
@bruin"""

# TODO: Add imports needed for your ingestion (e.g., pandas, requests).
# - Put dependencies in the nearest `requirements.txt` (this template has one at the pipeline root).
# Docs: https://getbruin.com/docs/bruin/assets/python



# TODO: Only implement `materialize()` if you are using Bruin Python materialization.
# If you choose the manual-write approach (no `materialization:` block), remove this function and implement ingestion
# as a standard Python script instead.
def materialize():
    """
    TODO: Implement ingestion using Bruin runtime context.

    Required Bruin concepts to use here:
    - Built-in date window variables:
      - BRUIN_START_DATE / BRUIN_END_DATE (YYYY-MM-DD)
      - BRUIN_START_DATETIME / BRUIN_END_DATETIME (ISO datetime)
      Docs: https://getbruin.com/docs/bruin/assets/python#environment-variables
    - Pipeline variables:
      - Read JSON from BRUIN_VARS, e.g. `taxi_types`
      Docs: https://getbruin.com/docs/bruin/getting-started/pipeline-variables

    Design TODOs (keep logic minimal, focus on architecture):
    - Use start/end dates + `taxi_types` to generate a list of source endpoints for the run window.
    - Fetch data for each endpoint, parse into DataFrames, and concatenate.
    - Add a column like `extracted_at` for lineage/debugging (timestamp of extraction).
    - Prefer append-only in ingestion; handle duplicates in staging.
    """
    import os
    import json
    import io
    from datetime import datetime, date
    import time
    from dateutil.relativedelta import relativedelta
    import pandas as pd
    import requests

    # Helpers
    def parse_date(s: str) -> date:
      return datetime.strptime(s, "%Y-%m-%d").date()

    def months_in_range(start: date, end: date):
      cur = date(start.year, start.month, 1)
      last = date(end.year, end.month, 1)
      while cur <= last:
        yield cur.year, cur.month
        cur += relativedelta(months=1)

    # Read run window from Bruin environment variables
    start_s = os.environ.get("BRUIN_START_DATE")
    end_s = os.environ.get("BRUIN_END_DATE")
    if not start_s or not end_s:
      raise RuntimeError("BRUIN_START_DATE and BRUIN_END_DATE must be set in the runtime environment")

    start_date = parse_date(start_s)
    end_date = parse_date(end_s)

    # Read pipeline variables (optional)
    vars_json = os.environ.get("BRUIN_VARS")
    taxi_types = ["yellow"]
    if vars_json:
      try:
        vars_obj = json.loads(vars_json)
        taxi_types = vars_obj.get("taxi_types", taxi_types)
      except Exception:
        print("Warning: failed to parse BRUIN_VARS; using defaults")

    # NYC TLC source mirrors and formats (prefer CSV variants to avoid parquet timezone issues)
    def build_sources(taxi: str, year: int, month: int):
      return [
        {
          "url": f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{taxi}/{taxi}_tripdata_{year}-{month:02d}.csv.gz",
          "format": "csv_gz",
        },
        {
          "url": f"https://s3.amazonaws.com/nyc-tlc/trip+data/{taxi}_tripdata_{year}-{month:02d}.csv",
          "format": "csv",
        },
        {
          "url": f"https://d37ci6vzurychx.cloudfront.net/trip-data/{taxi}_tripdata_{year}-{month:02d}.parquet",
          "format": "parquet",
        },
      ]

    dataframes = []
    extracted_at = datetime.utcnow().isoformat()

    for taxi in taxi_types:
      for year, month in months_in_range(start_date, end_date):
        month_label = f"{taxi}_tripdata_{year}-{month:02d}"
        fetched = False
        sources = build_sources(taxi, year, month)
        for source in sources:
          url = source["url"]
          source_format = source["format"]
          resp = None
          for attempt in range(1, 4):
            try:
              resp = requests.get(url, timeout=90)
              break
            except Exception as e:
              print(f"Request error for {url} (attempt {attempt}/3): {e}")
              if attempt < 3:
                time.sleep(2 * attempt)
          if resp is None:
            continue

          if resp.status_code == 200:
            try:
              bio = io.BytesIO(resp.content)
              if source_format == "parquet":
                df = pd.read_parquet(bio)
              elif source_format == "csv_gz":
                df = pd.read_csv(bio, compression="gzip")
              else:
                df = pd.read_csv(bio)
              df["extracted_at"] = extracted_at
              df["_source_url"] = url
              df["taxi_type"] = taxi
              dataframes.append(df)
              fetched = True
              print(f"Fetched {url} rows={len(df)}")
              break
            except Exception as e:
              print(f"Failed parsing payload from {url}: {e}")
              continue
          else:
            print(f"Source not available at {url}: status={resp.status_code}")
            # try next template or skip
            continue

        if not fetched:
          print(f"No data available for {month_label} (checked {len(sources)} sources)")

    if not dataframes:
      raise RuntimeError(
        f"No trip data fetched for interval {start_date} to {end_date}. "
        "Check network access and source URL availability."
      )

    final_df = pd.concat(dataframes, ignore_index=True)

    # Normalize timestamp-like columns to plain strings to avoid runtime tzdb issues
    # in pyarrow/dlt on Windows environments with missing TZDIR data.
    for col in final_df.columns:
      dtype_str = str(final_df[col].dtype).lower()
      col_name = col.lower()
      if (
        "tz=" in dtype_str
        or "timestamp[" in dtype_str
        or "datetime" in col_name
        or col_name.endswith("_at")
      ):
        final_df[col] = final_df[col].astype("string")

    return final_df


