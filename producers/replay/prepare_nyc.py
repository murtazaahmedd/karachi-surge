"""Turn a raw NYC TLC FHVHV month into a sorted, H3-mapped Parquet ready for replay."""
import argparse
from pathlib import Path

import duckdb
import h3

from producers.common.log import get_logger

log = get_logger("prepare_nyc")

H3_RES = 8
RAW = Path("data/raw/nyc")
OUT = Path("data/processed")
ZONES_SHP = f"/vsizip/{RAW}/taxi_zones.zip/taxi_zones/taxi_zones.shp"


def zone_cells(con: duckdb.DuckDBPyConnection) -> list[tuple[int, str]]:
    # Each taxi zone maps to the H3 cell of its centroid (shapefile is in NY State Plane, EPSG:2263).
    rows = con.sql(f"""
        SELECT LocationID, ST_Y(p) AS lat, ST_X(p) AS lon FROM (
            SELECT LocationID, ST_Transform(ST_Centroid(geom), 'EPSG:2263', 'EPSG:4326', always_xy := true) AS p
            FROM ST_Read('{ZONES_SHP}'))
    """).fetchall()
    return [(loc, h3.latlng_to_cell(lat, lon, H3_RES)) for loc, lat, lon in rows]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--month", required=True, help="YYYY-MM, e.g. 2026-09")
    args = ap.parse_args()
    src, dst = RAW / f"fhvhv_tripdata_{args.month}.parquet", OUT / f"nyc_fhvhv_{args.month}.parquet"
    OUT.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect()
    con.sql("INSTALL spatial; LOAD spatial; SET TimeZone = 'UTC'")
    con.execute("CREATE TABLE zones (location_id INTEGER, h3_cell VARCHAR)")
    con.executemany("INSERT INTO zones VALUES (?, ?)", zone_cells(con))
    # Zones 264/265 (unknown / outside NYC) have no geometry, so the inner joins drop them.
    con.sql(f"""
        COPY (
            SELECT
                'fhvhv_{args.month}:' || row_number() OVER (ORDER BY t.request_datetime, t.pickup_datetime) AS trip_key,
                timezone('America/New_York', t.request_datetime) AS event_time,
                pu.h3_cell AS pickup_h3,
                dz.h3_cell AS dropoff_h3,
                round(t.trip_miles * 1.609344, 3) AS est_distance_km
            FROM read_parquet('{src}') t
            JOIN zones pu ON pu.location_id = t.PULocationID
            JOIN zones dz ON dz.location_id = t.DOLocationID
            WHERE strftime(t.request_datetime, '%Y-%m') = '{args.month}'
            ORDER BY event_time
        ) TO '{dst}' (FORMAT parquet)
    """)
    n, lo, hi = con.execute(f"SELECT count(*), min(event_time)::VARCHAR, max(event_time)::VARCHAR FROM '{dst}'").fetchone()
    log.info("wrote %s rows=%d range=%s .. %s", dst, n, lo, hi)


if __name__ == "__main__":
    main()
