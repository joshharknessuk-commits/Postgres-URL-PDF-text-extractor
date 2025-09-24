"""
stats.py
--------
Quick progress dashboard for dev.documents.

Reads:
  id, processed, process_attempts, last_error, processed_at, mime, bytes

Env:
  DOCS_TABLE=dev.documents
"""

from __future__ import annotations
import os
from datetime import timedelta
from sqlalchemy import text
import load_env  # noqa: F401
from config import engine

TABLE = os.getenv("DOCS_TABLE", "dev.documents")

ATTEMPT_BUCKETS = [
    (0, 0),
    (1, 1),
    (2, 2),
    (3, 3),
    (4, 4),
    (5, 5),
    (6, 10),   # 6–10
    (11, 20),  # 11–20
    (21, 9999) # 21+
]


def q(conn, sql, **params):
    return conn.execute(text(sql), params)


def print_header(title: str):
    print("\n" + title)
    print("-" * len(title))


def main():
    with engine.begin() as conn:
        # Totals
        total = q(conn, f"SELECT count(*) FROM {TABLE}").scalar_one()
        processed = q(conn, f"SELECT count(*) FROM {TABLE} WHERE processed = TRUE").scalar_one()
        unprocessed = total - processed
        errors = q(conn, f"SELECT count(*) FROM {TABLE} WHERE last_error IS NOT NULL").scalar_one()

        print_header("Overview")
        print(f"Total documents       : {total:,}")
        print(f"Processed             : {processed:,}")
        print(f"Unprocessed           : {unprocessed:,}")
        print(f"With last_error       : {errors:,}")

        # Fresh vs stale queue (unprocessed only)
        fresh_24h = q(conn, f"""
            SELECT count(*) FROM {TABLE}
            WHERE processed = FALSE AND now() - COALESCE(processed_at, now() - interval '100 years') <= interval '24 hours'
        """).scalar_one()
        print(f"Recently processed (≤24h): {fresh_24h:,}")

        # Attempt buckets
        print_header("Process attempts (all rows)")
        for lo, hi in ATTEMPT_BUCKETS:
            if lo == hi:
                c = q(conn, f"SELECT count(*) FROM {TABLE} WHERE process_attempts = :n", n=lo).scalar_one()
                print(f"Attempts == {lo:>2}: {c:,}")
            else:
                c = q(conn, f"SELECT count(*) FROM {TABLE} WHERE process_attempts BETWEEN :lo AND :hi", lo=lo, hi=hi).scalar_one()
                label = f"{lo}–{hi if hi < 9999 else '∞'}"
                print(f"Attempts {label:>5}: {c:,}")

        # Error summaries (top 10 most recent)
        print_header("Recent errors (top 10)")
        rows = q(conn, f"""
            SELECT id, process_attempts, LEFT(last_error, 140) AS err, processed_at
            FROM {TABLE}
            WHERE last_error IS NOT NULL
            ORDER BY COALESCE(processed_at, '-infinity'::timestamptz) DESC
            LIMIT 10
        """).fetchall()
        if not rows:
            print("None 🎉")
        else:
            for r in rows:
                print(f"- {r.id}  attempts={r.process_attempts}  at={r.processed_at}  err={r.err}")

        # MIME distribution (top 10)
        print_header("MIME types (top 10 by count)")
        mime_rows = q(conn, f"""
            SELECT COALESCE(mime, 'unknown') AS mime, count(*) AS n
            FROM {TABLE}
            GROUP BY 1
            ORDER BY n DESC
            LIMIT 10
        """).fetchall()
        for m in mime_rows:
            print(f"{m.mime:30} {m.n:,}")

        # Size stats for processed with bytes
        print_header("Size (bytes) stats for processed rows with bytes IS NOT NULL")
        size_stats = q(conn, f"""
            SELECT
              count(*)                       AS n,
              MIN(bytes)                     AS min_b,
              PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY bytes) AS p25,
              PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY bytes) AS median,
              PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY bytes) AS p75,
              MAX(bytes)                     AS max_b,
              ROUND(AVG(bytes))              AS mean_b
            FROM {TABLE}
            WHERE processed = TRUE AND bytes IS NOT NULL
        """).mappings().one()
        if size_stats["n"] == 0:
            print("No size data yet.")
        else:
            print(f"N         : {size_stats['n']:,}")
            print(f"Min / Max : {size_stats['min_b']:,} / {size_stats['max_b']:,}")
            print(f"P25/50/75 : {int(size_stats['p25']):,} / {int(size_stats['median']):,} / {int(size_stats['p75']):,}")
            print(f"Mean      : {int(size_stats['mean_b']):,}")

        # Throughput last 24h
        print_header("Throughput (processed in last 24h)")
        thru_24 = q(conn, f"""
            SELECT count(*) FROM {TABLE}
            WHERE processed = TRUE AND processed_at >= now() - interval '24 hours'
        """).scalar_one()
        print(f"Processed last 24h: {thru_24:,}")

        # Oldest unprocessed (to spot stuck items)
        print_header("Oldest unprocessed (top 10 by id)")
        oldest = q(conn, f"""
            SELECT id, pdf_url, process_attempts, last_error
            FROM {TABLE}
            WHERE processed = FALSE
            ORDER BY id
            LIMIT 10
        """).fetchall()
        if not oldest:
            print("Queue empty ✅")
        else:
            for r in oldest:
                le = (r.last_error[:100] + "…") if r.last_error and len(r.last_error) > 100 else (r.last_error or "")
                print(f"- {r.id} attempts={r.process_attempts}  err={le}")

if __name__ == "__main__":
    main()