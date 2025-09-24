"""
process_pdfs.py
---------------
Processes a batch of PDF URLs from dev.documents:
  - downloads each PDF
  - extracts text with pypdf
  - writes raw_text and status fields back to Postgres

Table columns used:
  id (uuid) PK
  pdf_url (text) NOT NULL
  raw_text (text) NULL
  processed (bool) DEFAULT false
  process_attempts (int) DEFAULT 0
  processed_at (timestamptz) NULL
  last_error (text) NULL
  -- optional meta we also fill if available:
  downloaded_at (timestamptz) NULL
  bytes (int) NULL
  mime (text) NULL
  filename (text) NULL
  sha256 (varchar(64)) UNIQUE NOT NULL  (we will compute)

Env vars (optional):
  WORKER_BATCH_SIZE=200
  MAX_PDF_MB=30
  REQUEST_TIMEOUT=30
  REQUEST_RETRY_TOTAL=3
  REQUEST_RETRY_BACKOFF=0.5
  HTTP_USER_AGENT=Remedy-PDF-Processor/1.0
  DOCS_TABLE=dev.documents
  MAX_ATTEMPTS=5
"""

from __future__ import annotations

import os
import io
import re
import time
import hashlib
from urllib.parse import urlparse, unquote

import load_env  # noqa: F401
from config import engine
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError, SQLAlchemyError

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from pypdf import PdfReader
from pypdf.errors import PdfReadError


# --------- Settings ---------
BATCH_SIZE = int(os.getenv("WORKER_BATCH_SIZE", "200"))
TABLE = os.getenv("DOCS_TABLE", "dev.documents_test")
MAX_PDF_MB = float(os.getenv("MAX_PDF_MB", "30"))
REQUEST_TIMEOUT = float(os.getenv("REQUEST_TIMEOUT", "30"))
RETRY_TOTAL = int(os.getenv("REQUEST_RETRY_TOTAL", "3"))
RETRY_BACKOFF = float(os.getenv("REQUEST_RETRY_BACKOFF", "0.5"))
USER_AGENT = os.getenv("HTTP_USER_AGENT", "Remedy-PDF-Processor/1.0 (+neon)")
MAX_ATTEMPTS = int(os.getenv("MAX_ATTEMPTS", "5"))


# --------- HTTP client with retries ---------
def make_http_session() -> requests.Session:
    sess = requests.Session()
    retry = Retry(
        total=RETRY_TOTAL,
        connect=RETRY_TOTAL,
        read=RETRY_TOTAL,
        backoff_factor=RETRY_BACKOFF,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET", "HEAD"]),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_maxsize=20)
    sess.mount("http://", adapter)
    sess.mount("https://", adapter)
    sess.headers.update({"User-Agent": USER_AGENT})
    return sess


# --------- Helpers ---------
def fetch_batch(conn):
    """
    Lock a batch of rows that still need processing.
    Uses SKIP LOCKED so multiple workers can run safely in parallel.
    """
    sql = f"""
        SELECT id, pdf_url
        FROM {TABLE}
        WHERE processed IS DISTINCT FROM TRUE
          AND pdf_url IS NOT NULL
          AND process_attempts < :max_attempts
        ORDER BY id
        FOR UPDATE SKIP LOCKED
        LIMIT :batch
    """
    return conn.execute(
        text(sql), {"batch": BATCH_SIZE, "max_attempts": MAX_ATTEMPTS}
    ).fetchall()


def guess_filename(url: str, content_disposition: str | None) -> str | None:
    # Try Content-Disposition first
    if content_disposition:
        # e.g., attachment; filename="doc.pdf"
        m = re.search(r'filename\*?=(?:UTF-8\'\')?"?([^\";]+)"?', content_disposition, flags=re.I)
        if m:
            return unquote(m.group(1)).strip()
    # Fallback to URL path
    path = urlparse(url).path
    if path:
        name = os.path.basename(path)
        return unquote(name) if name else None
    return None


def download_pdf(sess: requests.Session, url: str) -> tuple[bytes, dict]:
    """
    Download PDF with size cap and basic metadata return.
    Returns (bytes, meta) where meta includes mime, filename, content_length, headers.
    """
    meta = {"mime": None, "filename": None, "content_length": None, "headers": {}}

    # HEAD (best-effort)
    try:
        h = sess.head(url, timeout=REQUEST_TIMEOUT, allow_redirects=True)
        meta["headers"] = dict(h.headers)
        meta["mime"] = h.headers.get("Content-Type")
        cl = h.headers.get("Content-Length")
        if cl and cl.isdigit():
            meta["content_length"] = int(cl)
            mb = int(cl) / (1024 * 1024)
            if mb > MAX_PDF_MB:
                raise ValueError(f"PDF too large (HEAD): ~{mb:.1f} MB > {MAX_PDF_MB} MB")
        meta["filename"] = guess_filename(url, h.headers.get("Content-Disposition"))
    except Exception:
        # Proceed regardless; some servers don’t support HEAD well
        pass

    with sess.get(url, timeout=REQUEST_TIMEOUT, stream=True) as r:
        r.raise_for_status()

        # If HEAD didn't give filename, try GET headers
        if not meta["filename"]:
            meta["filename"] = guess_filename(url, r.headers.get("Content-Disposition"))
        # Prefer GET content-type
        meta["mime"] = r.headers.get("Content-Type") or meta["mime"]

        if meta["mime"] and "pdf" not in meta["mime"].lower():
            raise ValueError(f"Unexpected content-type: {meta['mime']}")

        cap = int(MAX_PDF_MB * 1024 * 1024)
        buf = io.BytesIO()
        read = 0
        for chunk in r.iter_content(chunk_size=1024 * 64):
            if not chunk:
                continue
            buf.write(chunk)
            read += len(chunk)
            if read > cap:
                raise ValueError(f"PDF too large (GET): exceeded {MAX_PDF_MB} MB cap")

    data = buf.getvalue()
    meta["content_length"] = read if read else len(data) or meta["content_length"]

    return data, meta


def extract_text(pdf_bytes: bytes) -> str:
    """
    Extract text from PDF bytes using pypdf.
    """
    if not pdf_bytes:
        return ""
    out_lines = []
    try:
        reader = PdfReader(io.BytesIO(pdf_bytes))
    except PdfReadError as exc:
        raise ValueError(f"Failed to read PDF: {exc}") from exc

    total_pages = len(reader.pages)
    for i, page in enumerate(reader.pages, start=1):
        try:
            text = page.extract_text() or ""
        except PdfReadError as exc:
            raise ValueError(f"Failed to extract text on page {i}: {exc}") from exc
        out_lines.append(text.rstrip())
        if i < total_pages:
            out_lines.append("\n\n----- PAGE BREAK -----\n\n")
    return "\n".join(out_lines).strip()


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def mark_success(
    conn,
    doc_id,
    *,
    text_out: str,
    meta: dict,
    pdf_bytes: bytes,
    sha_digest: str,
):
    sql = f"""
        UPDATE {TABLE}
        SET
          raw_text = :raw_text,
          processed = TRUE,
          processed_at = now(),
          process_attempts = process_attempts + 1,
          last_error = NULL,
          downloaded_at = COALESCE(downloaded_at, now()),
          bytes = :bytes,
          mime = :mime,
          filename = COALESCE(filename, :filename),
          sha256 = :sha256
        WHERE id = :id
    """
    conn.execute(
        text(sql),
        {
            "id": doc_id,
            "raw_text": text_out,
            "bytes": len(pdf_bytes),
            "mime": meta.get("mime"),
            "filename": meta.get("filename"),
            "sha256": sha_digest,
        },
    )


def mark_failure(conn, doc_id, *, err_msg: str):
    sql = f"""
        UPDATE {TABLE}
        SET
          process_attempts = process_attempts + 1,
          last_error = :err,
          processed = FALSE
        WHERE id = :id
    """
    conn.execute(text(sql), {"id": doc_id, "err": err_msg[:800]})


# --------- Main ---------
def process_once(sess: requests.Session, batch_no: int) -> tuple[int, int, int]:
    with engine.begin() as conn:
        rows = fetch_batch(conn)
        if not rows:
            return 0, 0, 0

        print(f"Processing batch {batch_no} ({len(rows)} document(s))...")

        success = 0
        failures = 0

        for (doc_id, url) in rows:
            t0 = time.time()
            try:
                pdf_bytes, meta = download_pdf(sess, url)
                text_out = extract_text(pdf_bytes)
                sha_digest = sha256_hex(pdf_bytes)
            except Exception as e:
                err = f"{type(e).__name__}: {e}"
                with conn.begin_nested():
                    mark_failure(conn, doc_id, err_msg=err)
                failures += 1
                print(f"✖ {doc_id}  {err}")
                continue

            try:
                with conn.begin_nested():
                    mark_success(
                        conn,
                        doc_id,
                        text_out=text_out,
                        meta=meta,
                        pdf_bytes=pdf_bytes,
                        sha_digest=sha_digest,
                    )
            except IntegrityError as ie:
                err = f"IntegrityError: {ie.orig if hasattr(ie, 'orig') else ie}"
                with conn.begin_nested():
                    mark_failure(conn, doc_id, err_msg=err)
                failures += 1
                print(f"✖ {doc_id}  {err}")
                continue
            except SQLAlchemyError as db_err:
                err = f"SQLAlchemyError: {db_err}"
                with conn.begin_nested():
                    mark_failure(conn, doc_id, err_msg=err)
                failures += 1
                print(f"✖ {doc_id}  {err}")
                continue

            dt = time.time() - t0
            success += 1
            print(f"✔ {doc_id}  {len(text_out)} chars  {dt:.2f}s  ({meta.get('mime')})")

        print(f"Done. ✔={success} ✖={failures}")
        return len(rows), success, failures


def main():
    sess = make_http_session()

    total_rows = 0
    total_success = 0
    total_failures = 0
    batch_no = 0

    while True:
        batch_no += 1
        processed, success, failures = process_once(sess, batch_no)
        if processed == 0:
            if batch_no == 1:
                print("Nothing to process. ✅")
            break

        total_rows += processed
        total_success += success
        total_failures += failures

        if processed < BATCH_SIZE:
            break

    if total_rows:
        print(
            f"All batches done. rows={total_rows} success={total_success} failures={total_failures}"
        )


if __name__ == "__main__":
    main()
