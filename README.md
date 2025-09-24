# Postgres-URL-PDF-text-extractor

Batch worker for downloading tribunal decision PDFs, extracting text with `pypdf`,
and persisting results back into a Neon/Postgres table. Includes a stats dashboard
and connection healthcheck helpers.

## Features
- Locks unprocessed rows using `FOR UPDATE SKIP LOCKED` for safe parallel workers.
- Streams PDF downloads with retry/backoff, size caps, and MIME validation.
- Extracts text via `pypdf`, computes SHA-256, and records metadata (bytes, mime, filename).
- Tracks processing attempts with error logging and savepoints for graceful failures.
- `stats.py` dashboard for queue monitoring and throughput insights.

## Quickstart
1. **Create a virtualenv & install dependencies**
   ```bash
   python3 -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt
   ```

2. **Configure environment variables**
   - Copy `.env.example` to `.env` and fill in your Neon connection string (`NEON_URL`).
   - Optional: tune batch size, retry timings, and PDF size cap.

3. **Validate the connection**
   ```bash
   .venv/bin/python db_healthcheck.py
   ```

4. **Seed a test row (optional)**
   Insert a row into `dev.documents_test` with a valid `pdf_url` and dummy `sha256`
   to verify the pipeline end-to-end before running on production data.

5. **Run the worker**
   ```bash
   DOCS_TABLE=dev.documents .venv/bin/python process_pdfs.py
   ```
   - Override `DOCS_TABLE` (e.g., `dev.documents_test`) to target a different table.
   - The worker loops until it drains the queue; successes and failures are logged per batch.

6. **Monitor progress**
   ```bash
   .venv/bin/python stats.py
   ```
   Provides queue totals, attempt buckets, recent errors, MIME distribution, and throughput.

## Table expectations
The worker operates on a table with (at minimum) the following columns:

| column | type | notes |
| --- | --- | --- |
| `id` | `uuid` | primary key |
| `pdf_url` | `text` | download source |
| `raw_text` | `text` | extracted text output |
| `processed` | `boolean` | marks completion |
| `process_attempts` | `int` | incremented for every run |
| `processed_at` | `timestamptz` | timestamp when processed |
| `last_error` | `text` | truncated error message |
| `downloaded_at` | `timestamptz` | first successful download |
| `bytes` | `int` | size of PDF |
| `mime` | `text` | content type |
| `filename` | `text` | best-effort filename |
| `sha256` | `varchar(64)` | unique digest of PDF bytes |

You can extend the table with project-specific metadata such as enums for document type or classification.

## Testing tips
- Start with a small staging table (`dev.documents_test`) containing a single PDF.
- Use Neon branch isolation so you can test without touching production data.
- The worker increments `process_attempts` on both success and failure, so you can track retries.

## Security & hygiene
- `.env` is ignored from version control—keep credentials out of Git history.
- For deployments, export environment variables instead of relying on a file.

## License
MIT (add a different license file if needed).
