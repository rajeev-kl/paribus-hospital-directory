# Paribus Hospital Bulk Processor

Bulk CSV ingestion service built with **FastAPI**. Upload a hospital roster, stream each row into the Hospital Directory API, and activate the remote batch once every hospital is created successfully.

## Features

- `POST /hospitals/bulk` accepts multipart CSV uploads (`name,address,phone`).
- Validates headers, required fields, and enforces a configurable 20-row limit.
- Calls the upstream Hospital Directory API for each row, tagging every hospital with a shared batch ID.
- Activates the batch when all rows are created; partial failures are reported row-by-row.
- Returns a structured JSON summary: totals, timing, activation status, and per-row diagnostics.

## Requirements

- Python 3.12+
- Poetry (dependency management)

## Installation

```bash
cd paribus-hospital-directory
poetry install --no-root
```

## Configuration

All settings are environment variables (optional, with defaults):

| Variable | Default | Description |
| --- | --- | --- |
| `HOSPITAL_DIRECTORY_API_BASE_URL` | `https://hospital-directory.onrender.com` | Upstream Hospital Directory API base URL |
| `BATCH_SIZE_LIMIT` | `20` | Maximum number of hospitals per CSV |
| `OUTBOUND_TIMEOUT_SECONDS` | `10` | Timeout (seconds) for outbound HTTP calls |

Create a `.env` file if you prefer to store overrides locally.

## Running the Server

### Local development

```bash
poetry run uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

The interactive docs are available at `http://localhost:8000/docs`.

### Production examples

```bash
# Uvicorn (single process)
poetry run uvicorn app.main:app --host 0.0.0.0 --port 8000

# Gunicorn + Uvicorn workers
poetry run gunicorn app.main:app -k uvicorn.workers.UvicornWorker --bind 0.0.0.0:8000
```

## API Usage

`POST /hospitals/bulk`

- Content-Type: `multipart/form-data`
- Form field: `file` → CSV payload with headers `name,address,phone`

Example request (using `httpie`):

```bash
http --form POST :8000/hospitals/bulk \
  file@./sample.csv
```

Typical response:

```json
{
  "batch_id": "550e8400-e29b-41d4-a716-446655440000",
  "total_hospitals": 2,
  "processed_hospitals": 2,
  "failed_hospitals": 0,
  "processing_time_seconds": 1.234,
  "batch_activated": true,
  "hospitals": [
    {
      "row": 1,
      "hospital_id": 101,
      "name": "General Hospital",
      "status": "created_and_activated"
    }
  ]
}
```

If activation fails, `batch_activated` becomes `false` and `activation_error` describes the upstream issue while row-level results are preserved.

## Testing

```bash
poetry run pytest
```

The suite covers the service layer and the HTTP endpoint using `httpx.MockTransport` and ASGI integration tests.

## Deployment Notes

- Ship the app with Uvicorn or Gunicorn+Uvicorn workers; Render and similar platforms can use the production commands above.
- Ensure the `HOSPITAL_DIRECTORY_API_BASE_URL` environment variable matches the deployed Hospital Directory API.
- For observability, hook into the FastAPI log output (standard logging) or extend with your platform’s preferred logger.
- A sample `systemd` unit lives at `systemd/paribus-bulk.service`. Copy it to `/etc/systemd/system/paribus-bulk.service`, adjust `WorkingDirectory`, `User`, and `Group` for your host, then run:

  ```bash
  sudo systemctl daemon-reload
  sudo systemctl enable --now paribus-bulk
  ```

  The unit starts Uvicorn with three worker processes (`--workers 3`) by default; increase this count if the host has additional CPU capacity.