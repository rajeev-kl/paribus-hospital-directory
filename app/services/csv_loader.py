from __future__ import annotations

import csv
from dataclasses import dataclass
import io

from app.exceptions import CSVFormatError, CSVRowError, CSVTooLargeError

EXPECTED_HEADERS = {"name", "address", "phone"}
REQUIRED_HEADERS = {"name", "address"}


@dataclass(slots=True)
class HospitalCSVRow:
    row_number: int
    name: str
    address: str
    phone: str | None


def parse_hospital_csv(raw_bytes: bytes, *, limit: int) -> list[HospitalCSVRow]:
    try:
        text = raw_bytes.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise CSVFormatError([CSVRowError(0, "Unable to decode CSV as UTF-8")]) from exc

    reader = csv.DictReader(io.StringIO(text))

    if reader.fieldnames is None:
        raise CSVFormatError([CSVRowError(0, "Missing header row")])

    normalized_headers = {header.strip().lower() for header in reader.fieldnames if header}
    missing = REQUIRED_HEADERS - normalized_headers
    if missing:
        raise CSVFormatError([
            CSVRowError(0, f"Missing required column(s): {', '.join(sorted(missing))}"),
        ])

    unknown = normalized_headers - EXPECTED_HEADERS
    if unknown:
        raise CSVFormatError([
            CSVRowError(0, f"Unexpected column(s): {', '.join(sorted(unknown))}"),
        ])

    rows: list[HospitalCSVRow] = []
    errors: list[CSVRowError] = []

    for index, raw_row in enumerate(reader, start=1):
        name = (raw_row.get("name") or "").strip()
        address = (raw_row.get("address") or "").strip()
        phone_raw = raw_row.get("phone")
        phone = phone_raw.strip() if isinstance(phone_raw, str) and phone_raw.strip() else None

        if not name:
            errors.append(CSVRowError(index, "Name is required"))
        if not address:
            errors.append(CSVRowError(index, "Address is required"))

        rows.append(HospitalCSVRow(row_number=index, name=name, address=address, phone=phone))

        if len(rows) > limit:
            raise CSVTooLargeError(limit=limit, actual=len(rows))

    if errors:
        raise CSVFormatError(errors)

    if not rows:
        raise CSVFormatError([CSVRowError(0, "CSV contains no hospital rows")])

    return rows
