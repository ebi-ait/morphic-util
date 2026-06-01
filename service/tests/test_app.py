"""Minimal pytest coverage for the /validate HTTP service.

These tests intentionally avoid depending on a real morphic spreadsheet
fixture. They verify:

  * non-.xlsx uploads are rejected with 400
  * the health endpoint responds
  * an .xlsx that's missing the required morphic sheets returns a
    structured 400 (validation error), not a 500 (server error)

For deeper coverage, add fixtures with real spreadsheet content and test
the happy path against SpreadsheetValidator directly.
"""

import io

import pytest
from fastapi.testclient import TestClient
from openpyxl import Workbook

from service.app import app


@pytest.fixture
def client():
    return TestClient(app)


@pytest.fixture
def empty_xlsx_bytes():
    """An .xlsx with one empty 'Random' sheet — no morphic sheets present."""
    wb = Workbook()
    ws = wb.active
    ws.title = "Random"
    ws["A1"] = "placeholder"

    buf = io.BytesIO()
    wb.save(buf)
    buf.seek(0)
    return buf.read()


def test_health(client):
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_rejects_non_xlsx(client):
    response = client.post(
        "/validate",
        files={"file": ("notes.txt", b"hello world", "text/plain")},
    )
    assert response.status_code == 400
    body = response.json()
    assert "xlsx" in body["detail"].lower()


def test_missing_required_sheets_returns_400_with_errors(client, empty_xlsx_bytes):
    response = client.post(
        "/validate",
        files={
            "file": (
                "empty.xlsx",
                empty_xlsx_bytes,
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
        },
    )
    # Must be a structured validation failure, never a 500.
    assert response.status_code == 400, response.text
    body = response.json()
    assert body["valid"] is False
    assert body["file_name"] == "empty.xlsx"
    assert isinstance(body["errors"], list)
    assert body["errors"], "expected at least one validation error for an empty spreadsheet"
