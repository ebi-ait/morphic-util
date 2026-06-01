"""HTTP /validate service for morphic-util spreadsheets.

Shares the parsing/validation logic with the CLI via
``ait.commons.util.spreadsheet_validate.SpreadsheetValidator``.

Run locally:
    uvicorn service.app:app --host 0.0.0.0 --port 8000

Or via Docker (see service/Dockerfile)."""

from tempfile import NamedTemporaryFile
from typing import Any

import numpy as np
import pandas as pd
import uvicorn
from fastapi import FastAPI, File, HTTPException, UploadFile
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse

from ait.commons.util.spreadsheet_util import (
    SpreadsheetSubmitter,
    ValidationError,
    SubmissionError,  # noqa: F401  (re-exported for parity with the CLI surface)
)
from ait.commons.util.spreadsheet_validate import SpreadsheetValidator


def convert_to_json_serializable(obj: Any) -> Any:
    """Recursively convert pandas/numpy objects to JSON-serializable forms."""
    if isinstance(obj, dict):
        return {k: convert_to_json_serializable(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [convert_to_json_serializable(item) for item in obj]
    if isinstance(obj, pd.DataFrame):
        return obj.replace({np.nan: None, np.inf: None, -np.inf: None}).to_dict('records')
    if isinstance(obj, pd.Series):
        return obj.replace({np.nan: None, np.inf: None, -np.inf: None}).to_dict()
    if isinstance(obj, np.ndarray):
        return convert_to_json_serializable(obj.tolist())
    if isinstance(obj, (np.integer, np.int64)):
        return int(obj)
    if isinstance(obj, (np.floating, np.float64)):
        if np.isnan(obj) or np.isinf(obj):
            return None
        return float(obj)
    try:
        if pd.isna(obj):
            return None
    except (TypeError, ValueError):
        pass
    return obj


app = FastAPI(title="morphic-util validator", version="1.0")


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/validate")
async def validate_xlsx(file: UploadFile = File(...)):
    if not file.filename or not file.filename.endswith(".xlsx"):
        raise HTTPException(status_code=400, detail="Only .xlsx files are supported.")

    contents = await file.read()
    serialized_data: dict = {"file_name": file.filename}

    try:
        with NamedTemporaryFile(delete=True, suffix=".xlsx") as tmp:
            tmp.write(contents)
            tmp.flush()

            try:
                submitter = SpreadsheetSubmitter(tmp.name)
                parsed_data = SpreadsheetValidator._parse_spreadsheet(submitter)

                is_valid = not parsed_data.get("errors")

                serialized_data = convert_to_json_serializable(parsed_data)
                serialized_data["file_name"] = file.filename

                # Strip metadata-only keys from the data payload
                parsed_data_no_info = {
                    k: v for k, v in parsed_data.items() if k not in ("errors", "sheets")
                }
                serialized_data_no_info = convert_to_json_serializable(parsed_data_no_info)

                if not is_valid:
                    return JSONResponse(status_code=400, content=jsonable_encoder({
                        "valid": False,
                        "file_name": serialized_data.get("file_name"),
                        "errors": serialized_data.get("errors", []),
                        "message": "Spreadsheet validation failed",
                        "sheets_found": serialized_data.get("sheets", []),
                    }))

                return JSONResponse(content=jsonable_encoder({
                    "valid": True,
                    "file_name": serialized_data.get("file_name"),
                    "sheets_found": serialized_data.get("sheets", []),
                    "errors": serialized_data.get("errors", []),
                    "data": serialized_data_no_info,
                }))

            except ValidationError as ve:
                return JSONResponse(status_code=400, content={
                    "valid": False,
                    "file_name": serialized_data.get("file_name"),
                    "errors": getattr(ve, "errors", [str(ve)]),
                    "message": "Validation failed",
                })
            except AttributeError as ae:
                return JSONResponse(status_code=400, content={
                    "valid": False,
                    "file_name": serialized_data.get("file_name"),
                    "errors": [str(ae)],
                    "message": "Spreadsheet is missing required sheets or fields",
                })
            except Exception as e:
                return JSONResponse(status_code=400, content={
                    "valid": False,
                    "file_name": serialized_data.get("file_name"),
                    "errors": [str(e)],
                    "message": "Validation failed with an unexpected error",
                })
    except Exception as e:
        return JSONResponse(status_code=500, content={
            "valid": False,
            "file_name": serialized_data.get("file_name"),
            "errors": [str(e)],
            "message": "Server error",
        })


# For local dev: `python -m service.app`
if __name__ == "__main__":
    uvicorn.run("service.app:app", host="0.0.0.0", port=8000, reload=True)
