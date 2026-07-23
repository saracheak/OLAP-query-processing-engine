"""
Minimal FastAPI wrapper around the OLAP MF query compiler.

Proves the existing engine can run as a web service:
  POST /execute  →  generate(...)  →  JSON response
"""

import os

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from engine import generate

app = FastAPI(title="OLAP Query Processing Engine")


class ExecuteRequest(BaseModel):
    query: str = Field(
        ...,
        description=(
            "Path to a phi-parameter input file (e.g. example_inputs/mf_1.txt), "
            "or the raw phi-parameter text itself."
        ),
    )


class ExecuteResponse(BaseModel):
    status: str
    generated_code: str


@app.post("/execute", response_model=ExecuteResponse)
def execute(request: ExecuteRequest) -> ExecuteResponse:
    """
    Receive a query, run the existing compiler, return JSON.
    """
    query = request.query.strip()
    if not query:
        raise HTTPException(status_code=400, detail="query must not be empty")

    try:
        if os.path.isfile(query):
            code = generate(query)
        else:
            code = generate(phi_text=query)
        return ExecuteResponse(status="ok", generated_code=code)
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
