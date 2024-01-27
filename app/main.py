"""Main API module."""

import duckdb
from fastapi import FastAPI

app = FastAPI()


@app.get("/")
def hello_world():
    """Root route in Brazilian Weather Data API."""
    message = duckdb.sql("SELECT 'Hello, World!'").fetchone()[0]
    return {"message": message}
