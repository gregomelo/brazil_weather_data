"""Main API module."""

from fastapi import FastAPI

app = FastAPI()


@app.get("/")
def hello_world():
    """Root route in Brazilian Weather Data API."""
    return {"Hello": "World"}
