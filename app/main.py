"""Main API module."""

import json
import os

import duckdb
from fastapi import FastAPI, HTTPException
from fastapi.responses import RedirectResponse

from app.tools.pipeline import OUTPUT_PATH, STATIONS_FILE
from app.tools.validators import IdStationWhoType

API_DESCRIPTION = """
This API provides access to data from Brazil's automated weather stations,
sourced from [National Institute of Meteorology](https://portal.inmet.gov.br/).

For more information, usage examples, and collaboration, visit the project's
GitHub repository:
[Brazil Weather Data](https://github.com/gregomelo/brazil_weather_data).

**Note**: This project is for educational purposes only.
"""

tags_metadata = [
    {
        "name": "stations",
        "description": "Geographical data and deployment dates of weather stations.",  # noqa
    },
    {
        "name": "weather",
        "description": "*Future.* Detailed meteorological data.",
    },
    {
        "name": "query",
        "description": "*Future.* Simulate SQL queries for customized data"
        "analysis. This queries will be run with DuckDB.",
        "externalDocs": {
            "description": "DuckDB. SQL Introduction",
            "url": "https://duckdb.org/docs/archive/0.9.2/sql/introduction",
        },
    },
    {
        "name": "update",
        "description": "This endpoint will not be implemented due to "
        "resource restrictions in the Render free tier. However, this could "
        "use the pipeline module to update the weather database.",
    },
]


app = FastAPI(
    title="Brazil Weather Data API",
    description=API_DESCRIPTION,
    openapi_tags=tags_metadata,
)


@app.get("/")
def root() -> None:
    """Redirect user to API documentation."""
    return RedirectResponse(url="/docs")


def query_db(sql_query: str) -> list:
    """
    Execute a SQL query and return the results in JSON format.

    This function takes a SQL query string, executes it using DuckDB, and
    returns the results as a JSON array. It's designed to provide an easy
    interface for querying a database and getting the results in a
    web-friendly format. The response is formatted with 'records' orientation
    and ISO date format.

    Parameters
    ----------
    sql_query : str
        A SQL query string to be executed in the DuckDB database.

    Returns
    -------
    list
        A list of dictionaries representing the rows of the query result.
        Each dictionary corresponds to a row, with column names as keys.

    Raises
    ------
    Exception
        Raises an exception if the SQL query execution or JSON conversion
        fails.
    """
    try:
        response = (
            duckdb.sql(sql_query)
            .df()
            .to_json(
                orient="records",
                date_format="iso",
            )
        )
        response_json = json.loads(response)
        return response_json
    except Exception as e:
        raise Exception(f"Error in query_db function: {e}")


stations_parquet = f"{STATIONS_FILE}.parquet"
stations_db = os.path.join(OUTPUT_PATH, stations_parquet)


@app.get("/stations/", tags=["stations"])
def list_stations():
    """
    Return the following information about all the stations.

    - IdStationWho
    - Region
    - State
    - StationName
    - Latitude
    - Longitude
    - Altitude
    - FoundingDate

    *Future.* This information can be used in the query route to build the SQL
    statements.
    """
    query_sql = f"SELECT * FROM '{stations_db}'"  # nosec B608
    return query_db(query_sql)


@app.get("/stations/{IdStationWho}/", tags=["stations"])
def list_station(
    IdStationWho: IdStationWhoType,
):
    """Return information about a selected station using IdStationWho."""
    query_sql = f"""SELECT *
                    FROM '{stations_db}'
                    WHERE IdStationWho = '{IdStationWho}'"""  # nosec B608
    result = query_db(query_sql)
    if result:
        return result
    else:
        raise HTTPException(status_code=404, detail="Station do not exist.")
