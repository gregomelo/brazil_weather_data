import pandas as pd
import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.tools.pipeline import STATION_COLUMN_NAMES


@pytest.fixture
def test_client():
    """Create a test client for the FastAPI application.

    Yields
    -------
    TestClient
        An instance of TestClient wrapped around the FastAPI app.
    """
    with TestClient(app) as client:
        yield client


def test_main_route(test_client):
    """Run tests on root route.


    Parameters
    ----------
    test_client : TestClient
        An instance of TestClient for making requests to the FastAPI app.


    """
    response = test_client.get("/")
    assert response.status_code == 200
    assert response.url == "http://testserver/docs"


class TestStationsRoute:
    """Run tests on stations routes.

    Notes
    -------
    These tests could be done using test parameters to avoid repetition.

    Asserts
    -------
    The '/' route should return a status code of 200, and redirect user to
    the API documentation.
    """

    def test_stations_route(
        self,
        test_client,
    ):
        """Test the '/stations' route for correct response structure and data.

        Parameters
        ----------
        test_client : TestClient
            An instance of TestClient for making requests to the FastAPI app.

        Asserts
        -------
        The '/stations' route should return a status code of 200, the response
        should be a list, and the data should match the expected column names
        and have more than one row.
        """
        response = test_client.get("/stations")

        response_json = response.json()

        response_df = pd.DataFrame(response_json)

        response_number_rows, _ = response_df.shape

        assert response.status_code == 200
        assert isinstance(response_json, list)
        assert response_df.columns.to_list() == [
            column for column in STATION_COLUMN_NAMES.values()
        ]
        assert response_number_rows > 1

    def test_station_route(
        self,
        test_client,
    ):
        """Test the '/stations/{IdStationWho}' route for correct response
        structure and data.

        Parameters
        ----------
        test_client : TestClient
            An instance of TestClient for making requests to the FastAPI app.

        Asserts
        -------
        The '/stations/{IdStationWho}' route should return a status code of
        200, the response should be a list, and the data should match the
        expected column names, contain exactly one row, and the 'IdStationWho'
        column should match the requested station ID.
        """
        response = test_client.get("/stations/A544")

        response_json = response.json()

        response_df = pd.DataFrame(response_json)

        response_number_rows, _ = response_df.shape

        assert response.status_code == 200
        assert isinstance(response_json, list)
        assert response_df.columns.to_list() == [
            column for column in STATION_COLUMN_NAMES.values()
        ]
        assert response_df["IdStationWho"][0] == "A544"
        assert response_number_rows == 1

    def test_station_invalid(
        self,
        test_client,
    ):
        """Test the '/stations/{IdStationWho}' route for an incorrect
        response.

        Parameters
        ----------
        test_client : TestClient
            An instance of TestClient for making requests to the FastAPI app.

        Asserts
        -------
        The '/stations/{IdStationWho}' route should return a status code of
        404 and deliver a message 'Station do not exist.' .
        """
        response = test_client.get("/stations/A000")

        response_json = response.json()

        assert response.status_code == 404
        assert response_json == {"detail": "Station do not exist."}
