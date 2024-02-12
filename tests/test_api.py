import pandas as pd
import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.tools.pipeline import STATION_COLUMN_NAMES, WEATHER_COLUMN_NAMES


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


WEATHER_DF_COLUMN_NAMES = [column for column in WEATHER_COLUMN_NAMES.values()]


class TestWeatherRoute:
    """Run tests on weather routes.

    Notes
    -------
    These tests could be done using test parameters to avoid repetition.
    """

    def test_get_correct_data(
        self,
        test_client,
    ):
        response = test_client.get("/weather/A544/2023-01-01/2023-01-02/")

        response_json = response.json()

        response_df = pd.DataFrame(response_json)

        response_df_columns = response_df.columns.to_list()

        assert response.status_code == 200
        assert isinstance(response_json, list)
        assert response_df_columns.sort() == WEATHER_DF_COLUMN_NAMES.sort()

    def test_get_more_than_5_weeks(
        self,
        test_client,
    ):
        response = test_client.get("/weather/A544/2023-01-01/2023-05-02/")
        response_json = response.json()

        assert response.status_code == 422
        assert response_json == {
            "detail": "Maximum period between start_date and end_date should be 5 weeks.",  # noqa
        }  # noqa

    def test_get_end_before_start(
        self,
        test_client,
    ):
        response = test_client.get("/weather/A544/2023-02-01/2023-01-02/")
        response_json = response.json()

        assert response.status_code == 422
        assert response_json == {
            "detail": "end_date should be after start_date.",
        }  # noqa

    def test_get_future_data(
        self,
        test_client,
    ):
        response = test_client.get("/weather/A544/2099-01-01/2099-01-02/")

        assert response.status_code == 422

    def test_empty_query(
        self,
        test_client,
    ):
        response = test_client.get("/weather/A000/1999-01-01/1999-01-02/")

        response_json = response.json()

        assert response.status_code == 422
        assert response_json == {
            "detail": "There is no data to show. Rewrite your query.",
        }
