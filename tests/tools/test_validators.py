import os
from datetime import date, time

import pandas as pd
import pytest

from app.tools.validators import (
    StationData,
    WeatherData,
    validate_data_quality,
    validate_sublists,
)


@pytest.fixture()
def list_with_equal_elements_equal_order():
    """Provide a list where sublists have identical elements in the same order.

    Returns
    -------
    list
        A list of sublists with equal elements in identical order.
    """
    return [
        ["A", "B"],
        ["A", "B"],
    ]


@pytest.fixture()
def list_with_equal_elements_different_order():
    """Provide a list where sublists have identical elements in different
    orders.

    Returns
    -------
    list
        A list of sublists with equal elements in varying order.
    """
    return [
        ["A", "B"],
        ["B", "A"],
    ]


@pytest.fixture()
def list_with_unequal_elements():
    """Provide a list where sublists have different elements.

    Returns
    -------
    list
        A list of sublists with unequal elements.
    """
    return [
        ["A", "B"],
        ["C", "A"],
    ]


@pytest.fixture()
def list_with_unequal_elements_and_unbalance():
    """Provide a list where sublists have different elements and lengths.

    Returns
    -------
    list
        A list of sublists with unequal elements and varying lengths.
    """
    return [
        ["A", "B", "C"],
        ["C", "A"],
    ]


class TestValidateSublists:
    def test_equal_elements_and_order(
        self,
        list_with_equal_elements_equal_order,
    ):
        """Verify validate_sublists function with lists having identical
        elements in the same order.

        Parameters
        ----------
        list_with_equal_elements_equal_order : list
            Input provided by a fixture.

        Asserts
        -------
        The function returns True for lists with identical elements and order.
        """
        assert validate_sublists(list_with_equal_elements_equal_order)

    def test_equal_elements_and_different_order(
        self,
        list_with_equal_elements_different_order,
    ):
        """Verify validate_sublists function with lists having identical
        elements in different orders.

        Parameters
        ----------
        list_with_equal_elements_different_order : list
            Input provided by a fixture.

        Asserts
        -------
        The function returns True for lists with identical elements
        regardless of their order.
        """
        assert validate_sublists(list_with_equal_elements_different_order)

    def test_unequal_elements(self, list_with_unequal_elements):
        """Verify validate_sublists function with lists having different
        elements.

        Parameters
        ----------
        list_with_unequal_elements : list
            Input provided by a fixture.

        Asserts
        -------
        The function raises a ValueError for lists with differing elements.
        """
        with pytest.raises(ValueError) as excinfo:
            validate_sublists(list_with_unequal_elements)
        assert "Sublists do not have the same elements." in str(excinfo.value)

    def test_unequal_elements_and_unbalance(
        self,
        list_with_unequal_elements_and_unbalance,
    ):
        """Verify validate_sublists function with lists having different
        elements and lengths.

        Parameters
        ----------
        list_with_unequal_elements_and_unbalance : list
            Input provided by a fixture.

        Asserts
        -------
        The function raises a ValueError for lists with differing elements
        and lengths.
        """
        with pytest.raises(ValueError) as excinfo:
            validate_sublists(list_with_unequal_elements_and_unbalance)
        assert "Sublists do not have the same elements." in str(excinfo.value)


dicts_input_test = [
    # Station data cases
    # Case 0. Good data: complete
    {
        "Region": "CO",
        "State": "DF",
        "StationName": "BRASILIA",
        "IdStationWho": "A001",
        "Latitude": "-15,78944444",
        "Longitude": "-47,92583332",
        "Altitude": "1160,96",
        "FoundingDate": "07/05/00",
    },
    # Case 1. Bad data: field required IdStationWho missing
    {
        "Region": "CO",
        "State": "DF",
        "StationName": "BRASILIA",
        "Latitude": "-15,78944444",
        "Longitude": "-47,92583332",
        "Altitude": "1160,96",
        "FoundingDate": "07/05/00",
    },
    # Case 2. Bad data: field FoundingDate incorrect
    {
        "Region": "CO",
        "State": "DF",
        "StationName": "BRASILIA",
        "IdStationWho": "A001",
        "Latitude": "-15,78944444",
        "Longitude": "-47,92583332",
        "Altitude": "1160,96",
        "FoundingDate": "2000-05-07",
    },
    # Weather data cases
    # Case 3. Good data only positives
    {
        "IdStationWho": "A001",
        "Date": "2023/01/01",
        "Time": "0100 UTC",
        "TotalPrecipitation": "0",
        "AtmosphericPressure": "888,1",
        "MaxAtmosphericPressure": "888,1",
        "MinAtmosphericPressure": "887,7",
        "GlobalRadiation": "1000",
        "DryBulbTemperature": "19,3",
        "DewPointTemperature": "17,6",
        "MaxTemperature": "19,5",
        "MinTemperature": "19",
        "MaxDewPointTemperature": "17,8",
        "MinDewPointTemperature": "17,3",
        "MaxRelativeHumidity": "90",
        "MinRelativeHumidity": "89",
        "RelativeHumidity": "90",
        "WindDirection": "145",
        "MaxWindGust": "2,1",
        "WindSpeed": "1,5",
    },
    # Case 4. Good data: negative values
    {
        "IdStationWho": "A001",
        "Date": "2023/01/01",
        "Time": "0100 UTC",
        "TotalPrecipitation": "0",
        "AtmosphericPressure": "888,1",
        "MaxAtmosphericPressure": "888,1",
        "MinAtmosphericPressure": "887,7",
        "GlobalRadiation": "1000",
        "DryBulbTemperature": "-19,3",
        "DewPointTemperature": "-17,6",
        "MaxTemperature": "-19,5",
        "MinTemperature": "-19",
        "MaxDewPointTemperature": "-17,8",
        "MinDewPointTemperature": "-17,3",
        "MaxRelativeHumidity": "90",
        "MinRelativeHumidity": "89",
        "RelativeHumidity": "90",
        "WindDirection": "145",
        "MaxWindGust": "2,1",
        "WindSpeed": "1,5",
    },
    # Case 5. Bad data: field required Date missing
    {
        "IdStationWho": "A001",
        "Time": "0100 UTC",
        "TotalPrecipitation": "0",
        "AtmosphericPressure": "888,1",
        "MaxAtmosphericPressure": "888,1",
        "MinAtmosphericPressure": "887,7",
        "GlobalRadiation": "1000",
        "DryBulbTemperature": "19,3",
        "DewPointTemperature": "17,6",
        "MaxTemperature": "19,5",
        "MinTemperature": "19",
        "MaxDewPointTemperature": "17,8",
        "MinDewPointTemperature": "17,3",
        "MaxRelativeHumidity": "90",
        "MinRelativeHumidity": "89",
        "RelativeHumidity": "90",
        "WindDirection": "145",
        "MaxWindGust": "2,1",
        "WindSpeed": "1,5",
    },
    # Case 6. Bad data: field required Time missing
    {
        "IdStationWho": "A001",
        "Date": "2023/01/01",
        "TotalPrecipitation": "0",
        "AtmosphericPressure": "888,1",
        "MaxAtmosphericPressure": "888,1",
        "MinAtmosphericPressure": "887,7",
        "GlobalRadiation": "1000",
        "DryBulbTemperature": "19,3",
        "DewPointTemperature": "17,6",
        "MaxTemperature": "19,5",
        "MinTemperature": "19",
        "MaxDewPointTemperature": "17,8",
        "MinDewPointTemperature": "17,3",
        "MaxRelativeHumidity": "90",
        "MinRelativeHumidity": "89",
        "RelativeHumidity": "90",
        "WindDirection": "145",
        "MaxWindGust": "2,1",
        "WindSpeed": "1,5",
    },
    # Case 7. Bad data: TotalPrecipitation (non-negative file) negative
    {
        "IdStationWho": "A001",
        "Date": "2023/01/01",
        "Time": "0100 UTC",
        "TotalPrecipitation": "-100",
        "AtmosphericPressure": "888,1",
        "MaxAtmosphericPressure": "888,1",
        "MinAtmosphericPressure": "887,7",
        "GlobalRadiation": "1000",
        "DryBulbTemperature": "19,3",
        "DewPointTemperature": "17,6",
        "MaxTemperature": "19,5",
        "MinTemperature": "19",
        "MaxDewPointTemperature": "17,8",
        "MinDewPointTemperature": "17,3",
        "MaxRelativeHumidity": "90",
        "MinRelativeHumidity": "89",
        "RelativeHumidity": "90",
        "WindDirection": "145",
        "MaxWindGust": "2,1",
        "WindSpeed": "1,5",
    },
    # Case 8. Good data: non-required field with NaN
    {
        "IdStationWho": "A001",
        "Date": "2023/01/01",
        "Time": "0100 UTC",
        "TotalPrecipitation": "0",
        "AtmosphericPressure": "888,1",
        "MaxAtmosphericPressure": "888,1",
        "MinAtmosphericPressure": "887,7",
        "GlobalRadiation": "",
        "DryBulbTemperature": "19,3",
        "DewPointTemperature": "17,6",
        "MaxTemperature": "19,5",
        "MinTemperature": "19",
        "MaxDewPointTemperature": "17,8",
        "MinDewPointTemperature": "17,3",
        "MaxRelativeHumidity": "90",
        "MinRelativeHumidity": "89",
        "RelativeHumidity": "90",
        "WindDirection": "145",
        "MaxWindGust": "2,1",
        "WindSpeed": "1,5",
    },
    # Case 9. Data with non-required missing
    {
        "IdStationWho": "A001",
        "Date": "2023/01/01",
        "Time": "0100 UTC",
        "TotalPrecipitation": "0",
        "AtmosphericPressure": "888,1",
        "MaxAtmosphericPressure": "888,1",
        "MinAtmosphericPressure": "887,7",
        "DryBulbTemperature": "19,3",
        "DewPointTemperature": "17,6",
        "MaxTemperature": "19,5",
        "MinTemperature": "19",
        "MaxDewPointTemperature": "17,8",
        "MinDewPointTemperature": "17,3",
        "MaxRelativeHumidity": "90",
        "MinRelativeHumidity": "89",
        "RelativeHumidity": "90",
        "WindDirection": "145",
        "MaxWindGust": "2,1",
        "WindSpeed": "1,5",
    },
]

file_name_list = ["test"] * len(dicts_input_test)

schema_list = [
    StationData,
    StationData,
    StationData,
    WeatherData,
    WeatherData,
    WeatherData,
    WeatherData,
]

output_list = [
    # Station data cases
    # Case 0. Good data: complete
    [
        "CO",
        "DF",
        "BRASILIA",
        "A001",
        -15.78944444,
        -47.92583332,
        1160.96,
        date(2000, 5, 7),
    ],
    # Case 1. Bad data: field required IdStationWho missing
    [],
    # Case 2. Bad data: field FoundingDate incorrect
    [],
    # Weather data cases
    # Case 3. Good data only positives
    [
        "A001",
        date(2023, 1, 1),
        time(1, 0),
        0.0,
        888.1,
        887.7,
        1000.0,
        19.3,
        17.6,
        19.5,
        19.0,
        17.8,
        17.3,
        90.0,
        89.0,
        90.0,
        145.0,
        2.1,
        1.5,
    ],
    # Case 4. Good data: negative values
    [
        "A001",
        date(2023, 1, 1),
        time(1, 0),
        0.0,
        888.1,
        887.7,
        1000.0,
        -19.3,
        -17.6,
        -19.5,
        -19.0,
        -17.8,
        -17.3,
        90.0,
        89.0,
        90.0,
        145.0,
        2.1,
        1.5,
    ],
    # Case 5. Bad data: field required Date missing
    [],
    # Case 6. Bad data: field required Time missing
    [],
    # Case 7. Bad data: TotalPrecipitation (non-negative file) negative
    [
        "A001",
        date(2023, 1, 1),
        time(1, 0),
        None,
        888.1,
        887.7,
        1000.0,
        19.3,
        17.6,
        19.5,
        19.0,
        17.8,
        17.3,
        90.0,
        89.0,
        90.0,
        145.0,
        2.1,
        1.5,
    ],
    # Case 8. Good data: non-required field with NaN
    [
        "A001",
        date(2023, 1, 1),
        time(1, 0),
        0.0,
        888.1,
        887.7,
        None,
        19.3,
        17.6,
        19.5,
        19.0,
        17.8,
        17.3,
        90.0,
        89.0,
        90.0,
        145.0,
        2.1,
        1.5,
    ],
    # Case 9. Data with non-required missing
    [
        "A001",
        date(2023, 1, 1),
        time(1, 0),
        0.0,
        888.1,
        887.7,
        None,
        19.3,
        17.6,
        19.5,
        19.0,
        17.8,
        17.3,
        90.0,
        89.0,
        90.0,
        145.0,
        2.1,
        1.5,
    ],
]

# Creating the list parameters

data_for_validation_parameters = list(
    zip(
        dicts_input_test,
        file_name_list,
        schema_list,
        output_list,
    ),
)


@pytest.mark.parametrize(
    "data_raw, file_name_log, data_schema, data_clean",
    data_for_validation_parameters,
)
def test_data_validation(
    tmp_path_factory,
    data_raw,
    file_name_log,
    data_schema,
    data_clean,
):
    """Validate the functionality of validate_data_quality.

    This test iterates through a series of predefined datasets, including
    both valid and invalid data, to verify the data quality validation
    process. It checks whether the function correctly processes valid data,
    identifies invalid data, and logs errors as expected.

    Parameters
    ----------
    tmp_path_factory : _pytest.tmpdir.TempPathFactory
        A fixture provided by pytest to create temporary directories.
    data_raw : dict
        The raw data dictionary to be validated. Represents a single row of
        data intended for processing by the validate_data_quality function.
    file_name_log : str
        The name of the log file used to record validation errors.
    data_schema : BaseModel
        The Pydantic model that the raw data is validated against.
    data_clean : list
        The expected processed data outcome from the validation function,
        for comparison with the actual result.

    Asserts
    -------
    Asserts that the processed data matches the expected data_clean list.
    Additionally, it checks if the log file's existence aligns with the
    presence of invalid data, ensuring that logs are created only when there
    are validation errors.
    """
    df = pd.DataFrame(
        data_raw,
        index=[0],
        dtype=str,
    )

    output_dir = tmp_path_factory.mktemp("data_validation")

    data_process = list(
        validate_data_quality(
            df,
            str(output_dir),
            file_name_log,
            data_schema,
        ),
    )

    if data_process:
        data_process_value = list(data_process[0].values())
        is_bad_data = False
    else:
        data_process_value = []
        is_bad_data = True

    log_output = "test_invalid_records.log"
    output_empty_file_path = output_dir / log_output

    assert data_process_value == data_clean
    assert os.path.exists(output_empty_file_path) == is_bad_data
