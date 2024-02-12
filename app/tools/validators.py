# flake8: noqa:E501
"""Data Validators.

This module provides utility functions for data validation, such as
asserting that all sublists within a list have the same elements, regardless
of their order.

"""

import os
from datetime import date, datetime, time
from typing import Any, List

import pandas as pd
from pydantic import (
    BaseModel,
    Field,
    StringConstraints,
    ValidationError,
    field_validator,
)
from typing_extensions import Annotated


def validate_sublists(list_with_sublists: List[List[str]]) -> bool:
    """Confirm if all sublists within a given list contain identical elements, regardless of their order.

    This function is crucial for ensuring dataframes have consistent column
    names across multiple files. It assesses each sublist (representing
    dataframe columns) to verify they all contain the same elements
    (column names).

    Parameters:
    ----------
    list_with_sublists : List[List[str]]
        A list containing sublists to be validated for identical elements.

    Returns:
    -------
    bool
        True if all sublists contain identical elements; False otherwise.

    Raises:
    ------
    ValueError
        Raised if any sublist differs in elements, indicating inconsistent
        column names.

    Examples:
    --------
    >>> validate_sublists([["A", "B"], ["B", "A"]])
    True

    >>> validate_sublists([["C", "B"], ["B", "A"]])
    ValueError: Sublists do not contain the same elements.
    """
    set_columns = set(tuple(sorted(sublist)) for sublist in list_with_sublists)
    if len(set_columns) == 1:
        return True
    else:
        raise ValueError("Sublists do not have the same elements.")


def validate_data_quality(
    df: pd.DataFrame,
    output_path: str,
    file_name: str,
    schema: BaseModel,
):
    """Validate each row in a DataFrame against a Pydantic schema and log validation errors.

    This function iterates through the DataFrame, attempting to create
    instances of the specified Pydantic schema with each row's data.
    If a row fails validation, the error is logged. The log file is
    created only if there are invalid records.

    Parameters:
    ----------
    df : pd.DataFrame
        The DataFrame containing data to be validated.
    output_path : str
        The directory path where the log file will be saved, if necessary.
    file_name : str
        The name of the log file for recording validation errors, without
        the extension.
    schema : BaseModel
        The Pydantic model against which data rows will be validated.

    Yields:
    ------
    Yields instances of the Pydantic schema for valid rows or logs validation
    errors for invalid rows.
    """
    error_messages = []
    for index, row in df.iterrows():
        try:
            yield schema(**row.to_dict()).model_dump()
        except ValidationError as e:
            error_message = f"{index}: {e.json()}\n"
            error_messages.append(error_message)

    if error_messages:
        log_path = os.path.join(
            output_path,
            f"{file_name}_invalid_records.log",
        )
        with open(log_path, "w") as file:
            file.writelines(error_messages)


# IdStationType definition was created to share this field between the modules
IdStationWhoType = Annotated[
    str,
    StringConstraints(
        min_length=4,
        max_length=4,
        to_upper=True,
        pattern=r"[A-Z]\d{3}",  # noqa
    ),
]


class StationData(BaseModel):
    """Represent the weather station's data with validation rules, ensuring data consistency and integrity.

    This model includes comprehensive validation for each attribute to ensure
    that data about weather stations is accurate and in the correct format. It
    handles geographical coordinates, station identification, and operational
    dates with specific constraints.

    Attributes:
    ----------
    Region : str
        The geographical region code of the weather station, automatically
        converted to uppercase. It must be between 1 to 2 characters long.
    State : str
        The state code where the weather station is located, automatically
        converted to uppercase and required to be exactly 2 characters long.
    StationName : str
        The name of the weather station, automatically converted to uppercase.
        It can include both letters and numbers.
    IdStationWho : IdStationWhoType
        A unique identifier for the weather station, following a specific
        format ('A' followed by 3 digits).
    Latitude : float
        The geographical latitude of the station. This model accepts both
        comma and dot as decimal separators to accommodate different formats.
    Longitude : float
        The geographical longitude of the station. Similar to Latitude,
        it accepts both comma and dot for decimal separation.
    Altitude : float
        The station's altitude in meters above sea level. Accepts string
        input with comma or dot decimal separators and converts it to a float.
    FoundingDate : date
        The date when the station was established. It supports various date
        formats, including 'dd/mm/yyyy' and 'dd/mm/yy', and ensures that the
        date is converted into a standard date object.

    Methods:
    --------
    parse_geo_coords(cls, value: str) -> float:
        Class method to parse geographical coordinates from string to float.
        It's designed to accommodate the Brazilian format for decimal numbers,
        converting commas to dots.
    parse_date(cls, value: str) -> date:
        Class method to parse and validate the founding date from a string
        into a `datetime.date` object. It supports multiple date formats
        for flexibility.
    """

    Region: Annotated[
        str,
        StringConstraints(
            min_length=1,
            max_length=2,
            to_upper=True,
        ),
    ]
    State: Annotated[
        str,
        StringConstraints(
            min_length=2,
            max_length=2,
            to_upper=True,
        ),
    ]
    StationName: Annotated[
        str,
        StringConstraints(to_upper=True),
    ]
    IdStationWho: IdStationWhoType
    Latitude: Any
    Longitude: Any
    Altitude: Any
    FoundingDate: Any

    @field_validator(
        "Latitude",
        "Longitude",
        "Altitude",
        mode="before",
    )
    @classmethod
    def parse_geo_coords(cls, value):
        """Parse a string input representing geographic coordinates, converting it to a float.

        This method accommodates the common Brazilian format for decimal
        numbers, where commas are used as decimal separators.

        Parameters:
        ----------
        value : str
            The geographic coordinate as a string, potentially using a comma
            for decimal separation.

        Returns:
        -------
        float
            The geographic coordinate as a float.

        Raises:
        ------
        ValueError
            If the input string cannot be parsed into a float, indicating an
            invalid format.
        """
        try:
            return float(value.replace(",", "."))
        except ValueError:
            raise ValueError(f"Geographic Coordinate Invalid: {value}")

    @field_validator(
        "FoundingDate",
        mode="before",
    )
    @classmethod
    def parse_date(cls, value):
        """
        Parse and validate foundation dates in multiple formats.

        This validator function attempts to parse the date from a given
        string. It supports two date formats: 'dd/mm/yyyy' and 'dd/mm/yy'.
        This flexibility allows for handling variations in the date format.

        Parameters
        ----------
        value : str
            The string value of the date to be parsed.

        Returns
        -------
        datetime.date
            The parsed date as a datetime.date object.

        Raises
        ------
        ValueError
            If the provided value does not match any of the supported date
            formats.

        Example
        -------
        >>> parse_date("19/07/2020")
        datetime.date(2020, 7, 19)
        >>> parse_date("19/07/20")
        datetime.date(2020, 7, 19)
        """
        for date_format in ("%d/%m/%Y", "%d/%m/%y"):
            try:
                return datetime.strptime(value, date_format).date()
            except ValueError:
                pass
        raise ValueError(f"Foundation Date Invalid: {value}")


class WeatherData(BaseModel):
    """Represent meteorological data for a weather station, ensuring data integrity through validation.

    This model encapsulates and validates a range of meteorological
    measurements, such as temperature, humidity, atmospheric pressure, wind
    speed, and direction. It is designed to accommodate the nuances
    of meteorological data, including the allowance of NaN values for certain
    fields where data might be missing.

    Attributes:
    -----------
    IdStationWho : IdStationWhoType
        The unique identifier for the weather station, adhering to a specific
        format.
    Date : date
        The date on which the meteorological measurements were taken.
    Time : time
        The time at which the meteorological measurements were recorded, with
        support for UTC notation.
    TotalPrecipitation : float
        The total precipitation measured in millimeters. NaN values are
        permitted to indicate missing or invalid data.
    MaxAtmosphericPressure : float
        The maximum atmospheric pressure measured. NaN values are
        permitted to indicate missing or invalid data.
    MinAtmosphericPressure : float
        The minimum atmospheric pressure measured. NaN values are
        permitted to indicate missing or invalid data.
    GlobalRadiation : float
        The global radiation measured in Kj/m². NaN values are
        permitted to indicate missing or invalid data.
    DryBulbTemperature : float
        The air temperature measured by a dry bulb thermometer in degrees
        Celsius. NaN values are permitted to indicate missing or invalid data.
    DewPointTemperature : float
        The dew point temperature in degrees Celsius.  NaN values are
        permitted to indicate missing or invalid data.
    MaxTemperature : float
        The maximum temperature recorded in the last hour in degrees Celsius.
        NaN values are permitted to indicate missing or invalid data.
    MinTemperature : float
        The minimum temperature recorded in the last hour in degrees Celsius.
        NaN values are permitted to indicate missing or invalid data.
    MaxDewPointTemperature : float
        The maximum dew point temperature recorded in the last hour in degrees
        Celsius.  NaN values are permitted to indicate missing or invalid data.
    MinDewPointTemperature : float
        The minimum dew point temperature recorded in the last hour in degrees
        Celsius.  NaN values are permitted to indicate missing or invalid data.
    MaxRelativeHumidity : float
        The maximum relative humidity recorded in the last hour, expressed as
        a percentage.  NaN values are permitted to indicate missing or
        invalid data.
    MinRelativeHumidity : float
        The minimum relative humidity recorded in the last hour, expressed as
        a percentage. Allows NaN values.
    RelativeHumidity : float
        The relative humidity, expressed as a percentage. NaN values are
        permitted to indicate missing or invalid data.
    WindDirection : float
        The wind direction, in degrees from true north. NaN values are
        permitted to indicate missing or invalid data.
    MaxWindGust : float
        The maximum wind gust speed recorded in meters per second. NaN values
        are permitted to indicate missing or invalid data.
    WindSpeed : float
        The wind speed in meters per second. NaN values are
        permitted to indicate missing or invalid data.

    Methods:
    --------
    parse_custom_date_format(value: str) -> date:
        Parses and validates a date string formatted as 'yyyy/mm/dd',
        ensuring it conforms to this specific format.
    parse_time_utc(value: str) -> time:
        Parses and validates a time string formatted with UTC notation
        ('HHMM UTC'), converting it to a `time` object.
    parse_to_float(value: float) -> float | None:
        Validates and adjusts float fields, specifically handling
        NaN values.
    set_nan_out_range(value: float) -> float | None:
        Validates and adjusts float fields, specifically handling
        NaN values and converting negative values to None.

    Notes:
    ------
    The inclusion of NaN values and the conversion of negative
    values to None are crucial for maintaining the integrity of
    meteorological data, acknowledging the presence of missing
    or non-applicable measurements.
    """

    IdStationWho: IdStationWhoType
    Date: date
    Time: time
    TotalPrecipitation: Annotated[Any, Field(default=None)]
    MaxAtmosphericPressure: Annotated[Any, Field(default=None)]
    MinAtmosphericPressure: Annotated[Any, Field(default=None)]
    GlobalRadiation: Annotated[Any, Field(default=None)]
    DryBulbTemperature: Annotated[Any, Field(default=None)]
    DewPointTemperature: Annotated[Any, Field(default=None)]
    MaxTemperature: Annotated[Any, Field(default=None)]
    MinTemperature: Annotated[Any, Field(default=None)]
    MaxDewPointTemperature: Annotated[Any, Field(default=None)]
    MinDewPointTemperature: Annotated[Any, Field(default=None)]
    MaxRelativeHumidity: Annotated[Any, Field(default=None)]
    MinRelativeHumidity: Annotated[Any, Field(default=None)]
    RelativeHumidity: Annotated[Any, Field(default=None)]
    WindDirection: Annotated[Any, Field(default=None)]
    MaxWindGust: Annotated[Any, Field(default=None)]
    WindSpeed: Annotated[Any, Field(default=None)]

    @field_validator(
        "Date",
        mode="before",
    )
    @classmethod
    def parse_custom_date_format(
        cls,
        value,
    ):
        """Parse and validates a string representing a date into a date object, expecting the format 'yyyy/mm/dd'.

        This method is designed to ensure consistency in date representation
        within meteorological data, specifically accommodating the
        international standard format.

        Parameters:
        ----------
        value : str
            The string representation of a date, expected to be in the
            'yyyy/mm/dd' format.

        Returns:
        -------
        datetime.date
            The date converted into a date object.

        Raises:
        ------
        ValueError
            If the input string does not match the expected date format,
            indicating an invalid date format.
        """
        if isinstance(value, str):
            try:
                return datetime.strptime(value, "%Y/%m/%d").date()
            except ValueError:
                raise ValueError(
                    f"Invalid date format for {value}, expected yyyy/mm/dd",
                )
        return value

    @field_validator(
        "Time",
        mode="before",
    )
    @classmethod
    def parse_time_utc(
        cls,
        value,
    ):
        """Parse a string representing time with UTC notation ('HHMM UTC') into a time object.

        This function standardizes the representation of time within the
        dataset, aligning with international time notation standards.

        Parameters:
        ----------
        value : str
            The time as a string in 'HHMM UTC' format.

        Returns:
        -------
        datetime.time
            The time converted into a time object.

        Raises:
        ------
        ValueError
            If the string is not in the 'HHMM UTC' format or cannot be parsed
            into a time object.
        """
        if value and "UTC" in value:
            hour = int(value.split(" ")[0][:2])
            minute = int(value.split(" ")[0][2:])
            return time(hour, minute)
        raise ValueError("Invalid time format.")

    @field_validator(
        "DryBulbTemperature",
        "DewPointTemperature",
        "MaxTemperature",
        "MinTemperature",
        "MaxDewPointTemperature",
        "MinDewPointTemperature",
        mode="before",
    )
    @classmethod
    def parse_to_float(
        cls,
        value,
    ):
        """Parse and validates string inputs for temperature and dew point fields, allowing for Brazilian numeric format.

        Converts string representations of numerical values, which may use
        commas as decimal separators, into floats. This caters to the
        Brazilian format for decimal numbers and ensures that the data
        is accurately represented and validated.

        Parameters:
        ----------
        value : str
            The string representation of a numerical value, potentially
            using a comma as the decimal separator.

        Returns:
        -------
        float
            The numeric value converted into a float.

        Raises:
        ------
        ValueError
            If the input value cannot be converted into a float,
            indicating an invalid numeric format.
        """
        try:
            if value:
                return float(str(value).replace(",", "."))
            else:
                return None
        except ValueError:
            return None

    @field_validator(
        "TotalPrecipitation",
        "MaxAtmosphericPressure",
        "MinAtmosphericPressure",
        "MaxRelativeHumidity",
        "MinRelativeHumidity",
        "RelativeHumidity",
        "WindDirection",
        "MaxWindGust",
        "WindSpeed",
        "GlobalRadiation",
        mode="before",
    )
    @classmethod
    def set_nan_out_range(
        cls,
        value,
    ):
        """Validate numerical fields, allowing NaN values and converting negative or improperly formatted values to None.

        This method ensures that meteorological measurements are within logical
        ranges, acknowledging the possibility of missing data (represented as
        NaN) and correcting any negative values that do not make sense in the
        context of the measurement being taken.

        Parameters:
        ----------
        value : Any
            The value to validate, which may be a numerical value or NaN.

        Returns:
        -------
        float | None
            The original value if it's a valid number or None if the value
            is negative or improperly formatted.

        Note:
        ----
        This method emphasizes the flexibility required in handling
        meteorological data, particularly in accommodating missing data
        points and ensuring data integrity.
        """
        if value:
            float_parsed = float(str(value).replace(",", "."))
            if float_parsed >= 0:
                return float_parsed
        return None
