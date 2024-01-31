"""Data Validators.

This module provides utility functions for data validation, such as
asserting that all sublists within a list have the same elements, regardless
of their order.

"""

from datetime import datetime
from typing import Any, List

from pydantic import BaseModel, StringConstraints, field_validator
from typing_extensions import Annotated


def validate_sublists(list_with_sublists: List[List[str]]) -> bool:
    """Validate whether all sublists within a list contain the same elements.

    This function is useful for validating if a list of dataframes has
    consistent column names. It checks if every sublist (representing
    dataframe columns) contains the same set of elements (column names).

    Parameters
    ----------
    list_with_sublists : List[List[str]]
        List containing sublists to be validated.

    Returns
    -------
    bool
        Returns True if all sublists have the same elements, otherwise raises
        an exception.

    Raises
    ------
    ValueError
        If any of the sublists differ in elements.

    Examples
    --------
    >>> validate_sublists([["A", "B"], ["B", "A"]])
    True
    >>> validate_sublists([["C", "B"], ["B", "A"]])
    ValueError: Sublists do not have the same elements.
    """
    set_columns = set(tuple(sorted(sublist)) for sublist in list_with_sublists)
    if len(set_columns) == 1:
        return True
    else:
        raise ValueError("Sublists do not have the same elements.")


class StationData(BaseModel):
    """
    A Pydantic model to validate station data.

    This model applies constraints to various fields related to station
    information such as region, state, name, and geographical coordinates.
    It ensures that each field conforms to specific rules like length,format,
    and data type, making sure the data is consistent and reliable.

    Fields:
    - Region: 2-letter code, uppercase.
    - State: 2-letter state code, uppercase.
    - StationName: Name of the station, converted to uppercase.
    - IdStationWho: Unique station ID, format 'A' followed by 3 digits.
    - Latitude, Longitude, Altitude: Geographical coordinates.
    - FoundingDate: Date the station was founded, supports two date formats.
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
    StationName: Annotated[str, StringConstraints(to_upper=True)]
    IdStationWho: Annotated[
        str,
        StringConstraints(
            min_length=4,
            max_length=4,
            to_upper=True,
            pattern=r"[A-Z]\d{3}",  # noqa
        ),
    ]
    Latitude: Any
    Longitude: Any
    Altitude: Any
    FoundingDate: Any

    @field_validator("Latitude", "Longitude", "Altitude", mode="before")
    @classmethod
    def parse_geo_coords(cls, value):
        """
        Parse and validate geographic coordinate values.

        This validator function is responsible for converting geographic
        coordinates from a string format, potentially using a comma as the
        decimal separator, to a float. It ensures that the coordinates are
        in a valid numeric format.

        Parameters
        ----------
        value : str
            The string value of the geographic coordinate to be parsed.

        Returns
        -------
        float
            The parsed geographic coordinate as a float.

        Raises
        ------
        ValueError
            If the provided value cannot be converted to a float or if it's
            not in a valid numeric format.

        Example
        -------
        >>> parse_geo_coords("123,45")
        123.45
        """
        try:
            return float(value.replace(",", "."))
        except ValueError:
            raise ValueError(f"Geographic Coordinate Invalid: {value}")

    @field_validator("FoundingDate", mode="before")
    @classmethod
    def parse_date(cls, value):
        """
        Parse and validate foundation dates in multiple formats.

        This validator function attempts to parse the date from a given string.
        It supports two date formats: 'dd/mm/yyyy' and 'dd/mm/yy'. This
        flexibility allows for handling variations in the date format.

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
