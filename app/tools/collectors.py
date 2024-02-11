# flake8: noqa:E501
"""Data Collectors.

This module provides utility functions for data collection.

"""

import glob
import os
from datetime import datetime, timedelta
from typing import Dict, List

import pandas as pd
from loguru import logger

from .logger_config import logger_decorator

try:
    from .validators import validate_data_quality, validate_sublists
except ImportError:
    from validators import validate_data_quality, validate_sublists  # type: ignore


class StationDataCollector:
    """Data collector about weather stations in Brazil."""

    def __init__(
        self,
        input_folder: str,
        output_path: str,
        file_name: str,
        column_names: Dict[str, str],
        schema,
    ):
        """Initialize a new StationDataCollector instance.

        Parameters
        ----------
        input_folder : str
            Path to the folder containing the data files.
        output_path : str
            Path to the folder where processed files will be stored.
        file_name : str
            Name of the output file.
        column_names : Dict[str, str]
            Mapping of original column names to new names.
        schema : Pydantic model
            Pydantic model for data validation.
        """
        self._input_folder = input_folder
        self._output_path = output_path
        self._file_name = file_name
        self._schema = schema
        self._column_names = column_names

    def start(self):
        """Start the data collection process."""
        response = self.get_data()

        self.validate_data(response)

        response = self.transform_data(
            response,
            self._column_names,
            self._output_path,
            self._file_name,
        )

        self.load_data(response, self._output_path, self._file_name)

    @logger_decorator
    def get_data(self) -> List[pd.DataFrame]:
        """
        Retrieve data from CSV files and returns it as a list of DataFrames.

        Returns
        -------
        List[pd.DataFrame]
            List of partially processed pandas DataFrames.

        Raises
        ------
        ValueError
            If no CSV files are found in the specified
            directory.
        """
        # Collecting all CSV file paths from the input folder
        files = glob.glob(os.path.join(self._input_folder, "*.csv"))
        files.extend(glob.glob(os.path.join(self._input_folder, "*.CSV")))
        if not files:
            raise ValueError("No CSV files found in the specified folder.")

        all_data = [
            pd.read_csv(
                file,
                encoding="latin-1",
                nrows=8,
                sep=";",
                header=None,
                decimal=",",
                index_col=0,
            ).T
            for file in files
        ]

        return all_data

    @logger_decorator
    def validate_data(self, all_data: List[pd.DataFrame]) -> None:
        """Validate the structure of the collected data.

        Parameters
        ----------
        data_frames : List[pd.DataFrame]
            List of DataFrames to be validated.

        Raises
        ------
        ValueError
            If the structure of any DataFrame is inconsistent.
        """
        list_columns = [df.columns.to_list() for df in all_data]
        validate_sublists(list_columns)

    @logger_decorator
    def transform_data(
        self,
        all_data: List[pd.DataFrame],
        column_names: Dict[str, str],
        output_path: str,
        file_name: str,
    ) -> pd.DataFrame:
        """Transform and validates the collected data.

        Concatenates all DataFrames, renames columns, validates data quality,
        remove duplicates, and logs any invalid records.

        Parameters
        ----------
        data_frames : List[pd.DataFrame]
            List of DataFrames to be transformed.

        Returns
        -------
        pd.DataFrame
            Concatenated and validated DataFrame.

        Raises
        ------
        Exception
            If all collected data is invalid.
        """
        logger.info("Concatenating the data")
        raw_data = pd.concat(all_data)
        raw_data = raw_data.rename(columns=column_names)
        raw_rows, _ = raw_data.shape
        logger.info(f"Total rows to process = {raw_rows:,}")

        validate_data = pd.DataFrame(
            list(
                validate_data_quality(
                    raw_data,
                    output_path,
                    file_name,
                    self._schema,
                ),
            ),
        )

        logger.info("Data validation finished")
        if len(validate_data) > 0:
            return validate_data
        else:
            raise Exception("All collected data was invalid.")

    @logger_decorator
    def load_data(
        self,
        validate_data: pd.DataFrame,
        load_path: str,
        file_name: str,
    ) -> None:
        """Save the validated data to a Parquet file.

        Parameters
        ----------
        validated_data : pd.DataFrame
            DataFrame containing validated station data.


        Raises
        ------
        Exception
        If there is an error converting data to Parquet.

        """
        try:
            validate_data.to_parquet(
                os.path.join(
                    load_path,
                    file_name + ".parquet",
                ),
            )
        except Exception as e:
            print(f"Error to convert data to parquet: {e}")


class WeatherDataCollector:
    """Data collector about weather collect by stations in Brazil."""

    def __init__(
        self,
        input_folder: str,
        output_path: str,
        file_name: str,
        column_names: Dict[str, str],
        schema,
    ):
        """Initialize a new StationDataCollector instance.

        Parameters
        ----------
        input_folder : str
            Path to the folder containing the data files.
        output_path : str
            Path to the folder where processed files will be stored.
        file_name : str
            Name of the output file.
        column_names : Dict[str, str]
            Mapping of original column names to new names.
        schema : Pydantic model
            Pydantic model for data validation.
        """
        self._input_folder = input_folder
        self._output_path = output_path
        self._file_name = file_name
        self._schema = schema
        self._column_names = column_names

    def start(self):
        """Start the data collection process."""
        response, data_files = self.get_data()

        self.validate_data(response)

        response = self.transform_data(
            response,
            self._column_names,
            self._output_path,
            data_files,
        )

        self.load_data(response, self._output_path, self._file_name)

    @logger_decorator
    def get_data(self) -> tuple[List[pd.DataFrame], List[str]]:
        """
        Retrieve data from CSV files and returns it as a list of DataFrames.

        Returns
        -------
        all_data : List[pd.DataFrame]
            List of partially processed pandas DataFrames.

        data_files : List[str]
            List of files in process.

        Raises
        ------
        ValueError
            If no CSV files are found in the specified
            directory.
        """
        # Collecting all CSV file paths from the input folder
        data_files = glob.glob(os.path.join(self._input_folder, "*.csv"))
        data_files.extend(glob.glob(os.path.join(self._input_folder, "*.CSV")))
        if not data_files:
            raise ValueError("No CSV files found in the specified folder.")

        all_data = [
            pd.read_csv(
                file,
                encoding="latin-1",
                skiprows=8,
                sep=";",
                usecols=[x for x in range(0, 19)],
                na_values=["-9999"],
                dtype=str,
            )
            for file in data_files
        ]

        stations_data = [
            pd.read_csv(
                file,
                encoding="latin-1",
                nrows=8,
                sep=";",
                header=None,
                decimal=",",
                index_col=0,
            ).T
            for file in data_files
        ]

        for i, df in enumerate(all_data):
            df["IdStationWho"] = stations_data[i].loc[
                1,
                "CODIGO (WMO):",
            ]

        return all_data, data_files

    @logger_decorator
    def validate_data(self, all_data: List[pd.DataFrame]) -> None:
        """Validate the structure of the collected data.

        Parameters
        ----------
        all_data : List[pd.DataFrame]
            List of DataFrames to be validated.

        Raises
        ------
        ValueError
            If the structure of any DataFrame is inconsistent.
        """
        list_columns = [df.columns.to_list() for df in all_data]
        validate_sublists(list_columns)

    @logger_decorator
    def transform_data(
        self,
        all_data: List[pd.DataFrame],
        column_names: Dict[str, str],
        output_path: str,
        data_files: List[str],
    ) -> pd.DataFrame:
        """Transform and validates the collected data.

        Concatenates all DataFrames, renames columns, validates data quality,
        remove duplicates, and logs any invalid records.

        Parameters
        ----------
        data_frames : List[pd.DataFrame]
            List of DataFrames to be transformed.

        Returns
        -------
        pd.DataFrame
            Concatenated and validated DataFrame.

        Raises
        ------
        Exception
            If all collected data is invalid.
        """
        files_processing = len(all_data)
        logger.info(f"Total files to process = {files_processing:,}")

        logger.info("Analyzing data quality")
        process_data = []
        for i, df in enumerate(all_data):
            logger.info(f"Files processed: {i+1}")
            df = df.rename(columns=column_names)
            good_data = list(
                validate_data_quality(
                    df,
                    output_path,
                    self._file_name,
                    self._schema,
                ),
            )
            if len(good_data) > 0:
                good_df = pd.DataFrame(good_data)

            if len(good_df) > 0:
                process_data.append(
                    good_df,
                )

        logger.info("Data validation finished")

        if len(process_data) > 0:
            validate_data = pd.concat(process_data)
            return validate_data
        else:
            raise Exception("All collected data was invalid.")

    @logger_decorator
    def load_data(
        self,
        validate_data: pd.DataFrame,
        load_path: str,
        file_name: str,
    ) -> None:
        """Save the validated data to a Parquet file.

        Parameters
        ----------
        validated_data : pd.DataFrame
            DataFrame containing validated station data.


        Raises
        ------
        Exception
        If there is an error converting data to Parquet.

        """
        try:
            validate_data.to_parquet(
                os.path.join(
                    load_path,
                    file_name + ".parquet",
                ),
            )
        except Exception as e:
            print(f"Error to convert data to parquet: {e}")


def limit_years() -> tuple[int, int]:
    """Calculate the valid year range for data collection.

    Determines the earliest year from which data can be collected and the
    latest valid year based on the current date.

    Returns
    -------
    tuple[int, int]
        A tuple containing two integers:
        - The first integer is the earliest year from which data collection
        is valid.
        - The second integer is the latest valid year for data collection
        based on the current date.
    """
    FIRST_YEAR_WITH_DATA = 2000
    current_day = datetime.now().day
    last_year_month = datetime.today() - timedelta(current_day + 1)
    last_valid_year = last_year_month.year

    return FIRST_YEAR_WITH_DATA, last_valid_year


def collect_years_list(
    list_years: List[int],
) -> List[int]:
    """Extract the valid years from a list.

    This function collects years beginning in 2000 and ending in the
    year from the last month. It filters out any years that are not
    integers or are outside the valid range.

    Parameters
    ----------
    list_years : List[int]
        List with the years that will be processed.

    Returns
    -------
    List[int]
        A list containing only the valid years within the specified range.

    Raises
    ------
    ValueError
        If the list is empty or contains no valid years.
    """
    first_year, last_year = limit_years()
    valid_years = []
    invalid_years = []

    if len(list_years) == 0:
        raise ValueError(
            f"The list is empty. Provide a list with years after {first_year} and before {last_year}.",  # noqa
        )

    for year in list_years:
        if not isinstance(year, int):
            invalid_years.append(year)
        elif first_year <= year <= last_year:
            valid_years.append(year)
        else:
            invalid_years.append(year)

    if len(valid_years) == 0:
        raise ValueError(
            f"The list is empty. Provide a list with years after {first_year} and before {last_year}.",  # noqa
        )

    if len(invalid_years) > 0:
        print(f"The elements {invalid_years} were removed from the list.")

    return valid_years
