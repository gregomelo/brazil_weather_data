# flake8: noqa:E501
"""Data Collectors.

This module provides utility functions for data collection.

"""

import glob
import os
from datetime import datetime, timedelta
from typing import Dict, List, Tuple

import pandas as pd
from loguru import logger

from .logger_config import logger_decorator

try:
    from .validators import validate_data_quality, validate_sublists
except ImportError:
    from validators import validate_data_quality, validate_sublists  # type: ignore


class StationDataCollector:
    """Data collector for weather stations in Brazil.

    This class is designed to collect, validate, transform, and save data from
    CSV files related to weather stations across Brazil. It utilizes a
    Pydantic model for data validation to ensure the integrity and accuracy
    of the data collected.

    Parameters:
    ----------
    input_folder : str
        Directory containing the source CSV files.
    output_path : str
        Directory where processed files will be saved.
    file_name : str
        Base name for the output file.
    column_names : Dict[str, str]
        Mapping from original column names to desired column names.
    schema : BaseModel
        Pydantic model used for validating the data.

    Attributes:
    ----------
    _input_folder : str
        Input directory for source CSV files.
    _output_path : str
        Output directory for processed data.
    _file_name : str
        Base name for the output file.
    _schema : BaseModel
        Pydantic model for data validation.
    _column_names : Dict[str, str]
        Column name mapping.
    """

    def __init__(
        self,
        input_folder: str,
        output_path: str,
        file_name: str,
        column_names: Dict[str, str],
        schema,
    ):
        """Initialize a new StationDataCollector instance.

        Parameters:
        ----------
        input_folder : str
            Directory containing the source CSV files.
        output_path : str
            Directory where processed files will be saved.
        file_name : str
            Base name for the output file.
        column_names : Dict[str, str]
            Mapping from original column names to desired column names.
        schema : BaseModel
            Pydantic model used for validating the data.
        """
        self._input_folder = input_folder
        self._output_path = output_path
        self._file_name = file_name
        self._schema = schema
        self._column_names = column_names

    def start(self):
        """Initiate the data collection process.

        This method orchestrates the workflow of data collection including
        retrieving, validating, transforming, and storing data.
        """
        response, data_files = self.get_data()

        self.validate_data(response)

        response = self.transform_data(
            response,
            self._column_names,
            self._output_path,
            data_files,
        )

        self.load_data(
            response,
            self._output_path,
            self._file_name,
        )

    @logger_decorator
    def get_data(self) -> Tuple[List[pd.DataFrame], List[str]]:
        """Get data from CSV files.

        Returns:
        -------
        tuple[List[pd.DataFrame], List[str]]
            A tuple containing a list of DataFrames representing the data and
            a list of strings representing the file names.

        Raises:
        ------
        ValueError
            If no CSV files are found in the specified directory.
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
                nrows=8,
                sep=";",
                header=None,
                decimal=",",
                index_col=0,
            ).T
            for file in data_files
        ]

        return all_data, data_files

    @logger_decorator
    def validate_data(self, all_data: List[pd.DataFrame]) -> None:
        """Validate the structure of the collected data.

        Parameters:
        ----------
        all_data : List[pd.DataFrame]
            List of DataFrames to be validated.

        Raises:
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
        """Transform and validate the collected data.

        Parameters:
        ----------
        all_data : List[pd.DataFrame]
            List of DataFrames to be transformed.
        column_names : Dict[str, str]
            Mapping of original column names to new names.
        output_path : str
            Path to the folder where the processed data will be saved.
        data_files : List[str]
            List of file names being processed.

        Returns:
        -------
        pd.DataFrame
            Concatenated and validated DataFrame.

        Raises:
        ------
        Exception
            If all collected data is invalid.
        """
        files_processing = len(all_data)
        logger.info(f"Total files to process = {files_processing:,}")

        logger.info("Analyzing data quality")
        process_data = []
        good_df = pd.DataFrame()
        for i, df in enumerate(all_data):
            logger.info(f"Files to process: {files_processing-i:,}")

            file_name_process = data_files[i].split("/")[-1]
            file_log = file_name_process.replace(".", "_")

            df = df.rename(columns=column_names)
            good_data = list(
                validate_data_quality(
                    df,
                    output_path,
                    file_log,
                    self._schema,
                ),
            )
            if good_data:
                good_df = pd.DataFrame(good_data)
                process_data.append(good_df)
            else:
                good_df = pd.DataFrame()

        logger.info("Data validation finished")

        # Avoiding concatenating empty dataframes and warnings from Pandas
        process_data_filtered = [
            df for df in process_data if not df.empty and not df.isna().all().all()
        ]
        if process_data_filtered:
            validate_data = pd.concat(process_data_filtered)
            validate_data = validate_data.drop_duplicates(["IdStationWho"])
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

        Parameters:
        ----------
        validate_data : pd.DataFrame
            DataFrame containing validated station data.
        load_path : str
            Path to the folder where the Parquet file will be saved.
        file_name : str
            Name of the output file without the extension.

        Raises:
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
                index=False,
            )
        except Exception as e:
            print(f"Error to convert data to parquet: {e}")


class WeatherDataCollector:
    """Data collector for weather in Brazil.

    This class is designed to collect, validate, transform, and save data from
    CSV files related to weather stations across Brazil. It utilizes a
    Pydantic model for data validation to ensure the integrity and accuracy
    of the data collected.

    Parameters:
    ----------
    input_folder : str
        Directory containing the source CSV files.
    output_path : str
        Directory where processed files will be saved.
    file_name : str
        Base name for the output file.
    column_names : Dict[str, str]
        Mapping from original column names to desired column names.
    schema : BaseModel
        Pydantic model used for validating the data.

    Attributes:
    ----------
    _input_folder : str
        Input directory for source CSV files.
    _output_path : str
        Output directory for processed data.
    _file_name : str
        Base name for the output file.
    _schema : BaseModel
        Pydantic model for data validation.
    _column_names : Dict[str, str]
        Column name mapping.
    """

    def __init__(
        self,
        input_folder: str,
        output_path: str,
        file_name: str,
        column_names: Dict[str, str],
        schema,
    ):
        """Initialize a new StationDataCollector instance.

        Parameters:
        ----------
        input_folder : str
            Directory containing the source CSV files.
        output_path : str
            Directory where processed files will be saved.
        file_name : str
            Base name for the output file.
        column_names : Dict[str, str]
            Mapping from original column names to desired column names.
        schema : BaseModel
            Pydantic model used for validating the data.
        """
        self._input_folder = input_folder
        self._output_path = output_path
        self._file_name = file_name
        self._schema = schema
        self._column_names = column_names

    def start(self):
        """Initiate the data collection process.

        This method orchestrates the workflow of data collection including
        retrieving, validating, transforming, and storing data.
        """
        response, data_files = self.get_data()

        self.validate_data(response)

        response = self.transform_data(
            response,
            self._column_names,
            self._output_path,
            data_files,
        )

        self.load_data(
            response,
            self._output_path,
            self._file_name,
        )

    @logger_decorator
    def get_data(self) -> tuple[List[pd.DataFrame], List[str]]:
        """Get data from CSV files.

        Returns:
        -------
        tuple[List[pd.DataFrame], List[str]]
            A tuple containing a list of DataFrames representing the data and
            a list of strings representing the file names.

        Raises:
        ------
        ValueError
            If no CSV files are found in the specified directory.
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

        Parameters:
        ----------
        all_data : List[pd.DataFrame]
            List of DataFrames to be validated.

        Raises:
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
        """Transform and validate the collected data.

        Parameters:
        ----------
        all_data : List[pd.DataFrame]
            List of DataFrames to be transformed.
        column_names : Dict[str, str]
            Mapping of original column names to new names.
        output_path : str
            Path to the folder where the processed data will be saved.
        data_files : List[str]
            List of file names being processed.

        Returns:
        -------
        pd.DataFrame
            Concatenated and validated DataFrame.

        Raises:
        ------
        Exception
            If all collected data is invalid.
        """
        files_processing = len(all_data)
        logger.info(f"Total files to process = {files_processing:,}")

        logger.info("Analyzing data quality")
        process_data = []
        good_df = pd.DataFrame()
        for i, df in enumerate(all_data):
            logger.info(f"Files to process: {files_processing-i:,}")

            file_name_process = data_files[i].split("/")[-1]
            file_log = file_name_process.replace(".", "_")

            df = df.rename(columns=column_names)
            good_data = list(
                validate_data_quality(
                    df,
                    output_path,
                    file_log,
                    self._schema,
                ),
            )
            if good_data:
                good_df = pd.DataFrame(good_data)
                process_data.append(good_df)
            else:
                good_df = pd.DataFrame()

        logger.info("Data validation finished")

        # Avoiding concatenating empty dataframes and warnings from Pandas
        process_data_filtered = [
            df for df in process_data if not df.empty and not df.isna().all().all()
        ]
        if process_data_filtered:
            validate_data = pd.concat(process_data_filtered)
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

        Parameters:
        ----------
        validate_data : pd.DataFrame
            DataFrame containing validated station data.
        load_path : str
            Path to the folder where the Parquet file will be saved.
        file_name : str
            Name of the output file without the extension.

        Raises:
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
                index=False,
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
