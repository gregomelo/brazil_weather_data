"""Data Collectors.

This module provides full data pipeline.
"""

import argparse
from typing import Dict, List

from .collectors import StationDataCollector, collect_years_list
from .general import clear_folder, download_file, extract_zip
from .validators import StationData


def run_pipeline(
    years_to_process: List[int],
    inmet_url: str,
    save_path: str,
    stage_path: str,
    output_path: str,
    stations_file: str,
    stations_column_names: Dict[str, str],
    schema,
) -> None:
    """
    Execute the data collection pipeline for specified years.

    This function manages the workflow of downloading, extracting, processing,
    and saving weather station data. It iterates over each year provided,
    downloading and processing the relevant data files.

    Parameters
    ----------
    years_to_process : List[int]
        A list of years to process.
    inmet_url : str
        The base URL from which the data files are downloaded.
    save_path : str
        The local folder where downloaded zip files are saved.
    stage_path : str
        The path to the folder where extracted data is temporarily stored.
    output_path : str
        The path to the folder where processed data files are stored.
    stations_file : str
        The name of the output file for processed data.
    stations_column_names : Dict[str, str]
        Mapping of original column names to desired column names.
    schema
        The Pydantic schema used for data validation.

    Returns
    -------
    None
        This function does not return anything. It processes and saves data
        to the specified locations.

    Raises
    ------
    Exception
        If any step in the process (downloading, extracting, processing,
        saving) fails, an exception is raised considering the functions
        applied.
    """
    years = collect_years_list(years_to_process)

    print(f"Start processing the years {years}...")

    for year in years:
        file_name = str(year) + ".zip"

        print(f"Beginning download data from {year}...")
        download_file(inmet_url, file_name, save_path)
        print(f"Beginning unzip data from {year}...")
        extract_zip(save_path, file_name, stage_path)
        print(f"Finishing process {year}.\n")

    print(f"Start processing all data {years}...")

    stations_data = StationDataCollector(
        stage_path,
        output_path,
        stations_file,
        stations_column_names,
        schema,
    )

    stations_data.start()

    print("Start cleaning temp folders...")

    clear_folder(save_path)
    clear_folder(stage_path)

    print("Finish the pipeline.")


# Pipeline Parameters
INMET_URL = "https://portal.inmet.gov.br/uploads/dadoshistoricos/"
SAVE_PATH = "data/input"
STAGE_PATH = "data/stage"
OUTPUT_PATH = "data/output"
STATIONS_FILE = "stations"
STATION_COLUMN_NAMES = {
    "REGIAO:": "Region",
    "UF:": "State",
    "ESTACAO:": "StationName",
    "CODIGO (WMO):": "IdStationWho",
    "LATITUDE:": "Latitude",
    "LONGITUDE:": "Longitude",
    "ALTITUDE:": "Altitude",
    "DATA DE FUNDACAO:": "FoundingDate",
}

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Run the data collection pipeline for specified years.",
    )
    parser.add_argument(
        "years_list",
        nargs="+",
        type=int,
        help="List of years to process.",
    )
    args = parser.parse_args()

    run_pipeline(
        args.years_list,
        INMET_URL,
        SAVE_PATH,
        STAGE_PATH,
        OUTPUT_PATH,
        STATIONS_FILE,
        STATION_COLUMN_NAMES,
        StationData,
    )
