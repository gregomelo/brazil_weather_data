"""Data Collectors.

This module provides full data pipeline.
"""

import argparse
from typing import Dict, List

from loguru import logger

from .collectors import (  # noqa
    StationDataCollector,
    WeatherDataCollector,
    collect_years_list,
)
from .general import clear_folder, download_file, extract_zip
from .logger_config import configure_logger, logger_decorator
from .validators import StationData, WeatherData


@logger_decorator
def run_pipeline(
    years_to_process: List[int],
    inmet_url: str,
    save_path: str,
    stage_path: str,
    output_path: str,
    stations_file: str,
    weather_file: str,
    stations_column_names: Dict[str, str],
    weather_column_names: Dict[str, str],
    stations_schema,
    weather_schema,
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
    logger.info(f"Start extraction data from years {years}")

    for year in years:
        file_name = str(year) + ".zip"

        logger.info(f"Beginning download data from {year}")
        download_file(inmet_url, file_name, save_path)

        logger.info(f"Beginning unzip data from {year}")
        extract_zip(save_path, file_name, stage_path)

        logger.info(f"Finishing process {year}")

    logger.info("Starting processing stations data")
    stations_data = StationDataCollector(
        stage_path,
        output_path,
        stations_file,
        stations_column_names,
        stations_schema,
    )
    stations_data.start()

    logger.info("Starting processing weather data")
    weather_data = WeatherDataCollector(
        stage_path,
        output_path,
        weather_file,
        weather_column_names,
        weather_schema,
    )
    weather_data.start()

    logger.info("Cleaning temp folders")
    clear_folder(save_path)
    clear_folder(stage_path)

    logger.info("Finish the pipeline!")


# Pipeline Parameters
INMET_URL = "https://portal.inmet.gov.br/uploads/dadoshistoricos/"
SAVE_PATH = "data/input"
STAGE_PATH = "data/stage"
OUTPUT_PATH = "data/output"
STATIONS_FILE = "stations"
WEATHER_FILE = "weather"
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
WEATHER_COLUMN_NAMES = {
    "Data": "Date",
    "Hora UTC": "Time",
    "PRECIPITAÇÃO TOTAL, HORÁRIO (mm)": "TotalPrecipitation",
    "PRESSAO ATMOSFERICA AO NIVEL DA ESTACAO, HORARIA (mB)": "AtmosphericPressure",  # noqa
    "PRESSÃO ATMOSFERICA MAX.NA HORA ANT. (AUT) (mB)": "MaxAtmosphericPressure",  # noqa
    "PRESSÃO ATMOSFERICA MIN. NA HORA ANT. (AUT) (mB)": "MinAtmosphericPressure",  # noqa
    "RADIACAO GLOBAL (Kj/m²)": "GlobalRadiation",
    "TEMPERATURA DO AR - BULBO SECO, HORARIA (°C)": "DryBulbTemperature",
    "TEMPERATURA DO PONTO DE ORVALHO (°C)": "DewPointTemperature",
    "TEMPERATURA MÁXIMA NA HORA ANT. (AUT) (°C)": "MaxTemperature",
    "TEMPERATURA MÍNIMA NA HORA ANT. (AUT) (°C)": "MinTemperature",
    "TEMPERATURA ORVALHO MAX. NA HORA ANT. (AUT) (°C)": "MaxDewPointTemperature",  # noqa
    "TEMPERATURA ORVALHO MIN. NA HORA ANT. (AUT) (°C)": "MinDewPointTemperature",  # noqa
    "UMIDADE REL. MAX. NA HORA ANT. (AUT) (%)": "MaxRelativeHumidity",
    "UMIDADE REL. MIN. NA HORA ANT. (AUT) (%)": "MinRelativeHumidity",
    "UMIDADE RELATIVA DO AR, HORARIA (%)": "RelativeHumidity",
    "VENTO, DIREÇÃO HORARIA (gr) (° (gr))": "WindDirection",
    "VENTO, RAJADA MAXIMA (m/s)": "MaxWindGust",
    "VENTO, VELOCIDADE HORARIA (m/s)": "WindSpeed",
}

if __name__ == "__main__":
    configure_logger()

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
        WEATHER_FILE,
        STATION_COLUMN_NAMES,
        WEATHER_COLUMN_NAMES,
        StationData,
        WeatherData,
    )
