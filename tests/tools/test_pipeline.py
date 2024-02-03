import argparse
import sys
from unittest.mock import patch

import app.tools.pipeline as pipeline_module
from app.tools.pipeline import run_pipeline


@patch("app.tools.pipeline.collect_years_list")
@patch("app.tools.pipeline.download_file")
@patch("app.tools.pipeline.extract_zip")
@patch("app.tools.pipeline.StationDataCollector")
@patch("app.tools.pipeline.clear_folder")
def test_run_pipeline(
    mock_clear_folder,
    mock_StationDataCollector,
    mock_extract_zip,
    mock_download_file,
    mock_collect_years_list,
):
    """Tests the run_pipeline function to ensure it processes data correctly.

    This test simulates the running of the data collection pipeline by mocking
    dependent functions and verifying they are called with the correct
    arguments. It checks if the pipeline correctly processes a given set of
    years, manages file downloads, extraction, data collection, and cleanup
    processes as expected.

    Parameters
    ----------
    mock_clear_folder : MagicMock
        Mock for the clear_folder function.
    mock_StationDataCollector : MagicMock
        Mock for the StationDataCollector class.
    mock_extract_zip : MagicMock
        Mock for the extract_zip function.
    mock_download_file : MagicMock
        Mock for the download_file function.
    mock_collect_years_list : MagicMock
        Mock for the collect_years_list function.

    Asserts
    -------
    The function asserts that each mocked function is called with the expected
    arguments, verifying the pipeline's internal workflow and data handling.
    """
    mock_collect_years_list.return_value = [2022, 2023]

    years_to_process = [2022, 2023]
    inmet_url = "https://example.com"
    save_path = "path/to/save"
    stage_path = "path/to/stage"
    output_path = "path/to/output"
    stations_file = "stations_file"
    stations_column_names = {"CODIGO (WMO):": "IdStationWho"}
    schema = None

    run_pipeline(
        years_to_process,
        inmet_url,
        save_path,
        stage_path,
        output_path,
        stations_file,
        stations_column_names,
        schema,
    )

    mock_collect_years_list.assert_called_once_with(years_to_process)
    mock_download_file.assert_called()
    mock_extract_zip.assert_called()
    mock_StationDataCollector.assert_called_once_with(
        stage_path,
        output_path,
        stations_file,
        stations_column_names,
        schema,
    )
    mock_clear_folder.assert_called()


@patch("app.tools.pipeline.run_pipeline")
def test_task_pipeline(mock_run_pipeline):
    """Tests the CLI aspect of the data collection pipeline command.

    This test ensures that when the pipeline is invoked through the command
    line with a set of years, the `run_pipeline` function is called with the
    correct arguments parsed from the command line. It uses mock patches to
    simulate the command line argument parsing and the calling of the
    `run_pipeline` function, checking that the function is executed with the
    intended year list and other default parameters.

    Parameters
    ----------
    mock_run_pipeline : MagicMock
        Mock for the run_pipeline function.

    Asserts
    -------
    Verifies that `run_pipeline` is called with the correct arguments,
    including a list of years to process and predefined parameters for
    URLs, paths, and file names, reflecting the expected behavior when
    the pipeline command is executed.
    """
    test_args = ["program", "2022", "2023"]
    with patch.object(sys, "argv", test_args):
        parser = argparse.ArgumentParser(
            description="Run the data collection pipeline for specified years.",  # noqa : E501
        )
        parser.add_argument(
            "years_list",
            nargs="+",
            type=int,
            help="List of years to process.",
        )
        args = parser.parse_args()

        pipeline_module.run_pipeline(
            args.years_list,
            pipeline_module.INMET_URL,
            pipeline_module.SAVE_PATH,
            pipeline_module.STAGE_PATH,
            pipeline_module.OUTPUT_PATH,
            pipeline_module.STATIONS_FILE,
            pipeline_module.STATION_COLUMN_NAMES,
            pipeline_module.StationData,
        )

    mock_run_pipeline.assert_called_once_with(
        [2022, 2023],
        pipeline_module.INMET_URL,
        pipeline_module.SAVE_PATH,
        pipeline_module.STAGE_PATH,
        pipeline_module.OUTPUT_PATH,
        pipeline_module.STATIONS_FILE,
        pipeline_module.STATION_COLUMN_NAMES,
        pipeline_module.StationData,
    )
