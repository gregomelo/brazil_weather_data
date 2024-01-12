"""General shared utilities.

This module provides utility functions for common tasks such as
downloading files from the internet and extract zip files.
"""

import os
import zipfile

import httpx


def download_file(
    url: str,
    file_name: str,
    save_path: str,
    chunk_size: int = 128,
) -> None:
    """Download a file from a given URL and save it to a specified path.

    Parameters
    ----------
    url : str
        The URL of the file to be downloaded.
    file_name : str
        The name of the file as it will be saved locally.
    save_path : str
        The local directory to save the downloaded file.
    chunk_size : int, optional
        The size of chunks to download at a time, by default 128 bytes.

    Returns
    -------
    None
        This function does not return anything. It writes the downloaded
        file to the specified location.

    Raises
    ------
    Exception
        If any error occurs during the download or file writing process.
    """
    # Ensure the save directory exists
    os.makedirs(save_path, exist_ok=True)

    # Construct the full file path
    file_path = os.path.join(save_path, file_name)

    # Attempt to download the file
    try:
        response = httpx.get(f"{url}{file_name}", follow_redirects=True)
        with open(file_path, "wb") as file:
            for chunk in response.iter_bytes(chunk_size=chunk_size):
                file.write(chunk)
    except Exception as e:
        print(f"Error during download: {e}")
    finally:
        # Ensure the response is closed properly
        response.close()


def extract_zip(zip_path: str, zip_file: str, extract_to: str) -> None:
    """Extract a zip file to the specified directory.

    Parameters
    ----------
    zip_path : str
        Path to the zip file.
    zip_file : str
        Name of zip file.
    extract_to : str
        Directory where files will be extracted.
    """
    # Ensure the save directory exists
    os.makedirs(extract_to, exist_ok=True)

    file_path = os.path.join(zip_path, zip_file)

    with zipfile.ZipFile(file_path, "r") as zip_ref:
        zip_ref.extractall(extract_to)


if __name__ == "__main__":
    # Constants for the INMET data download
    INMET_URL = "https://portal.inmet.gov.br/uploads/dadoshistoricos/"
    FILE_NAME = "2023.zip"
    SAVE_PATH = "data/input"

    # Download the specified file
    download_file(INMET_URL, FILE_NAME, SAVE_PATH)

    # Constants for downloaded file
    STAGE_PATH = "data/stage"
    extract_zip(SAVE_PATH, FILE_NAME, STAGE_PATH)
