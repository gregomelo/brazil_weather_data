"""General shared utilities.

This module provides utility functions for common tasks such as
downloading files from the internet and extract zip files.
"""

import os
import shutil
import zipfile

import httpx


def download_file(
    url: str,
    file_name: str,
    save_path: str,
    chunk_size: int = 128,
) -> None:
    """
    Download a file from a given URL and save it to a specified path.

    Parameters
    ----------
    url : str
        The URL of the file to be downloaded.
    file_name : str
        The name of the file to be saved locally.
    save_path : str
        The local folder where the downloaded file will be saved.
    chunk_size : int, optional
        The size of chunks for downloading the file, defaults to 128 bytes.

    Returns
    -------
    None
        The function writes the downloaded file to the specified location.

    Raises
    ------
    Exception
        If an error occurs during the download or file writing process.
    """
    os.makedirs(save_path, exist_ok=True)
    file_path = os.path.join(save_path, file_name)

    try:
        response = httpx.get(f"{url}{file_name}", follow_redirects=True)
        with open(file_path, "wb") as file:
            for chunk in response.iter_bytes(chunk_size=chunk_size):
                file.write(chunk)
    except Exception as e:
        print(f"Error during download: {e}")
    finally:
        if response:
            response.close()


def extract_zip(
    zip_path: str,
    zip_file: str,
    extract_to: str,
) -> None:
    """
    Extract the contents of a zip file to the specified directory.

    Parameters
    ----------
    zip_path : str
        The path to the zip file.
    zip_file : str
        The name of the zip file.
    extract_to : str
        The directory where the contents of the zip file will be extracted.
    """
    os.makedirs(extract_to, exist_ok=True)
    file_path = os.path.join(zip_path, zip_file)

    with zipfile.ZipFile(file_path, "r") as zip_ref:
        zip_ref.extractall(extract_to)


def clear_folder(
    folder_path: str,
) -> None:
    """Remove all files and subdirectories within a specified folder.

    This function deletes all contents within the specified folder, including
    files and subdirectories, while keeping the folder itself intact.

    Parameters
    ----------
    folder_path : str
        The path to the folder to be cleared.

    Raises
    ------
    FileNotFoundError
        If the specified folder does not exist.
    """
    if not os.path.exists(folder_path):
        raise FileNotFoundError(f"The folder {folder_path} does not exist.")

    for item in os.listdir(folder_path):
        item_path = os.path.join(folder_path, item)
        if os.path.isfile(item_path) or os.path.islink(item_path):
            os.unlink(item_path)
        elif os.path.isdir(item_path):
            shutil.rmtree(item_path)
