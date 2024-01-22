import os
import zipfile
from tempfile import TemporaryDirectory
from unittest.mock import MagicMock, patch

import pytest

from app.tools.general import clear_folder, download_file, extract_zip


@pytest.fixture(scope="module")
def tools_temp_folder():
    """Create a temporary folder to run the tests."""
    with TemporaryDirectory() as test_tools:
        # Creating the zip file to test extract_zip
        test_zip_file = "file.zip"
        zip_file_path = os.path.join(test_tools, test_zip_file)
        with zipfile.ZipFile(zip_file_path, "w") as zip_file:
            zip_file.writestr("test.txt", "This is a test file")

        yield test_tools


def test_download_file(tools_temp_folder):
    """
    Test the download_file function to ensure it downloads and saves a file.

    This test simulates a file download using a mocked HTTP response and
    verifies if the file is correctly downloaded and saved to the specified
    path.

    Parameters:
    tools_temp_folder (str): The path to a temporary folder provided by a
    fixture.

    Asserts:
    The file exists at the specified path and contains the expected content.
    """
    # Define test parameters
    test_url = "http://example.com/data/"
    test_file_name = "test_file.zip"
    test_save_path = tools_temp_folder
    fake_content = b"Test content"

    # Mock the HTTP response
    mock_response = MagicMock()
    mock_response.iter_bytes = lambda chunk_size: [fake_content]

    # Path where the file should be saved
    test_file_path = os.path.join(test_save_path, test_file_name)

    # Mock the httpx.get function
    with patch("httpx.get", return_value=mock_response):
        download_file(test_url, test_file_name, test_save_path)

        # Check if the file was downloaded and saved
        assert os.path.exists(test_file_path)
        with open(test_file_path, "rb") as file:
            content = file.read()
            # Verify the content of the downloaded file
            assert content == fake_content


def test_extract_zip(tools_temp_folder):
    """
    Test the extract_zip function to ensure it extracts files from a zip
    archive.

    This test checks if the extract_zip function correctly extracts all files
    from a provided zip archive into a specified directory.

    Parameters:
    tools_temp_folder (str): The path to a temporary folder provided by a
    fixture.

    Asserts:
    All files from the zip archive are extracted to the target directory.
    """
    # Define test parameters
    test_zip_path = tools_temp_folder
    test_zip_file = "file.zip"

    extract_to = os.path.join(test_zip_path, "extracted_data")

    extract_zip(test_zip_path, test_zip_file, extract_to)

    expected_file_path = os.path.join(extract_to, "test.txt")

    # Count extracted files
    files_extracted = len(
        [
            name
            for name in os.listdir(extract_to)
            if os.path.isfile(os.path.join(extract_to, name))
        ],
    )

    # Verify the extracted file exists
    assert os.path.exists(expected_file_path)

    # Verify only one file was extracted
    assert files_extracted == 1


def test_clear_folder(tools_temp_folder):
    """
    Test the clear_folder function to ensure it remove files from a folder.

    This test checks if the clear_folder function correctly extracts all files
    from a provided folder.

    Parameters:
    tools_temp_folder (str): The path to a temporary folder provided by a
    fixture.

    Asserts:
    All files from the folder archive are removed from a target directory.

    Note:
    Considering that FileNotFoundError will not be tested considering that this
    is a build-in function.
    """
    # Define test parameters
    test_zip_path = tools_temp_folder

    extract_to = os.path.join(test_zip_path, "extracted_data")

    clear_folder(extract_to)

    # Count extracted files
    files_extracted = len(
        [
            name
            for name in os.listdir(extract_to)
            if os.path.isfile(os.path.join(extract_to, name))
        ],
    )

    # Verify only one file was extracted
    assert files_extracted == 0