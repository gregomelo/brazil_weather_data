import os
import zipfile
from tempfile import TemporaryDirectory
from unittest.mock import MagicMock, patch

import pytest

from app.tools.general import clear_folder, download_file, extract_zip


@pytest.fixture(scope="module")
def tools_temp_folder():
    """Provide a temporary folder for running tests.

    Yields
    ------
    str
        The path to the temporary folder created for the duration of the
        module's tests.
    """
    with TemporaryDirectory() as test_tools:
        test_zip_file = "file.zip"
        zip_file_path = os.path.join(test_tools, test_zip_file)
        with zipfile.ZipFile(zip_file_path, "w") as zip_file:
            zip_file.writestr("test.txt", "This is a test file")

        yield test_tools


def test_download_file(tools_temp_folder):
    """Verify that the download_file function downloads and saves a
    file correctly.

    Parameters
    ----------
    tools_temp_folder : str
        Path to a temporary folder provided by the fixture.

    Asserts
    -------
    The file is saved to the specified path with the correct content.
    """

    test_url = "http://example.com/data/"
    test_file_name = "test_file.zip"
    test_save_path = tools_temp_folder
    fake_content = b"Test content"

    mock_response = MagicMock()
    mock_response.iter_bytes = lambda chunk_size: [fake_content]

    test_file_path = os.path.join(test_save_path, test_file_name)

    with patch("httpx.get", return_value=mock_response):
        download_file(test_url, test_file_name, test_save_path)

        assert os.path.exists(test_file_path)
        with open(test_file_path, "rb") as file:
            content = file.read()
            assert content == fake_content


def test_extract_zip(tools_temp_folder):
    """Verify that the extract_zip function correctly extracts files
    from a zip archive.

    Parameters
    ----------
    tools_temp_folder : str
        Path to a temporary folder provided by the fixture.

    Asserts
    -------
    The zip archive is extracted to the target directory with all files.
    """
    test_zip_path = tools_temp_folder
    test_zip_file = "file.zip"

    extract_to = os.path.join(test_zip_path, "extracted_data")

    extract_zip(test_zip_path, test_zip_file, extract_to)

    expected_file_path = os.path.join(extract_to, "test.txt")

    files_extracted = len(
        [
            name
            for name in os.listdir(extract_to)
            if os.path.isfile(os.path.join(extract_to, name))
        ],
    )

    assert os.path.exists(expected_file_path)

    assert files_extracted == 1


def test_clear_folder(tools_temp_folder):
    """Confirm that the clear_folder function removes all files and
    subdirectories from a folder.

    Parameters
    ----------
    tools_temp_folder : str
        Path to a temporary folder provided by the fixture.

    Asserts
    -------
    The folder is empty after the clear_folder function is executed.
    """
    test_zip_path = tools_temp_folder

    extract_to = os.path.join(test_zip_path, "extracted_data")

    clear_folder(extract_to)

    files_extracted = len(
        [
            name
            for name in os.listdir(extract_to)
            if os.path.isfile(os.path.join(extract_to, name))
        ],
    )

    assert files_extracted == 0
