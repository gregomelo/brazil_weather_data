"""Fixture to tools module."""

import os
import zipfile
from tempfile import TemporaryDirectory

import pytest


@pytest.fixture(scope="module")
def tools_temp_folder():
    """Create a temporary folder to run the tests."""
    with TemporaryDirectory() as test_tools:
        # Creating the zip file to test extract_zip
        test_zip_file = "test_file.zip"
        zip_file_path = os.path.join(test_tools, test_zip_file)
        with zipfile.ZipFile(zip_file_path, "w") as zip_file:
            zip_file.writestr("test.txt", "This is a test file")

        yield test_tools
