# Welcome to Brazilian Weather Data

The Brazil Weather Data project is a purely educational initiative aimed at transforming the data from Brazilian automatic weather stations into a lightweight API. This project serves as an excellent resource for learning and experimentation with API development, data handling, and modern software engineering practices.

This API is hosted in a free tier webservice on [Render](https://brazil-weather-data.onrender.com/). Due the resource avaliable, the data is ingested in a local machine and only upload to the repository.

## Project layout

    app/
        main.py                 # Contains the routes for the API
        tools/                  # Tools for data loading and manipulation
        general                 # General utility tools
        validators              # Tools for ensuring data quality and integrity
        collectors              # Modules for data collection
        pipeline                # The complete pipeline for loading data into the database
    tests/
        test_api                # Tests for API routes
        tools/                  # Tests for various utility tools
        test_general            # Tests for general utility tools
        test_validators         # Tests for data validation tools
        test_collectors         # Tests for data collection modules
