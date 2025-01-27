
# Brazil Weather Data API

[Ler em Português](./README_PT_BR.md)

## Introduction
The Brazil Weather Data project is a purely educational initiative aimed at transforming the data from Brazilian automatic weather stations into a lightweight API. This project serves as an excellent resource for learning and experimentation with API development, data handling, and modern software engineering practices.

This API is hosted in a free tier webservice on [Render](https://brazil-weather-data.onrender.com/). Due the resource avaliable, the data is ingested in a local machine and only upload to the repository.

You can access the documentation at [GitHub](https://gregomelo.github.io/brazil_weather_data/).

## Project Objective
The primary goal of this project is to provide easy and structured access to meteorological data from Brazilian weather stations. The API offers three main endpoints:
- `stations_data`: Provides data about the weather stations, including geographical location and date of implementation.
- `weather_data`: Offers meteorological data collected from the stations.
- `query`: Allows users to simulate SQL queries for customized data retrieval.

## Data Source
The data is sourced from the Brazilian National Institute of Meteorology (INMET - [Instituto Nacional de Meteorologia](https://portal.inmet.gov.br/)), ensuring reliability and comprehensiveness.

## Refactoring and Technical Stack
This project is a refactored version of a [Kaggle notebook](https://www.kaggle.com/code/gregoryoliveira/brazil-weather-change-part-i-data-collection) focused on collecting and analyzing Brazilian weather data. The technical stack for this project includes:
- pandas for data wrangling.
- FastAPI for building the API.
- DuckDB as the database management system.
- pytest for running tests.
- Pre-commit and Commitizen to ensure code quality and standardized commit messages.
- Continuous Integration and Continuous Deployment (CI/CD) using GitHub Actions.

## Database and Scalability
Initially, the database for this project was populated locally due to resource constraints. This approach ensured a solid foundation for our initial data analysis and API functionality. However, in a professional setting, the system is designed to be scalable and automated.

Ideally, the data collection pipeline would be set up to run automatically on a monthly basis. This would allow the database to be continuously updated with the latest weather data. Such a setup would not only provide real-time insights but also enrich the database over time, enhancing the depth and accuracy of our analyses.

This automated approach, combined with a more robust hosting solution, would make the project more dynamic and valuable for ongoing weather data analysis and research.

## Installation
### Pyenv and Poetry
To get started with the Brazil Weather Data API, follow these steps:
1. Ensure you have [pyenv](https://github.com/pyenv/pyenv) and [Poetry](https://python-poetry.org/) installed on your system for dependency management.

2. Ensure you have the python version 3.11.11 avaiable in your system using the command `pyenv versions`. If 3.11.11 is not listed, use the command `pyenv install 3.11.11`.

3. Clone the repository from GitHub:
   ```bash
   git clone https://github.com/gregomelo/brazil_weather_data.git
   ```

4. Navigate to the cloned directory and install the dependencies using Poetry:
   ```bash
   cd brazil_weather_data
   pyenv local 3.11.11
   poetry env use 3.11.11
   poetry install --no-root
   poetry lock --no-update
   ```

### Docker
To get started with the Brazil Weather Data API, follow these steps:

1. Ensure you have (Docker)[https://www.docker.com/] installed and free space (around 3GB),

2. Clone the repository from GitHub:
   ```bash
   git clone https://github.com/gregomelo/brazil_weather_data.git
   ```

3. Run the command `docker build -t bwd-container .`.

4. After completer, run the command `docker run -d -p 8000:8000 -p 8001:8001 bwd-container`.

5. To use the API, open the address http://localhost:8000/. To read the documentation, open the address: http://localhost:8001/.

## Usage
### Starting the webservice API:
1. Run the following commands:
   ```bash
   poetry run task run
   ```
2. Open your favorite browser and navigate to [Brazil Weather Data API](http://127.0.0.1:8000/docs).

Note: if your are running other service on 8000 port, you need to edit the running port on `[tool.taskipy.tasks]` section at `pyproject.toml` file.

If you want to kill all other process on 8000 port, you can use the command:
   ```bash
   poetry run task killr
   ```

### Accessing local documentation:
1. Run the following commands:

```bash
poetry run task docs
```

2. Open your favorite browser and navigate to [Brazil Weather Data API Docs](http://127.0.0.1:8001).

Note: if your are running other service on 8001 port, you need to edit the running port on `[tool.taskipy.tasks]` section at `pyproject.toml` file.

If you want to kill all other process on 8001 port, you can use the command:
   ```bash
   poetry run task killd
   ```

### Running tests
Run the following commands:
   ```bash
   poetry run task test
   ```

### Running the data pipeline
Run the following commands:
   ```bash
   poetry run task pipeline -- list_years
   ```

For example, to run the pipeline only for 2023:
   ```bash
   poetry run task pipeline -- 2023
   ```

For more than one year, digit years with a simple-space between then:
   ```bash
   poetry run task pipeline -- 2023 2022 2021
   ```

## Contribution
As an educational project, contributions are highly encouraged. Whether you're looking to fix bugs, add features, or improve documentation, your input is welcome. Please follow the standard GitHub flow for contributions.

## License
This project is open-sourced under the [MIT license](https://opensource.org/licenses/MIT).

---
Enjoy exploring and utilizing the Brazil Weather Data API! 🌦️🇧🇷

---
