FROM python:3.11.11

RUN pip install poetry

COPY . /src

WORKDIR /src

RUN poetry install --without dev --no-root

EXPOSE 8000 8001

ENTRYPOINT [ "sh", "-c", "poetry run task run & poetry run task docs & wait" ]
