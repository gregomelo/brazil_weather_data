# flake8: noqa:E501
import os
import pathlib
from typing import List

import pytest

# fmt: off
from app.tools.collectors import StationDataCollector, collect_years_list, limit_years
from app.tools.pipeline import STATION_COLUMN_NAMES, STATIONS_FILE
from app.tools.validators import StationData


# fmt: on
def create_files(
    destiny_folder: pathlib.Path,
    data_to_files: List[str],
) -> None:
    """Create CSV files from a list of CSV content strings in a specified
    folder.

    Parameters
    ----------
    destiny_folder : pathlib.Path
        Destination folder where CSV files will be created.
    data_to_files : List[str]
        List of strings, each representing the content of a CSV file.
    """
    for file_number, csv_data in enumerate(data_to_files):
        file_name = "file_" + str(file_number) + ".csv"
        file = destiny_folder / file_name
        file.write_text(csv_data)


@pytest.fixture()
def tmp_station_valid_data(tmp_path):
    """Create a temporary directory with valid station data for testing.

    Parameters
    ----------
    tmp_path : py.path.local
        Fixture provided by pytest to create and return temporary directories.

    Yields
    ------
    tuple
        A tuple containing the path to the valid staging data and the output
        directory.
    """
    temp_stage_valid = tmp_path / "stage-valid"
    temp_stage_valid.mkdir()
    temp_output_valid = tmp_path / "output-valid"
    temp_output_valid.mkdir()
    csv_data_infos = [
        """REGIAO:;CO
UF:;DF
ESTACAO:;BRASILIA
CODIGO (WMO):;A001
LATITUDE:;-15,78944444
LONGITUDE:;-47,92583332
ALTITUDE:;1160,96
DATA DE FUNDACAO:;07/05/00
Data;Hora UTC;PRECIPITAÇÃO TOTAL, HORÁRIO (mm);PRESSAO ATMOSFERICA AO NIVEL DA ESTACAO, HORARIA (mB);PRESSÃO ATMOSFERICA MAX.NA HORA ANT. (AUT) (mB);PRESSÃO ATMOSFERICA MIN. NA HORA ANT. (AUT) (mB);RADIACAO GLOBAL (Kj/m²);TEMPERATURA DO AR - BULBO SECO, HORARIA (°C);TEMPERATURA DO PONTO DE ORVALHO (°C);TEMPERATURA MÁXIMA NA HORA ANT. (AUT) (°C);TEMPERATURA MÍNIMA NA HORA ANT. (AUT) (°C);TEMPERATURA ORVALHO MAX. NA HORA ANT. (AUT) (°C);TEMPERATURA ORVALHO MIN. NA HORA ANT. (AUT) (°C);UMIDADE REL. MAX. NA HORA ANT. (AUT) (%);UMIDADE REL. MIN. NA HORA ANT. (AUT) (%);UMIDADE RELATIVA DO AR, HORARIA (%);VENTO, DIREÇÃO HORARIA (gr) (° (gr));VENTO, RAJADA MAXIMA (m/s);VENTO, VELOCIDADE HORARIA (m/s);
2023/01/01;0000 UTC;0;887,7;887,7;887,2;;20,1;17,9;20,9;20;19,2;17,8;91;87;87;187;3,3;1,2;
2023/01/01;0100 UTC;0;888,1;888,1;887,7;;19,2;17,5;20,1;19,2;17,8;17,4;90;87;90;153;2,9;,8;
2023/01/01;0200 UTC;0;887,8;888,1;887,8;;19,3;17,6;19,5;19;17,8;17,3;90;89;90;145;2,5;1,5;
2023/01/01;0300 UTC;0;887,8;887,9;887,7;;19,3;17,7;19,4;19,1;17,8;17,5;91;90;91;162;3,2;1,4;
2023/01/01;0400 UTC;0;887,6;887,9;887,6;;19,7;18,1;19,7;19,1;18,1;17,4;91;90;90;140;5,7;2,7;
2023/01/01;0500 UTC;0;886,7;887,6;886,7;;19,1;17,7;19,7;19,1;18,1;17,7;92;90;92;128;7,1;2;""",
        """REGIAO:;CO
UF:;DF
ESTACAO:;BRAZLANDIA
CODIGO (WMO):;A042
LATITUDE:;-15,59972221
LONGITUDE:;-48,1311111
ALTITUDE:;1143
DATA DE FUNDACAO:;19/07/17
Data;Hora UTC;PRECIPITAÇÃO TOTAL, HORÁRIO (mm);PRESSAO ATMOSFERICA AO NIVEL DA ESTACAO, HORARIA (mB);PRESSÃO ATMOSFERICA MAX.NA HORA ANT. (AUT) (mB);PRESSÃO ATMOSFERICA MIN. NA HORA ANT. (AUT) (mB);RADIACAO GLOBAL (Kj/m²);TEMPERATURA DO AR - BULBO SECO, HORARIA (°C);TEMPERATURA DO PONTO DE ORVALHO (°C);TEMPERATURA MÁXIMA NA HORA ANT. (AUT) (°C);TEMPERATURA MÍNIMA NA HORA ANT. (AUT) (°C);TEMPERATURA ORVALHO MAX. NA HORA ANT. (AUT) (°C);TEMPERATURA ORVALHO MIN. NA HORA ANT. (AUT) (°C);UMIDADE REL. MAX. NA HORA ANT. (AUT) (%);UMIDADE REL. MIN. NA HORA ANT. (AUT) (%);UMIDADE RELATIVA DO AR, HORARIA (%);VENTO, DIREÇÃO HORARIA (gr) (° (gr));VENTO, RAJADA MAXIMA (m/s);VENTO, VELOCIDADE HORARIA (m/s);
2023/01/01;0000 UTC;0;888,7;888,9;888,2;;20,5;18;21;20,5;18,4;18;87;84;86;146;3,9;1,8;
2023/01/01;0100 UTC;0;889;889;888,7;;20,4;17,5;20,5;20,3;18;17,5;86;84;84;142;4,1;1,8;
2023/01/01;0200 UTC;0;888,9;889,1;888,9;;19,9;17,4;20,4;19,8;17,5;17,3;86;83;86;153;4;,2;
2023/01/01;0300 UTC;0;888,7;888,9;888,7;;19,8;17,4;19,9;19,7;17,4;17,2;87;85;86;140;3,5;1,2;
2023/01/01;0400 UTC;0;888,4;888,8;888,4;;20,1;17,3;20,1;19,8;17,5;17,3;86;84;84;139;3,3;,3;
2023/01/01;0500 UTC;0;887,8;888,4;887,8;;19,8;17,6;20,3;19,8;17,6;17,2;87;83;87;138;5,2;1,3;
2023/01/01;0600 UTC;0;887,5;887,9;887,5;;19,6;17,3;19,8;19,5;17,7;17,3;89;87;87;133;4,1;,3;
2023/01/01;0700 UTC;0;887,5;887,5;887,3;;19,1;17,3;19,6;19,1;17,3;17,2;89;87;89;95;4,4;1,4;
2023/01/01;0800 UTC;0;887,6;887,6;887,5;;18,9;17,1;19,2;18,3;17,5;16,9;92;89;89;49;5,5;2,4;
2023/01/01;0900 UTC;0;888;888;887,4;5,3;18,8;17;19,1;18,4;17,1;16,6;90;88;89;73;4,8;,6;""",
        """REGIAO:;N
UF:;AM
ESTACAO:;COARI
CODIGO (WMO):;A117
LATITUDE:;-4,09749999
LONGITUDE:;-63,14527777
ALTITUDE:;33,84
DATA DE FUNDACAO:;12/04/08
Data;Hora UTC;PRECIPITAÇÃO TOTAL, HORÁRIO (mm);PRESSAO ATMOSFERICA AO NIVEL DA ESTACAO, HORARIA (mB);PRESSÃO ATMOSFERICA MAX.NA HORA ANT. (AUT) (mB);PRESSÃO ATMOSFERICA MIN. NA HORA ANT. (AUT) (mB);RADIACAO GLOBAL (Kj/m²);TEMPERATURA DO AR - BULBO SECO, HORARIA (°C);TEMPERATURA DO PONTO DE ORVALHO (°C);TEMPERATURA MÁXIMA NA HORA ANT. (AUT) (°C);TEMPERATURA MÍNIMA NA HORA ANT. (AUT) (°C);TEMPERATURA ORVALHO MAX. NA HORA ANT. (AUT) (°C);TEMPERATURA ORVALHO MIN. NA HORA ANT. (AUT) (°C);UMIDADE REL. MAX. NA HORA ANT. (AUT) (%);UMIDADE REL. MIN. NA HORA ANT. (AUT) (%);UMIDADE RELATIVA DO AR, HORARIA (%);VENTO, DIREÇÃO HORARIA (gr) (° (gr));VENTO, RAJADA MAXIMA (m/s);VENTO, VELOCIDADE HORARIA (m/s);
2023/01/01;0000 UTC;;;;;;;;;;;;;;;;;;
2023/01/01;0100 UTC;;;;;;;;;;;;;;;;;;
2023/01/01;0200 UTC;;;;;;;;;;;;;;;;;;
2023/01/01;0300 UTC;;;;;;;;;;;;;;;;;;
2023/01/01;0400 UTC;;;;;;;;;;;;;;;;;;""",
    ]

    create_files(temp_stage_valid, csv_data_infos)

    yield temp_stage_valid, temp_output_valid


@pytest.fixture()
def tmp_station_invalid_data(tmp_path):
    """Create a temporary directory with invalid station data for testing.

    Parameters
    ----------
    tmp_path : py.path.local
        Fixture provided by pytest to create and return temporary directories.

    Yields
    ------
    tuple
        A tuple containing the path to the invalid staging data and the output directory.
    """
    temp_stage_invalid = tmp_path / "stage-invalid"
    temp_stage_invalid.mkdir()
    temp_output_invalid = tmp_path / "output-invalid"
    temp_output_invalid.mkdir()

    csv_data_infos = [
        """REGIAO:;CO
UF:;DF
ESTACAO:;BRASILIA
CODIGO (WMO):;A001
LATITUDE:;aaaaaaaaaa
LONGITUDE:;-47,92583332
ALTITUDE:;1160,96
DATA DE FUNDACAO:;07/05/00
Data;Hora UTC;PRECIPITAÇÃO TOTAL, HORÁRIO (mm);PRESSAO ATMOSFERICA AO NIVEL DA ESTACAO, HORARIA (mB);PRESSÃO ATMOSFERICA MAX.NA HORA ANT. (AUT) (mB);PRESSÃO ATMOSFERICA MIN. NA HORA ANT. (AUT) (mB);RADIACAO GLOBAL (Kj/m²);TEMPERATURA DO AR - BULBO SECO, HORARIA (°C);TEMPERATURA DO PONTO DE ORVALHO (°C);TEMPERATURA MÁXIMA NA HORA ANT. (AUT) (°C);TEMPERATURA MÍNIMA NA HORA ANT. (AUT) (°C);TEMPERATURA ORVALHO MAX. NA HORA ANT. (AUT) (°C);TEMPERATURA ORVALHO MIN. NA HORA ANT. (AUT) (°C);UMIDADE REL. MAX. NA HORA ANT. (AUT) (%);UMIDADE REL. MIN. NA HORA ANT. (AUT) (%);UMIDADE RELATIVA DO AR, HORARIA (%);VENTO, DIREÇÃO HORARIA (gr) (° (gr));VENTO, RAJADA MAXIMA (m/s);VENTO, VELOCIDADE HORARIA (m/s);
2023/01/01;0000 UTC;0;887,7;887,7;887,2;;20,1;17,9;20,9;20;19,2;17,8;91;87;87;187;3,3;1,2;
2023/01/01;0100 UTC;0;888,1;888,1;887,7;;19,2;17,5;20,1;19,2;17,8;17,4;90;87;90;153;2,9;,8;
2023/01/01;0200 UTC;0;887,8;888,1;887,8;;19,3;17,6;19,5;19;17,8;17,3;90;89;90;145;2,5;1,5;
2023/01/01;0300 UTC;0;887,8;887,9;887,7;;19,3;17,7;19,4;19,1;17,8;17,5;91;90;91;162;3,2;1,4;
2023/01/01;0400 UTC;0;887,6;887,9;887,6;;19,7;18,1;19,7;19,1;18,1;17,4;91;90;90;140;5,7;2,7;
2023/01/01;0500 UTC;0;886,7;887,6;886,7;;19,1;17,7;19,7;19,1;18,1;17,7;92;90;92;128;7,1;2;""",
        """REGIAO:;CO
UF:;DF
ESTACAO:;BRAZLANDIA
CODIGO (WMO):;A042
LATITUDE:;-15,59972221
LONGITUDE:;aaaaaaa
ALTITUDE:;1143
DATA DE FUNDACAO:;19/07/17
Data;Hora UTC;PRECIPITAÇÃO TOTAL, HORÁRIO (mm);PRESSAO ATMOSFERICA AO NIVEL DA ESTACAO, HORARIA (mB);PRESSÃO ATMOSFERICA MAX.NA HORA ANT. (AUT) (mB);PRESSÃO ATMOSFERICA MIN. NA HORA ANT. (AUT) (mB);RADIACAO GLOBAL (Kj/m²);TEMPERATURA DO AR - BULBO SECO, HORARIA (°C);TEMPERATURA DO PONTO DE ORVALHO (°C);TEMPERATURA MÁXIMA NA HORA ANT. (AUT) (°C);TEMPERATURA MÍNIMA NA HORA ANT. (AUT) (°C);TEMPERATURA ORVALHO MAX. NA HORA ANT. (AUT) (°C);TEMPERATURA ORVALHO MIN. NA HORA ANT. (AUT) (°C);UMIDADE REL. MAX. NA HORA ANT. (AUT) (%);UMIDADE REL. MIN. NA HORA ANT. (AUT) (%);UMIDADE RELATIVA DO AR, HORARIA (%);VENTO, DIREÇÃO HORARIA (gr) (° (gr));VENTO, RAJADA MAXIMA (m/s);VENTO, VELOCIDADE HORARIA (m/s);
2023/01/01;0000 UTC;0;888,7;888,9;888,2;;20,5;18;21;20,5;18,4;18;87;84;86;146;3,9;1,8;
2023/01/01;0100 UTC;0;889;889;888,7;;20,4;17,5;20,5;20,3;18;17,5;86;84;84;142;4,1;1,8;
2023/01/01;0200 UTC;0;888,9;889,1;888,9;;19,9;17,4;20,4;19,8;17,5;17,3;86;83;86;153;4;,2;
2023/01/01;0300 UTC;0;888,7;888,9;888,7;;19,8;17,4;19,9;19,7;17,4;17,2;87;85;86;140;3,5;1,2;
2023/01/01;0400 UTC;0;888,4;888,8;888,4;;20,1;17,3;20,1;19,8;17,5;17,3;86;84;84;139;3,3;,3;
2023/01/01;0500 UTC;0;887,8;888,4;887,8;;19,8;17,6;20,3;19,8;17,6;17,2;87;83;87;138;5,2;1,3;
2023/01/01;0600 UTC;0;887,5;887,9;887,5;;19,6;17,3;19,8;19,5;17,7;17,3;89;87;87;133;4,1;,3;
2023/01/01;0700 UTC;0;887,5;887,5;887,3;;19,1;17,3;19,6;19,1;17,3;17,2;89;87;89;95;4,4;1,4;
2023/01/01;0800 UTC;0;887,6;887,6;887,5;;18,9;17,1;19,2;18,3;17,5;16,9;92;89;89;49;5,5;2,4;
2023/01/01;0900 UTC;0;888;888;887,4;5,3;18,8;17;19,1;18,4;17,1;16,6;90;88;89;73;4,8;,6;""",
        """REGIAO:;N
UF:;AM
ESTACAO:;COARI
CODIGO (WMO):;A117
LATITUDE:;-4,09749999
LONGITUDE:;aaaaaaaa
ALTITUDE:;33,84
DATA DE FUNDACAO:;12/04/08
Data;Hora UTC;PRECIPITAÇÃO TOTAL, HORÁRIO (mm);PRESSAO ATMOSFERICA AO NIVEL DA ESTACAO, HORARIA (mB);PRESSÃO ATMOSFERICA MAX.NA HORA ANT. (AUT) (mB);PRESSÃO ATMOSFERICA MIN. NA HORA ANT. (AUT) (mB);RADIACAO GLOBAL (Kj/m²);TEMPERATURA DO AR - BULBO SECO, HORARIA (°C);TEMPERATURA DO PONTO DE ORVALHO (°C);TEMPERATURA MÁXIMA NA HORA ANT. (AUT) (°C);TEMPERATURA MÍNIMA NA HORA ANT. (AUT) (°C);TEMPERATURA ORVALHO MAX. NA HORA ANT. (AUT) (°C);TEMPERATURA ORVALHO MIN. NA HORA ANT. (AUT) (°C);UMIDADE REL. MAX. NA HORA ANT. (AUT) (%);UMIDADE REL. MIN. NA HORA ANT. (AUT) (%);UMIDADE RELATIVA DO AR, HORARIA (%);VENTO, DIREÇÃO HORARIA (gr) (° (gr));VENTO, RAJADA MAXIMA (m/s);VENTO, VELOCIDADE HORARIA (m/s);
2023/01/01;0000 UTC;;;;;;;;;;;;;;;;;;
2023/01/01;0100 UTC;;;;;;;;;;;;;;;;;;
2023/01/01;0200 UTC;;;;;;;;;;;;;;;;;;
2023/01/01;0300 UTC;;;;;;;;;;;;;;;;;;
2023/01/01;0400 UTC;;;;;;;;;;;;;;;;;;""",
    ]

    create_files(temp_stage_invalid, csv_data_infos)

    yield temp_stage_invalid, temp_output_invalid


@pytest.fixture()
def tmp_station_mixed_data(tmp_path):
    """Create a temporary directory with mixed (valid and invalid) station data for testing.

    Parameters
    ----------
    tmp_path : py.path.local
        Fixture provided by pytest to create and return temporary directories.

    Yields
    ------
    tuple
        A tuple containing the path to the mixed staging data and the output directory.
    """
    temp_stage_mixed = tmp_path / "stage-mixed"
    temp_stage_mixed.mkdir()
    temp_output_mixed = tmp_path / "output-mixed"
    temp_output_mixed.mkdir()

    csv_data_infos = [
        """REGIAO:;CO
UF:;DF
ESTACAO:;BRASILIA
CODIGO (WMO):;A001
LATITUDE:;aaaaaaaaaa
LONGITUDE:;-47,92583332
ALTITUDE:;1160,96
DATA DE FUNDACAO:;07/05/00
Data;Hora UTC;PRECIPITAÇÃO TOTAL, HORÁRIO (mm);PRESSAO ATMOSFERICA AO NIVEL DA ESTACAO, HORARIA (mB);PRESSÃO ATMOSFERICA MAX.NA HORA ANT. (AUT) (mB);PRESSÃO ATMOSFERICA MIN. NA HORA ANT. (AUT) (mB);RADIACAO GLOBAL (Kj/m²);TEMPERATURA DO AR - BULBO SECO, HORARIA (°C);TEMPERATURA DO PONTO DE ORVALHO (°C);TEMPERATURA MÁXIMA NA HORA ANT. (AUT) (°C);TEMPERATURA MÍNIMA NA HORA ANT. (AUT) (°C);TEMPERATURA ORVALHO MAX. NA HORA ANT. (AUT) (°C);TEMPERATURA ORVALHO MIN. NA HORA ANT. (AUT) (°C);UMIDADE REL. MAX. NA HORA ANT. (AUT) (%);UMIDADE REL. MIN. NA HORA ANT. (AUT) (%);UMIDADE RELATIVA DO AR, HORARIA (%);VENTO, DIREÇÃO HORARIA (gr) (° (gr));VENTO, RAJADA MAXIMA (m/s);VENTO, VELOCIDADE HORARIA (m/s);
2023/01/01;0000 UTC;0;887,7;887,7;887,2;;20,1;17,9;20,9;20;19,2;17,8;91;87;87;187;3,3;1,2;
2023/01/01;0100 UTC;0;888,1;888,1;887,7;;19,2;17,5;20,1;19,2;17,8;17,4;90;87;90;153;2,9;,8;
2023/01/01;0200 UTC;0;887,8;888,1;887,8;;19,3;17,6;19,5;19;17,8;17,3;90;89;90;145;2,5;1,5;
2023/01/01;0300 UTC;0;887,8;887,9;887,7;;19,3;17,7;19,4;19,1;17,8;17,5;91;90;91;162;3,2;1,4;
2023/01/01;0400 UTC;0;887,6;887,9;887,6;;19,7;18,1;19,7;19,1;18,1;17,4;91;90;90;140;5,7;2,7;
2023/01/01;0500 UTC;0;886,7;887,6;886,7;;19,1;17,7;19,7;19,1;18,1;17,7;92;90;92;128;7,1;2;""",
        """REGIAO:;CO
UF:;DF
ESTACAO:;BRAZLANDIA
CODIGO (WMO):;A042
LATITUDE:;-15,59972221
LONGITUDE:;-48,1311111
ALTITUDE:;1143
DATA DE FUNDACAO:;19/07/17
Data;Hora UTC;PRECIPITAÇÃO TOTAL, HORÁRIO (mm);PRESSAO ATMOSFERICA AO NIVEL DA ESTACAO, HORARIA (mB);PRESSÃO ATMOSFERICA MAX.NA HORA ANT. (AUT) (mB);PRESSÃO ATMOSFERICA MIN. NA HORA ANT. (AUT) (mB);RADIACAO GLOBAL (Kj/m²);TEMPERATURA DO AR - BULBO SECO, HORARIA (°C);TEMPERATURA DO PONTO DE ORVALHO (°C);TEMPERATURA MÁXIMA NA HORA ANT. (AUT) (°C);TEMPERATURA MÍNIMA NA HORA ANT. (AUT) (°C);TEMPERATURA ORVALHO MAX. NA HORA ANT. (AUT) (°C);TEMPERATURA ORVALHO MIN. NA HORA ANT. (AUT) (°C);UMIDADE REL. MAX. NA HORA ANT. (AUT) (%);UMIDADE REL. MIN. NA HORA ANT. (AUT) (%);UMIDADE RELATIVA DO AR, HORARIA (%);VENTO, DIREÇÃO HORARIA (gr) (° (gr));VENTO, RAJADA MAXIMA (m/s);VENTO, VELOCIDADE HORARIA (m/s);
2023/01/01;0000 UTC;0;888,7;888,9;888,2;;20,5;18;21;20,5;18,4;18;87;84;86;146;3,9;1,8;
2023/01/01;0100 UTC;0;889;889;888,7;;20,4;17,5;20,5;20,3;18;17,5;86;84;84;142;4,1;1,8;
2023/01/01;0200 UTC;0;888,9;889,1;888,9;;19,9;17,4;20,4;19,8;17,5;17,3;86;83;86;153;4;,2;
2023/01/01;0300 UTC;0;888,7;888,9;888,7;;19,8;17,4;19,9;19,7;17,4;17,2;87;85;86;140;3,5;1,2;
2023/01/01;0400 UTC;0;888,4;888,8;888,4;;20,1;17,3;20,1;19,8;17,5;17,3;86;84;84;139;3,3;,3;
2023/01/01;0500 UTC;0;887,8;888,4;887,8;;19,8;17,6;20,3;19,8;17,6;17,2;87;83;87;138;5,2;1,3;
2023/01/01;0600 UTC;0;887,5;887,9;887,5;;19,6;17,3;19,8;19,5;17,7;17,3;89;87;87;133;4,1;,3;
2023/01/01;0700 UTC;0;887,5;887,5;887,3;;19,1;17,3;19,6;19,1;17,3;17,2;89;87;89;95;4,4;1,4;
2023/01/01;0800 UTC;0;887,6;887,6;887,5;;18,9;17,1;19,2;18,3;17,5;16,9;92;89;89;49;5,5;2,4;
2023/01/01;0900 UTC;0;888;888;887,4;5,3;18,8;17;19,1;18,4;17,1;16,6;90;88;89;73;4,8;,6;""",
        """REGIAO:;N
UF:;AM
ESTACAO:;COARI
CODIGO (WMO):;A117
LATITUDE:;-4,09749999
LONGITUDE:;-63,14527777
ALTITUDE:;33,84
DATA DE FUNDACAO:;12/04/08
Data;Hora UTC;PRECIPITAÇÃO TOTAL, HORÁRIO (mm);PRESSAO ATMOSFERICA AO NIVEL DA ESTACAO, HORARIA (mB);PRESSÃO ATMOSFERICA MAX.NA HORA ANT. (AUT) (mB);PRESSÃO ATMOSFERICA MIN. NA HORA ANT. (AUT) (mB);RADIACAO GLOBAL (Kj/m²);TEMPERATURA DO AR - BULBO SECO, HORARIA (°C);TEMPERATURA DO PONTO DE ORVALHO (°C);TEMPERATURA MÁXIMA NA HORA ANT. (AUT) (°C);TEMPERATURA MÍNIMA NA HORA ANT. (AUT) (°C);TEMPERATURA ORVALHO MAX. NA HORA ANT. (AUT) (°C);TEMPERATURA ORVALHO MIN. NA HORA ANT. (AUT) (°C);UMIDADE REL. MAX. NA HORA ANT. (AUT) (%);UMIDADE REL. MIN. NA HORA ANT. (AUT) (%);UMIDADE RELATIVA DO AR, HORARIA (%);VENTO, DIREÇÃO HORARIA (gr) (° (gr));VENTO, RAJADA MAXIMA (m/s);VENTO, VELOCIDADE HORARIA (m/s);
2023/01/01;0000 UTC;;;;;;;;;;;;;;;;;;
2023/01/01;0100 UTC;;;;;;;;;;;;;;;;;;
2023/01/01;0200 UTC;;;;;;;;;;;;;;;;;;
2023/01/01;0300 UTC;;;;;;;;;;;;;;;;;;
2023/01/01;0400 UTC;;;;;;;;;;;;;;;;;;""",
    ]

    create_files(temp_stage_mixed, csv_data_infos)

    yield temp_stage_mixed, temp_output_mixed


@pytest.fixture()
def list_with_invalid_int_years():
    """Provide a list of integer years that are not valid for data collection,
    as they precede the established start year.

    This fixture is used to simulate scenarios where the data collector receives
    years that are outside the scope of the collection period.

    Returns
    -------
    list
        A list containing integer years that fall before the first year from
        which data collection is considered valid.
    """
    return [1998, 1988, 1999]


@pytest.fixture()
def list_with_invalid_str_years():
    """Supply a list of strings that are not valid year representations for
    testing the data collector's year filtering.

    This fixture is useful for testing the data collector's ability to ignore
    non-integer inputs when filtering years.

    Returns
    -------
    list
        A list containing strings that are invalid as year inputs.
    """
    return ["A", "B", "C"]


@pytest.fixture()
def list_with_valid_years():
    """Provide a list of integer years that are all within the valid data
    collection range for testing.

    Returns
    -------
    list
        A list containing only valid integer years, starting from the first
        year data collection is considered valid.
    """
    return [2000, 2010, 2023]


@pytest.fixture()
def list_with_mix_int_years():
    """Generate a list with a mix of valid and invalid integer years for
    testing the data collector's filtering logic.

    This fixture helps to ensure that the data collector correctly identifies
    and filters out years that are not within the valid range.

    Returns
    -------
    list
        A list containing a combination of valid and invalid integer years.
    """
    return [1999, 2010, 2023]


@pytest.fixture()
def list_with_mix_int_str_years():
    """Produce a list with a combination of valid integer years and invalid
    string representations for testing year filtering.

    This fixture aids in testing the data collector's robustness in handling
    mixed data types in year lists.

    Returns
    -------
    list
        A list that includes both valid integer years and strings that
        represent invalid years.
    """
    return ["A", 2010, 2023]


class TestStationDataCollector:
    def test_collecting_empty_folder(
        self,
        tmp_path_factory,
    ):
        """Ensure StationDataCollector raises an error when scanning an empty folder.

        Parameters
        ----------
        tmp_path_factory : fixture
            Pytest fixture that provides a factory for temporary directories.

        Asserts
        ------
        ValueError
            Expected error when the collector encounters an empty folder.
        """
        stage_empty = tmp_path_factory.mktemp("stage_empty")
        output_empty = tmp_path_factory.mktemp("output_empty")

        stations_data = StationDataCollector(
            stage_empty,
            output_empty,
            STATIONS_FILE,
            STATION_COLUMN_NAMES,
            StationData,
        )

        output_file = STATIONS_FILE + ".parquet"

        output_empty_file_path = output_empty / output_file

        with pytest.raises(ValueError) as excinfo:
            stations_data.start()
        assert "No CSV files found in the specified folder." in str(
            excinfo.value,
        )
        assert not os.path.exists(output_empty_file_path)

    def test_collecting_valid_data(
        self,
        tmp_station_valid_data,
    ):
        """Ensure StationDataCollector does not create a log file when processing valid data.

        Parameters
        ----------
        tmp_station_valid_data : fixture
            Fixture providing a directory with valid CSV data.

        Asserts
        ------
        bool
            No log file is created and an output file exists.
        """
        stage_valid, output_valid = tmp_station_valid_data

        stations_data = StationDataCollector(
            stage_valid,
            output_valid,
            STATIONS_FILE,
            STATION_COLUMN_NAMES,
            StationData,
        )

        stations_data.start()

        log_file = STATIONS_FILE + "_invalid_records.log"

        log_file_path = output_valid / log_file

        output_file = STATIONS_FILE + ".parquet"

        output_valid_file_path = output_valid / output_file

        assert not os.path.exists(log_file_path)
        assert os.path.exists(output_valid_file_path)

    def test_collecting_invalid_data(
        self,
        tmp_station_invalid_data,
    ):
        """Ensure StationDataCollector creates a log file when processing invalid data.

        Parameters
        ----------
        tmp_station_invalid_data : fixture
            Fixture providing a directory with invalid CSV data.

        Asserts
        ------
        Exception
            Expected error when the collector encounters only invalid data.
        """
        stage_invalid, output_invalid = tmp_station_invalid_data

        stations_data = StationDataCollector(
            stage_invalid,
            output_invalid,
            STATIONS_FILE,
            STATION_COLUMN_NAMES,
            StationData,
        )

        log_file = STATIONS_FILE + "_invalid_records.log"
        log_file_path = output_invalid / log_file

        output_file = STATIONS_FILE + ".parquet"
        output_invalid_file_path = output_invalid / output_file

        with pytest.raises(Exception) as excinfo:
            stations_data.start()
        assert "All collected data was invalid." in str(
            excinfo.value,
        )
        assert os.path.exists(log_file_path)
        assert not os.path.exists(output_invalid_file_path)

    def test_collecting_mixed_data(
        self,
        tmp_station_mixed_data,
    ):
        """Ensure StationDataCollector handles mixed data correctly.

        Parameters
        ----------
        tmp_station_mixed_data : fixture
            Fixture providing a directory with mixed CSV data.

        Asserts
        ------
        bool
            A log file is created for invalid records and an output file exists.
        """
        stage_mixed, output_mixed = tmp_station_mixed_data

        stations_data = StationDataCollector(
            stage_mixed,
            output_mixed,
            STATIONS_FILE,
            STATION_COLUMN_NAMES,
            StationData,
        )

        output_file = STATIONS_FILE + ".parquet"
        output_mixed_file_path = output_mixed / output_file

        log_file = STATIONS_FILE + "_invalid_records.log"
        log_file_path = output_mixed / log_file

        stations_data.start()

        assert os.path.exists(log_file_path)

        assert os.path.exists(output_mixed_file_path)


first_year, last_year = limit_years()


class TestCollectYearsList:
    def test_empty_list(self):
        """Test the behavior of collect_years_list when provided with an
        empty list.

        An empty list should trigger a ValueError to indicate that valid years
        are required for processing.

        Raises
        ------
        ValueError
            If the provided list is empty.
        """
        empty_list = []
        with pytest.raises(ValueError) as excinfo:
            collect_years_list(empty_list)
        assert (
            f"The list is empty. Provide a list with years after {first_year} and before {last_year}."
            in str(excinfo.value)
        )

    def test_all_valid_years(
        self,
        list_with_valid_years,
    ):
        """Test collect_years_list with a list containing only valid years.

        The function should return the same list of years if all are valid
        within the accepted range.

        Parameters
        ----------
        list_with_valid_years : list
            A list of years that are all valid and within the collection range.
        """
        result_list = collect_years_list(list_with_valid_years)

        assert result_list == [2000, 2010, 2023]

    def test_all_invalid_years(
        self,
        list_with_invalid_int_years,
        list_with_invalid_str_years,
    ):
        """Test collect_years_list with lists that contain only invalid years.

        The function is expected to raise a ValueError indicating that no
        valid years were provided.

        Parameters
        ----------
        list_with_invalid_int_years : list
            A list of integers representing years, all of which are invalid.
        list_with_invalid_str_years : list
            A list of strings, none of which are valid representations of years.

        Raises
        ------
        ValueError
            If the list does not contain any valid years.
        """
        with pytest.raises(ValueError) as excinfo:
            collect_years_list(list_with_invalid_int_years)
        assert (
            f"The list is empty. Provide a list with years after {first_year} and before {last_year}."
            in str(excinfo.value)
        )

        with pytest.raises(ValueError) as excinfo:
            collect_years_list(list_with_invalid_str_years)
        assert (
            f"The list is empty. Provide a list with years after {first_year} and before {last_year}."
            in str(excinfo.value)
        )

    def test_mixed_list(
        self,
        list_with_mix_int_years,
        list_with_mix_int_str_years,
        capsys,
    ):
        """Test collect_years_list with lists that contain a mixture of valid
        and invalid years or strings.

        Valid years should be returned and invalid ones should be reported via
        standard output.

        Parameters
        ----------
        list_with_mix_int_years : list
            A list containing a mix of valid and invalid integer years.
        list_with_mix_int_str_years : list
            A list containing both valid integer years and invalid string years.
        capsys : fixture
            Pytest fixture that captures standard output and error streams.
        """
        result_with_int = collect_years_list(list_with_mix_int_years)

        assert result_with_int == [2010, 2023]

        assert (
            capsys.readouterr().out
            == "The elements [1999] were removed from the list.\n"
        )

        result_with_str = collect_years_list(list_with_mix_int_str_years)

        assert result_with_str == [2010, 2023]

        assert (
            capsys.readouterr().out
            == "The elements ['A'] were removed from the list.\n"
        )
