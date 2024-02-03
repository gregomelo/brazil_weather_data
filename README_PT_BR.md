
# Brazil Weather Data API

[Read in English](./README.md)

## Introdução
O projeto Dados Meteorológicos do Brasil é uma iniciativa puramente educacional voltada à transformação dos dados das estações meteorológicas automáticas brasileiras em uma API leve. Este projeto serve como um excelente recurso para aprendizagem e experimentação no desenvolvimento de API, manipulação de dados e práticas modernas de engenharia de software.

A API é hospedada em uma serviço gratuíto do [Render](https://brazil-weather-data.onrender.com/). Sendo assim, a etapa de criação da base de dados foi feita localmente e depois carregada no repositório do projeto.

Você pode acessar a documentação em [GitHub](https://gregomelo.github.io/brazil_weather_data/).

## Objetivo do Projeto
O objetivo principal deste projeto é fornecer acesso fácil e estruturado aos dados meteorológicos das estações meteorológicas brasileiras. A API oferece três principais endpoints:
- `stations_data`: Fornece dados sobre as estações meteorológicas, incluindo localização geográfica e data de implementação.
- `weather_data`: Oferece dados meteorológicos coletados das estações.
- `query`: Permite que os usuários simulem consultas SQL para recuperação de dados personalizada.

## Fonte de Dados
Os dados são obtidos do Instituto Nacional de Meteorologia do Brasil (INMET - [Instituto Nacional de Meteorologia](https://portal.inmet.gov.br/)), garantindo confiabilidade e abrangência.

## Refatoração e Tecnologias Aplicadas
Este projeto é uma versão refatorada de um [caderno do Kaggle](https://www.kaggle.com/code/gregoryoliveira/brazil-weather-change-part-i-data-collection) focado na coleta e análise de dados meteorológicos brasileiros. A pilha técnica para este projeto inclui:
- pandas para manipulação de dados.
- FastAPI para construção da API.
- DuckDB como sistema de gerenciamento de banco de dados.
- pytest para execução de testes.
- Pre-commit e Commitizen para garantir a qualidade do código e padronização das mensagens de commit.
- Integração Contínua e Implantação Contínua (CI/CD) usando GitHub Actions.

## Banco de Dados e Escalabilidade
Inicialmente, o banco de dados deste projeto foi povoado localmente devido a restrições de recursos. Essa abordagem garantiu uma base sólida para nossa análise de dados inicial e funcionalidade da API. No entanto, em um cenário profissional, o sistema é projetado para ser escalável e automatizado.

Idealmente, a pipeline de coleta de dados seria configurada para rodar automaticamente em uma base mensal. Isso permitiria que o banco de dados fosse continuamente atualizado com os dados meteorológicos mais recentes. Tal configuração não apenas forneceria insights em tempo real, mas também enriqueceria o banco de dados ao longo do tempo, aprimorando a profundidade e a precisão de nossas análises.

Essa abordagem automatizada, combinada com uma solução de hospedagem mais robusta, tornaria o projeto mais dinâmico e valioso para a análise de dados meteorológicos contínua e pesquisa.

## Uso
### Iniciando o webservice da API:
1. Execute os comandos:
```bash
poetry run task run
```
2. Abra seu navegador preferido e navegue para [Brazil Weather Data API](http://127.0.0.1:8000/docs).

Note: se você está rodando outro processo na porta 8000, você precisa editar a porta na seção `[tool.taskipy.tasks]`  do arquivo `pyproject.toml`.

Se você quiser matar todos os procssos da porta 8000, execute o comando:
```bash
poetry run task killr
```

### Accessing local documentation:
1. Execute os comandos:

```bash
poetry run task run
```

2. Abra seu navegador preferido e navegue para [Brazil Weather Data API Docs](http://127.0.0.1:8001).

Note: se você está rodando outro processo na porta 8001, você precisa editar a porta na seção `[tool.taskipy.tasks]` section at `pyproject.toml` file.

Se você quiser matar todos os procssos da porta 8000, execute o comando:
```bash
poetry run task killd
```

### Executando os testes
Execute os comandos:
```bash
poetry run task test
```

### Executando a pipeline de dados
Execute os comandos:
```bash
poetry run task pipeline -- list_years
```

Por exemplo, para rodar somente para o ano de 2023:
```bash
poetry run task pipeline -- 2023
```

Para mais de um ano, digitei os anos com espaço simples entre eles:
```bash
poetry run task pipeline -- 2023 2022 2021
```

## Recursos Futuros
- Documentação abrangente renderizada com Mkdocs.
- Recursos e melhorias adicionais na API.

## Contribuição
Como um projeto educacional, contribuições são altamente encorajadas. Seja para corrigir bugs, adicionar funcionalidades ou melhorar a documentação, sua contribuição é bem-vinda. Por favor, siga o fluxo padrão do GitHub para contribuições.

## Licença
Este projeto é de código aberto sob a [MIT license](https://opensource.org/licenses/MIT).

---
Aproveite para explorar e utilizar Brazil Weather Data API! 🌦️🇧🇷

---
