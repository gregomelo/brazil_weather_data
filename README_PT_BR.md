
# Brazil Weather Data API

[Read in English](./README.md)

## Introdução
O projeto Dados Meteorológicos do Brasil é uma iniciativa puramente educacional voltada à transformação dos dados das estações meteorológicas automáticas brasileiras em uma API leve. Este projeto serve como um excelente recurso para aprendizagem e experimentação no desenvolvimento de API, manipulação de dados e práticas modernas de engenharia de software.

A API é hospedada em uma serviço gratuíto do [Render](https://brazil-weather-data.onrender.com/). Sendo assim, a etapa de criação da base de dados foi feita localmente e depois carregada no repositório do projeto.

## Objetivo do Projeto
O objetivo principal deste projeto é fornecer acesso fácil e estruturado aos dados meteorológicos das estações meteorológicas brasileiras. A API oferece três principais endpoints:
- `stations_data`: Fornece dados sobre as estações meteorológicas, incluindo localização geográfica e data de implementação.
- `weather_data`: Oferece dados meteorológicos coletados das estações.
- `query`: Permite que os usuários simulem consultas SQL para recuperação de dados personalizada.

## Fonte de Dados
Os dados são obtidos do Instituto Nacional de Meteorologia do Brasil (INMET - [Instituto Nacional de Meteorologia](https://portal.inmet.gov.br/)), garantindo confiabilidade e abrangência.

## Refatoração e Pilha Técnica
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

## Instalação
Para instalar e executar o projeto, siga os passos abaixo:

1. Clone o repositório do GitHub:
   ```
   git clone https://github.com/gregomelo/brazil_weather_data.git
   ```
2. Instale as dependências usando [Poetry](https://python-poetry.org/):
   ```
   poetry install
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
