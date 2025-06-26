# You_intelligence_062025# ⚡ Youon Intelligence – Plataforma de Inteligência de Mercado no Setor de Energia

Bem-vindo ao repositório oficial do **Youon Intelligence**, uma plataforma completa de inteligência comercial e técnica para o setor de energia, focada em mapeamento de mercado, enriquecimento de leads e insights estratégicos para os produtos da You.On (Arbitragem, Backup, GTD, etc).

---

## 💡 Visão Geral

Este projeto coleta, trata e analisa **dados públicos massivos** do setor energético brasileiro e chileno, transforma esses dados em **informações qualificadas** e os disponibiliza via dashboards e APIs para uso por times técnicos, comerciais e de marketing.

---

## 📌 Funcionalidades Principais

* 🔄 Ingestão automática de dados da ANEEL, CCEE, Receita Federal, entre outras
* 🧹 Tratamento e normalização massiva de dados (90M+ registros)
* 🎯 Enriquecimento de leads com CNPJ, geolocalização, CNAE, porte, etc
* 🧠 Aplicação de IA para classificação de leads e recomendação de abordagem
* 🗺️ Visualização espacial via mapas de calor e pontos
* 📊 Dashboards interativos com filtros, KPIs e insights estratégicos
* 🌐 API REST em FastAPI para integração com outros sistemas
* 🧽 Orquestração completa com Airflow
* 📅 Scraping inteligente de notícias sobre o setor energético (leilões, leis, etc.)

---

## 🛠️ Stack Tecnológica

| Camada         | Tecnologia                                |
| -------------- | ----------------------------------------- |
| Backend        | Python 3.11, FastAPI                      |
| Frontend       | Next.js (React) + Tailwind CSS            |
| Banco de Dados | PostgreSQL (Azure)                        |
| Jobs & ETL     | Python (Pandas, Async, Requests)          |
| Orquestração   | Apache Airflow                            |
| IA / ML        | Scikit-Learn, HuggingFace                 |
| Deploy         | Docker, Docker Compose, Terraform (Azure) |

---

# 🗂️ Folder Structure – Youon Intelligence (Scalable)

This is the **final and scalable folder structure** for the Youon Intelligence project, following best practices, clean architecture principles and all naming conventions in **English** for clarity, integration and future extensibility.

```
youon-intelligence/

├── apps/                          # Main applications (API and Frontend)
│   ├── api/                       # FastAPI application (Python backend)
│   │   ├── main.py
│   │   ├── routes/                # Endpoints (controllers)
│   │   ├── services/              # Business logic
│   │   ├── schemas/               # Pydantic models (DTOs)
│   │   ├── dependencies/          # Auth, DB, etc.
│   │   └── config.py              # App settings
│   └── frontend/                  # Next.js + Tailwind (React frontend)
│       ├── pages/
│       ├── components/
│       ├── services/
│       ├── layouts/
│       └── styles/

├── packages/                      # Reusable backend modules
│   ├── jobs/                      # Data pipelines (ETL)
│   │   ├── importers/             # UCAT, UCMT, UCBT...
│   │   ├── transformers/          # Normalization & standardization
│   │   ├── enrichers/             # CNPJ, Geo, CNAE...
│   │   └── exporters/             # Exports to BI/API
│   ├── ai/                        # Machine Learning and AI
│   │   ├── training/              # Model training and validation
│   │   ├── models/                # Checkpoints and models
│   │   └── inference/             # Predictions and API integration
│   ├── database/                  # DB schemas, connection logic (SQLAlchemy)
│   ├── orchestrator/              # Airflow DAGs and operators
│   └── core/                      # Helpers, logging, utilities

├── infra/                         # Infrastructure as code
│   ├── docker/                    # Dockerfiles and Docker Compose setup
│   ├── terraform/                 # Azure deployment scripts
│   └── scripts/                   # Backup, restore, setup scripts

├── data/                          # Temporary and persisted data
│   ├── downloads/                 # Raw CSVs and ZIPs
│   ├── processed/                 # Cleaned and formatted
│   ├── logs/                      # Logs from job execution
│   └── models/                    # Trained ML models

├── tests/                         # Automated testing
│   ├── api/                       # API routes
│   ├── jobs/                      # ETL pipelines
│   └── ai/                        # Inference and ML models

├── docs/                          # Technical documentation
│   ├── dataset_dictionary.md      # Dataset map and description
│   └── architecture.md            # System diagrams and logic

├── .env.example                   # Example of required environment variables
├── requirements.txt               # Python dependencies
├── docker-compose.yml             # Local orchestration
├── README.md                      # Main project README
└── Makefile                       # Command shortcuts (make import-ucat, etc.)
```

Everything is now in **English**, modular and future-proof.
Let me know if you want descriptions per folder or to bootstrap the first job or API endpoint right away!


## 🚀 Primeiros Passos (Ambiente de Desenvolvimento)

1. **Clone o projeto**

   ```bash
   git clone https://github.com/youon/youon-intelligence.git
   cd youon-intelligence
   ```

2. **Instale os requisitos**

   ```bash
   pip install -r requirements.txt
   ```

3. \*\*Configure o \*\*\`\`
   Copie o `.env.example` e preencha as credenciais do banco, APIs e paths.

4. **Execute a API (FastAPI)**

   ```bash
   uvicorn apps.api.main:app --reload
   python -m venv venv
   venv\Scripts\activate  

   ```

5. **Rode um job (exemplo)**

   ```bash
   python packages/jobs/importar/importar_ucbt.py
   ```

---

## 🔎 Airflow – Orquestração

* DAGs organizadas em `packages/orchestrator/dags/`
* Rodam pipelines de importação → tratamento → enriquecimento
* Possível deploy local ou via Google Composer/Azure Data Factory

---

## 🔬 Inteligência Artificial

Modelos treináveis para:

* Classificação automática de lead bom/ruim
* Previsão de solução ideal (Arbitragem, GTD, etc)
* Análise semântica de notícias
* Análise de clusters de consumo/qualidade/mercado

Script principal: `packages/ai/inference/lead_predictor.py`

---

## 📦 Dados Utilizados

Bases públicas (automatizadas):

* [ANEEL BDGD Geo](https://dadosabertos-aneel.opendata.arcgis.com/)
* [Bases CSV da ANEEL (UCAT, UCMT, UCBT)](https://dadosabertos.aneel.gov.br/)
* [CCEE](https://www.ccee.org.br/web/guest/dados-compartilhados)
* Receita Federal (via CNPJá API)
* OpenWeather, Google Maps, IBGE, Mapas Climáticos (opcional)

Ver documentação completa em: [`docs/dicionario_bases_dados.md`](docs/dicionario_bases_dados.md)

---

## 🤪 Testes

* Testes com `pytest`
* `tests/api/` cobre rotas da API
* `tests/jobs/` cobre jobs de transformação
* Cobertura via `coverage`

---

## 🌐 Deploy

Recomendado com Docker + Azure:

```bash
docker-compose up --build
```

Infra como código (Azure):

```
infra/terraform/
```

---

## 🔐 Segurança e Autenticação

* Autenticação baseada em JWT (apenas para equipe interna)
* Recomendado: uso de VPN ou IP restrito para acesso administrativo

---

## 👨‍💻 Contribuidores

* Guilherme Costa Proença – Engenharia de Software & Dados
* \[@seuGitHub] – Backend, IA
* \[@outrosDevs] – Frontend, DevOps

---

## 🗌 Próximas Etapas

*

---

## 📣 Contato

Para dúvidas, ideias ou contribuições, entre em contato com a equipe técnica da You.On ou abra uma issue no repositório.

---

> “Não adianta ter os dados. O valor está em transformá-los em inteligência.” – YouOn Tech Team
