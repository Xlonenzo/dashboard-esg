# CLAUDE.md - Dashboard ESG

This document provides comprehensive guidance for AI assistants working with this codebase.

## Project Overview

This is an **Environmental, Social, and Governance (ESG) Dashboard** that provides comprehensive analysis and monitoring of sustainable investments and green financing in Brazil. The platform integrates multiple data sources including ANBIMA (Brazilian investment funds association), TSB (Taxonomia Sustentável Brasileira - Brazilian Sustainable Taxonomy), and CVM (Comissão de Valores Mobiliários) corporate data.

### Key Features
- Track ESG compliance and sustainable investment portfolios
- Monitor sustainable bonds, green bonds, social bonds, and sustainability-linked instruments
- Analyze Brazilian companies against the Sustainable Taxonomy framework
- AI-powered financial consulting using Groq LLaMA models
- Integration with official ANBIMA API for fund data

## Directory Structure

```
/dashboard-esg/
├── api/                      # Flask API Backend
│   ├── servidor.py           # Main API with 27+ endpoints
│   ├── app.py                # Alternative Flask app configuration
│   ├── carregar_fundos.py    # Load funds from CSV to database
│   ├── carregar_emissores.py # Load public company data from CVM
│   ├── carregar_cricra.py    # Load CRI/CRA securities data
│   └── tsb_report.py         # TSB taxonomy mapping report
│
├── dashboard/                # Frontend HTML Files
│   ├── dashboard_anbima_real.html  # Main dashboard (funds + TSB + securities)
│   ├── dashboard_anbima_esg.html   # Alternative ESG-focused dashboard
│   ├── dashboard_tsb.html          # TSB taxonomy dashboard
│   └── index.html                  # Login page
│
├── etl/                      # ETL Pipeline
│   ├── main.py               # Main ETL orchestrator
│   ├── config.py             # Database & file path configuration
│   ├── database.py           # PostgreSQL connection manager
│   ├── etl_dimensoes.py      # Dimension table loading
│   ├── etl_fatos.py          # Fact table loading
│   ├── .env.example          # Environment configuration template
│   └── anbima/               # ANBIMA-specific ETL scripts
│       ├── scraper_anbima_api.py  # Official ANBIMA API client (OAuth2)
│       └── scraper_anbima.py      # Web scraping for ANBIMA data
│
├── sql/                      # SQL Server Database Schema (legacy)
│   ├── 00_create_database.sql
│   ├── 01_create_schema.sql
│   ├── 02_dim_tables.sql     # Dimension tables
│   ├── 03_fact_tables.sql    # Fact tables
│   ├── 04_bridge_tables.sql  # Many-to-many relationships
│   └── 05_initial_data.sql   # ODS, setores, categorias
│
├── sql_postgres/             # PostgreSQL Migration (current)
│   └── 00_create_database.sql
│
├── sql_anbima/               # ANBIMA Database Schema
│   ├── 00_master_deploy.sql  # Full deployment script
│   └── 07_tsb_dim_tables.sql # TSB-specific tables
│
├── render.yaml               # Render.com deployment config
├── requirements.txt          # Python dependencies
└── .gitignore
```

## Technology Stack

### Backend
- **Flask 2.0+** - Python web framework with CORS support
- **PostgreSQL** - Primary database (migrated from SQL Server)
- **psycopg2-binary** - PostgreSQL adapter
- **SQLAlchemy 2.0+** - ORM for database operations
- **Gunicorn** - Production WSGI HTTP server

### Frontend
- **HTML5/CSS3/JavaScript** - Static HTML dashboards (no framework)
- Responsive design with vanilla JavaScript

### AI Integration
- **Groq API** - LLaMA-3.3-70B model for intelligent financial consulting
- Context-aware responses using live database data

### Data Processing
- **Pandas 2.0+** - Data manipulation and analysis
- **OpenPyXL 3.1+** - Excel file processing

### Deployment
- **Render.com** - Cloud hosting platform
- **Python 3.11** - Runtime version

## Development Setup

### Prerequisites
- Python 3.10+
- PostgreSQL 14+
- Git

### Local Development

```bash
# Clone and setup
git clone <repository>
cd dashboard-esg

# Create virtual environment
python -m venv venv
source venv/bin/activate  # Linux/Mac
# or: venv\Scripts\activate  # Windows

# Install dependencies
pip install -r requirements.txt

# Configure environment
cd etl
cp .env.example .env
# Edit .env with your database credentials

# Test database connection
python main.py --test

# Run ETL (if needed)
python main.py

# Start API server
cd ../api
python servidor.py  # Runs on port 5000
```

### Environment Variables

Required environment variables (set in `.env` or system):

```bash
DB_MODE=local              # "local" or "cloud"
PG_HOST=localhost
PG_PORT=5432
PG_DATABASE=esg_bv
PG_USER=postgres
PG_PASSWORD=your_password
GROQ_API_KEY=your_key     # Optional, for AI features
```

## Database Schema

The database uses a **Star Schema** optimized for BI/analytics:

### Schemas
- `esg` - Core ESG tables (companies, sectors, KPIs)
- `fundos` - Investment fund data (ANBIMA)
- `titulos` - Fixed income securities (bonds, debentures)
- `tsb` - Brazilian Sustainable Taxonomy data
- `emissores` - Listed companies (CVM data)

### Key Dimension Tables
- `DimEmpresa` - Companies with CNPJ, sector, category
- `DimSetor` - Sectors: Energia, Saneamento, Saúde, Educação, Inclusão Digital
- `DimCategoria` - ESG categories: Green, Social, Sustainable
- `DimODS` - 17 UN Sustainable Development Goals
- `DimTempo` - Date dimension (2020-2035)

### Key Fact Tables
- `FatoCarteira` - Portfolio values by company/period
- `FatoKPI` - KPI measurements for companies
- `FatoMeta2030` - 2030 sustainability goals and progress
- `FatoEarlyWarning` - Early warning risk indicators

### ANBIMA Tables
- `fundos.todosfundos` - All active funds
- `fundos.gestorassimilares` - Fund managers
- `titulos.debentures` - Corporate bonds
- `titulos.cricra` - Receivable securitizations

### Table Naming
- PostgreSQL uses **lowercase** table/column names
- CNPJ is stored as VARCHAR (cleaned of special characters)
- Timestamps use `TIMESTAMP` type

## API Reference

The Flask API (`api/servidor.py`) exposes 27+ endpoints:

### Health & Status
- `GET /api/health` - Database connection check

### Funds (ANBIMA)
- `GET /api/fundos` - List funds with pagination, filtering, search
  - Query params: `page`, `per_page`, `search`, `categoria`, `tipo`, `fonte`
- `GET /api/fundos/categorias` - Aggregate fund categories
- `GET /api/fundos/stats` - Fund statistics

### Fund Managers
- `GET /api/gestoras` - List fund managers with metrics
- `GET /api/gestoras/<gestora>/fundos` - Funds by specific manager
- `GET /api/gestoras/search` - Search managers by name

### Fixed Income Securities
- `GET /api/cricra` - CRI/CRA securitizations
- `GET /api/debentures` - Corporate bonds
- `GET /api/titulos-publicos` - Government securities

### TSB (Brazilian Sustainable Taxonomy)
- `GET /api/tsb/empresas` - Companies classified under TSB
- `GET /api/tsb/kpis` - KPI indicators
- `GET /api/tsb/empresa/<id>/kpis` - KPIs for specific company
- `GET /api/tsb/titulos-verdes` - Green classified securities
- `GET /api/tsb/fundos-sustentaveis` - ESG/sustainability funds
- `GET /api/tsb/visao-geral` - Dashboard consolidation

### Public Companies (CVM)
- `GET /api/emissores` - Listed companies with filters
- `GET /api/emissores/<cnpj>` - Detailed company info
- `GET /api/emissores/stats` - Company statistics

### AI Consulting
- `POST /api/ai/consulta` - Intelligent financial queries
  - Body: `{ "mensagem": "question", "tipo_resposta": "...", "contexto": "...", "historico": [] }`

### Risk Analysis
- `GET /api/risk-scoring` - Portfolio risk assessment

## Key Coding Conventions

### Python Style
- Use Python 3.10+ features
- Follow PEP 8 guidelines
- Use f-strings for string formatting
- Use type hints where practical
- SQL queries use parameterized statements (prevent SQL injection)

### Database Queries
```python
# Always use parameterized queries
cursor.execute(
    "SELECT * FROM fundos.todosfundos WHERE categoria = %s",
    (categoria,)
)
```

### Error Handling
```python
# API endpoints return consistent JSON structure
return jsonify({
    "success": True,
    "data": results,
    "total": len(results)
})

# Error responses
return jsonify({
    "success": False,
    "error": "Error message"
}), 500
```

### Language
- Code comments: Portuguese (pt-BR)
- Variable names: English preferred, but Portuguese acceptable
- API responses: Portuguese field names (e.g., `nome`, `categoria`)

## UI/Frontend Standards

### Tables
- **All data tables MUST include column filters** for filtering data directly in each column header
- Use **Simple-DataTables** library (https://github.com/fiduswriter/Simple-DataTables) for table functionality
- Required features for all tables:
  - Column filters (text input or dropdown as appropriate)
  - Sortable columns
  - Pagination for large datasets
  - Responsive design for mobile

### Simple-DataTables Integration
```html
<!-- Add to HTML head -->
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/simple-datatables@latest/dist/style.css">
<script src="https://cdn.jsdelivr.net/npm/simple-datatables@latest"></script>

<!-- Initialize table -->
<script>
const table = new simpleDatatables.DataTable("#myTable", {
    searchable: true,
    fixedHeight: true,
    perPage: 25
});
</script>
```

## Deployment

### Render.com Deployment

The project deploys automatically to Render.com via `render.yaml`:

1. Push to GitHub
2. Render auto-builds from configuration
3. Database: PostgreSQL (esg-db)
4. Start command: `gunicorn api.servidor:app --bind 0.0.0.0:$PORT`

### Manual Deploy
```bash
# Build
pip install -r requirements.txt

# Run production server
gunicorn api.servidor:app --bind 0.0.0.0:5000
```

## ETL Pipeline

The ETL pipeline loads data from Excel files and external APIs:

### Commands
```bash
cd etl

# Test database connection
python main.py --test

# Verify Excel files exist
python main.py --check

# Run full ETL
python main.py

# Truncate and reload dimensions only
python main.py --truncate --dim

# Load fact tables only
python main.py --fato
```

### Data Sources
- Excel files (5 sectors: Energia, Saneamento, Saúde, Educação, Inclusão Digital)
- ANBIMA API (OAuth2 authentication)
- CVM public company data

## ESG Frameworks

The system tracks compliance with multiple ESG frameworks:

1. **Green Bond Principles (ICMA)**
2. **Social Bond Principles (ICMA)**
3. **Sustainability Linked Bonds/Loans**
4. **Brazilian Sustainable Taxonomy (TSB)**
5. **FEBRABAN Green Taxonomy**
6. **Blue Economy Bonds**

## Business Domains

### Sectors Tracked
- **Energia** - Renewable energy, efficiency, generation
- **Saneamento** - Water, sewage, sanitation services
- **Saúde** - Healthcare, medical facilities
- **Educação** - Educational institutions
- **Inclusão Digital** - Digital technology & inclusion

### ESG Categories
- **Green** - Environmental focus (#28A745)
- **Social** - Social impact focus (#007BFF)
- **Sustainable** - Mixed ESG (#6F42C1)

### TSB Classifications
- **VERDE** - Full compliance with TSB
- **TRANSIÇÃO** - Transition pathway
- Score: 0-100 for sustainability rating

## Testing

### Available Tests
```bash
# Database connection test
python etl/main.py --test

# API health check
curl http://localhost:5000/api/health
```

### Manual Testing Points
- Data validation in ETL (CNPJ cleanup, number extraction)
- Duplicate prevention in fund loading
- Fallback responses when Groq API unavailable

## Git Workflow

### Branch Naming
- Feature branches: `claude/<description>-<id>`
- Main development branch: `main`

### Commit Messages
- Use descriptive messages in Portuguese or English
- Reference the feature/fix being implemented

## Common Tasks

### Adding a New API Endpoint
1. Add route function in `api/servidor.py`
2. Use parameterized SQL queries
3. Return consistent JSON structure
4. Add CORS headers (handled by flask-cors)

### Adding a New Dimension Table
1. Add SQL definition in `sql_postgres/00_create_database.sql`
2. Add loading logic in `etl/etl_dimensoes.py`
3. Update foreign keys in dependent fact tables

### Loading New Fund Data
1. Prepare CSV with required columns
2. Use `api/carregar_fundos.py` or update ETL pipeline
3. Verify data in `fundos.todosfundos` table

## Security Notes

### Implemented
- Parameterized SQL queries (SQL injection prevention)
- CORS configuration
- Environment variables for secrets
- PostgreSQL SSL for cloud connections

### Not Implemented (be aware)
- No user authentication/authorization layer
- API endpoints are publicly accessible
- No rate limiting

## Known Limitations

- Free Render tier may have performance constraints
- ANBIMA API credentials required for real fund data
- No caching layer (database queried on each request)
- Some ETL scripts have hardcoded Windows paths
- Groq API key required for AI consulting features

## Troubleshooting

### Database Connection Issues
```bash
# Test connection
python etl/main.py --test

# Check environment variables
echo $PG_HOST $PG_DATABASE
```

### API Not Starting
- Verify `requirements.txt` dependencies installed
- Check port 5000 is available
- Verify database credentials in environment

### ETL Failures
- Run `python main.py --check` to verify input files
- Check PostgreSQL logs for constraint violations
- Verify schema exists: `CREATE SCHEMA IF NOT EXISTS esg;`

## Documentation References

- `sql/MODELAGEM_ESG.md` - Star schema documentation
- `sql/INDICADORES_FRAMEWORKS_EXPLICACAO.md` - ESG frameworks mapping
- `etl/README.md` - ETL pipeline documentation
