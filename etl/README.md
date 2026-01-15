# ETL - Modelagem ESG Banco Votorantim

Scripts Python para importar dados dos arquivos Excel para SQL Server (Local ou Azure).

## Pre-requisitos

1. **Python 3.10+**
2. **SQL Server** (uma das opcoes):
   - SQL Server Developer/Express (local)
   - SQL Server LocalDB (vem com Visual Studio)
   - Azure SQL Server (nuvem)
3. **ODBC Driver 18 for SQL Server**
   - [Download Microsoft](https://docs.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server)

## Instalacao

```bash
cd etl
pip install -r requirements.txt
```

## Configuracao

### Opcao 1: Conexao LOCAL (SQL Server na maquina)

1. Crie o arquivo `.env`:
```bash
cp .env.example .env
```

2. Configure para modo local (ja e o padrao):
```env
DB_MODE=local
LOCAL_SQL_SERVER=localhost
LOCAL_SQL_DATABASE=ESG_BV
# Deixe vazio para Windows Authentication
LOCAL_SQL_USER=
LOCAL_SQL_PASSWORD=
```

**Servidores comuns:**
| Tipo | Servidor |
|------|----------|
| SQL Server padrao | `localhost` |
| SQL Server Express | `localhost\SQLEXPRESS` |
| LocalDB | `(localdb)\MSSQLLocalDB` |

### Opcao 2: Conexao AZURE

1. Configure o `.env`:
```env
DB_MODE=azure
AZURE_SQL_SERVER=seu-servidor.database.windows.net
AZURE_SQL_DATABASE=ESG_BV
AZURE_SQL_USER=seu_usuario
AZURE_SQL_PASSWORD=sua_senha
```

## Criar o Database

### Local (SSMS)
1. Abra o SQL Server Management Studio
2. Conecte ao seu servidor
3. Execute `sql/00_create_database_simple.sql`
4. Execute os scripts `01` a `06` na ordem

### Azure
1. Crie o database no Portal Azure
2. Execute os scripts `01` a `06`

## Uso do ETL

### Testar conexao
```bash
python main.py --test
```

### Verificar arquivos Excel
```bash
python main.py --check
```

### ETL completo
```bash
python main.py
```

### ETL com limpeza de tabelas
```bash
python main.py --truncate
```

### Apenas dimensoes
```bash
python main.py --dim
```

### Apenas fatos
```bash
python main.py --fato
```

### Trocar modo via linha de comando
```bash
# Usar local
set DB_MODE=local && python main.py --test

# Usar Azure
set DB_MODE=azure && python main.py --test
```

## Estrutura dos Arquivos

```
etl/
├── config.py          # Configuracoes LOCAL e AZURE
├── database.py        # Conexao com SQL Server
├── etl_dimensoes.py   # ETL tabelas de dimensao
├── etl_fatos.py       # ETL tabelas de fato
├── main.py            # Script principal
├── requirements.txt   # Dependencias Python
├── .env.example       # Exemplo de configuracao
└── README.md          # Este arquivo
```

## Fluxo do ETL

```
1. Conexao (Local ou Azure)
        |
2. ETL Dimensoes
   ├── DimCNAE
   ├── DimSubSetor
   ├── DimProduto
   ├── DimEmpresa
   ├── DimMetaODS
   └── BridgeKPIODS
        |
3. ETL Fatos
   ├── FatoCarteira
   ├── FatoKPI
   ├── FatoIndicadorSaneamento
   ├── FatoIndicadorSaude
   ├── FatoMeta2030
   └── ValidacaoEmpresa
        |
4. Log e Estatisticas
```

## Conexao Power BI

### Local
```
Servidor: localhost (ou localhost\SQLEXPRESS)
Banco: ESG_BV
Autenticacao: Windows
```

### Azure
```
Servidor: seu-servidor.database.windows.net
Banco: ESG_BV
Autenticacao: SQL Server (usuario/senha)
```

No Power BI Desktop:
1. Obter Dados > SQL Server
2. Preencha servidor e banco
3. Selecione as views do schema `esg`

## Troubleshooting

### Erro: ODBC Driver not found
```
Instale o ODBC Driver 18:
https://go.microsoft.com/fwlink/?linkid=2223304
```

### Erro: Login failed (local)
```
1. Abra SQL Server Configuration Manager
2. Habilite TCP/IP em SQL Server Network Configuration
3. Reinicie o servico SQL Server
```

### Erro: Cannot connect to localhost\SQLEXPRESS
```
1. Verifique se o servico "SQL Server (SQLEXPRESS)" esta rodando
2. Services.msc > SQL Server (SQLEXPRESS) > Start
```

### Erro: Certificate not trusted (local)
```
Ja configurado no config.py com TrustServerCertificate=yes
```

### Erro: Schema 'esg' not found
```
Execute os scripts SQL (01 a 06) antes de rodar o ETL
```
