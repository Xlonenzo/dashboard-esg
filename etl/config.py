"""
Configuracoes do ETL - Modelagem ESG Banco Votorantim
Suporta conexao LOCAL e AZURE
"""
import os
from pathlib import Path

# =============================================================================
# DIRETORIOS
# =============================================================================
BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR
ENERGIA_DIR = DATA_DIR / "Energia"
SANEAMENTO_DIR = DATA_DIR / "Saneamento"
SAUDE_DIR = DATA_DIR / "Saude"
EDUCACAO_DIR = DATA_DIR / "Educação"
INCLUSAO_DIR = DATA_DIR / "Inclusão Digital"

# =============================================================================
# MODO DE CONEXAO: "local" ou "azure"
# =============================================================================
# Altere para "azure" quando for usar o Azure SQL Server
CONNECTION_MODE = os.getenv("DB_MODE", "local")

# =============================================================================
# CONFIGURACAO LOCAL (SQL Server / SQL Server Express / LocalDB)
# =============================================================================
LOCAL_CONFIG = {
    # Opcoes de servidor local:
    # - "localhost" ou "." para SQL Server padrao
    # - "localhost\\SQLEXPRESS" para SQL Server Express
    # - "(localdb)\\MSSQLLocalDB" para LocalDB
    "server": os.getenv("LOCAL_SQL_SERVER", "localhost"),
    "database": os.getenv("LOCAL_SQL_DATABASE", "ESG_BV"),
    # Autenticacao Windows (deixe vazio para usar)
    "username": os.getenv("LOCAL_SQL_USER", ""),
    "password": os.getenv("LOCAL_SQL_PASSWORD", ""),
    # Driver - tente 18, senao 17
    "driver": "{ODBC Driver 18 for SQL Server}",
    # Para conexao local, geralmente precisa confiar no certificado
    "encrypt": "yes",
    "trust_server_certificate": "yes",
}

# =============================================================================
# CONFIGURACAO AZURE SQL
# =============================================================================
AZURE_CONFIG = {
    "server": os.getenv("AZURE_SQL_SERVER", "seu-servidor.database.windows.net"),
    "database": os.getenv("AZURE_SQL_DATABASE", "ESG_BV"),
    "username": os.getenv("AZURE_SQL_USER", "seu_usuario"),
    "password": os.getenv("AZURE_SQL_PASSWORD", "sua_senha"),
    "driver": "{ODBC Driver 18 for SQL Server}",
    "encrypt": "yes",
    "trust_server_certificate": "no",
}

# =============================================================================
# SELECAO AUTOMATICA DE CONFIGURACAO
# =============================================================================
DB_CONFIG = LOCAL_CONFIG if CONNECTION_MODE == "local" else AZURE_CONFIG

# =============================================================================
# FUNCOES DE CONEXAO
# =============================================================================

def get_connection_string():
    """Retorna a string de conexao para SQLAlchemy."""
    config = DB_CONFIG

    # Conexao local com Windows Authentication
    if CONNECTION_MODE == "local" and not config["username"]:
        # Remove chaves {} do driver para SQLAlchemy
        driver_name = config['driver'].replace('{', '').replace('}', '').replace(' ', '+')
        return (
            f"mssql+pyodbc://@{config['server']}/{config['database']}"
            f"?driver={driver_name}"
            f"&Trusted_Connection=yes"
            f"&TrustServerCertificate={config['trust_server_certificate']}"
        )

    # Conexao com usuario/senha (local ou Azure)
    driver_name = config['driver'].replace('{', '').replace('}', '').replace(' ', '+')
    return (
        f"mssql+pyodbc://{config['username']}:{config['password']}"
        f"@{config['server']}/{config['database']}"
        f"?driver={driver_name}"
        f"&Encrypt={config['encrypt']}"
        f"&TrustServerCertificate={config['trust_server_certificate']}"
    )


def get_pyodbc_connection_string():
    """Retorna a string de conexao para pyodbc."""
    config = DB_CONFIG

    # Conexao local com Windows Authentication
    if CONNECTION_MODE == "local" and not config["username"]:
        return (
            f"DRIVER={config['driver']};"
            f"SERVER={config['server']};"
            f"DATABASE={config['database']};"
            f"Trusted_Connection=yes;"
            f"TrustServerCertificate={config['trust_server_certificate']};"
        )

    # Conexao com usuario/senha
    return (
        f"DRIVER={config['driver']};"
        f"SERVER={config['server']};"
        f"DATABASE={config['database']};"
        f"UID={config['username']};"
        f"PWD={config['password']};"
        f"Encrypt={config['encrypt']};"
        f"TrustServerCertificate={config['trust_server_certificate']};"
    )

# =============================================================================
# MAPEAMENTO DE ARQUIVOS EXCEL
# =============================================================================
EXCEL_FILES = {
    # Arquivos da raiz
    "carteira": DATA_DIR / "carteira.xlsx",
    "ods": DATA_DIR / "ods.xlsx",
    "metaods": DATA_DIR / "metaods.xlsx",
    "de_para": DATA_DIR / "DE_PARA CARTEIRA TAXONOMIA E FRAMEWORK.xlsx",
    "tabela_social": DATA_DIR / "tabela_social_projects.xlsx",
    "status_meta_2030": DATA_DIR / "Status_Meta_2030_BV.xlsx",

    # Energia
    "energia_consolidado": ENERGIA_DIR / "energiaconsolidado.xlsx",
    "energia_renovavel": ENERGIA_DIR / "Energia_Renovavel_Eficiencia_Energetica.xlsx",
    "kpi_enel": ENERGIA_DIR / "Ampla" / "KPIs_Energia_Enel_2024.xlsx",
    "kpi_edp": ENERGIA_DIR / "EDP" / "KPIs_Energia_EDP_2024_Com_Totais.xlsx",
    "kpi_engie": ENERGIA_DIR / "ENGIE" / "KPIs_Energia_ENGIE_2024_ComExplicacao.xlsx",
    "kpi_isa": ENERGIA_DIR / "ISA" / "KPIs_ISA_2024_ComExplicacao.xlsx",
    "kpi_taesa": ENERGIA_DIR / "Taesa" / "KPIs_TAESA_2024.xlsx",
    "kpi_maz": ENERGIA_DIR / "Marlim Azul" / "KPIs_MAZ.xlsx",
    "kpi_eneva": ENERGIA_DIR / "PARNAIBA GERACAO E COMERCIALIZACAO DE ENERGIA SA" / "KPIs_Eneva_2024.xlsx",

    # Saneamento
    "indicadores_saneamento": SANEAMENTO_DIR / "Indicadores_ESG_Saneamento.xlsx",
    "carteira_saneamento": SANEAMENTO_DIR / "Carteira_Saneamento_4Regras (4).xlsx",

    # Saude
    "empresa_saude": SAUDE_DIR / "empresasaude.xlsx",
    "carteira_saude": SAUDE_DIR / "Saude_Servicos_Saude_BemEstar_Limpa.xlsx",
    "kpi_alianca": SAUDE_DIR / "Relatorios Buscados" / "Aliança Saude" / "KPIs_Allianca_Saude.xlsx",
    "kpi_onco": SAUDE_DIR / "Relatorios Buscados" / "onco clinicas" / "KPIs_Saude_Oncoclinicas.xlsx",

    # Educacao
    "educacao": EDUCACAO_DIR / "tabela_social_projects_cnpj_formatado.xlsx",

    # Inclusao Digital
    "inclusao_digital": INCLUSAO_DIR / "tabela_inclusao_digital (2).xlsx",
}

# =============================================================================
# ANO DE REFERENCIA PADRAO
# =============================================================================
ANO_REFERENCIA = 2024
