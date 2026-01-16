"""
Configuracoes do ETL - Modelagem ESG Banco Votorantim
Suporta conexao LOCAL e CLOUD (PostgreSQL)
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
# MODO DE CONEXAO: "local" ou "cloud"
# =============================================================================
CONNECTION_MODE = os.getenv("DB_MODE", "local")

# =============================================================================
# CONFIGURACAO LOCAL (PostgreSQL)
# =============================================================================
LOCAL_CONFIG = {
    "host": os.getenv("PG_HOST", "localhost"),
    "port": os.getenv("PG_PORT", "5432"),
    "database": os.getenv("PG_DATABASE", "esg_bv"),
    "username": os.getenv("PG_USER", "postgres"),
    "password": os.getenv("PG_PASSWORD", ""),
}

# =============================================================================
# CONFIGURACAO CLOUD (PostgreSQL - ex: Render, Railway, Supabase, Neon)
# =============================================================================
CLOUD_CONFIG = {
    "host": os.getenv("CLOUD_PG_HOST", ""),
    "port": os.getenv("CLOUD_PG_PORT", "5432"),
    "database": os.getenv("CLOUD_PG_DATABASE", "esg_bv"),
    "username": os.getenv("CLOUD_PG_USER", ""),
    "password": os.getenv("CLOUD_PG_PASSWORD", ""),
    "sslmode": os.getenv("CLOUD_PG_SSLMODE", "require"),
}

# =============================================================================
# SELECAO AUTOMATICA DE CONFIGURACAO
# =============================================================================
DB_CONFIG = LOCAL_CONFIG if CONNECTION_MODE == "local" else CLOUD_CONFIG

# =============================================================================
# FUNCOES DE CONEXAO
# =============================================================================

def get_connection_string():
    """Retorna a string de conexao para SQLAlchemy (PostgreSQL)."""
    config = DB_CONFIG

    base_url = (
        f"postgresql+psycopg2://{config['username']}:{config['password']}"
        f"@{config['host']}:{config['port']}/{config['database']}"
    )

    # Adiciona SSL para conexao cloud
    if CONNECTION_MODE == "cloud" and config.get("sslmode"):
        base_url += f"?sslmode={config['sslmode']}"

    return base_url


def get_psycopg2_connection_params():
    """Retorna os parametros de conexao para psycopg2."""
    config = DB_CONFIG

    params = {
        "host": config['host'],
        "port": config['port'],
        "dbname": config['database'],
        "user": config['username'],
        "password": config['password'],
    }

    # Adiciona SSL para conexao cloud
    if CONNECTION_MODE == "cloud" and config.get("sslmode"):
        params["sslmode"] = config['sslmode']

    return params

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
