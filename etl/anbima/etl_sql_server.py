"""
ETL - Carga de Dados no SQL Server
==================================
Script para carregar dados da ANBIMA no banco SQL Server

Requisitos:
- pyodbc
- pandas
- sqlalchemy
"""

import pandas as pd
import pyodbc
from sqlalchemy import create_engine
import os
import logging
from datetime import datetime
from typing import Dict, Optional

# Configuracao
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Diretorios
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DATA_DIR = os.path.join(BASE_DIR, 'data', 'anbima')


class SQLServerLoader:
    """
    Classe para carga de dados no SQL Server
    """

    def __init__(self, server: str = 'localhost', database: str = 'ANBIMA_ESG',
                 trusted_connection: bool = True, username: str = None, password: str = None):
        """
        Inicializa conexao com SQL Server

        Args:
            server: Nome do servidor SQL
            database: Nome do banco de dados
            trusted_connection: Usar autenticacao Windows
            username: Usuario SQL (se trusted_connection=False)
            password: Senha SQL (se trusted_connection=False)
        """
        self.server = server
        self.database = database

        if trusted_connection:
            self.connection_string = (
                f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                f"SERVER={server};"
                f"DATABASE={database};"
                f"Trusted_Connection=yes;"
            )
        else:
            self.connection_string = (
                f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                f"SERVER={server};"
                f"DATABASE={database};"
                f"UID={username};"
                f"PWD={password};"
            )

        self.engine = None

    def conectar(self) -> bool:
        """
        Estabelece conexao com o banco
        """
        try:
            conn = pyodbc.connect(self.connection_string)
            conn.close()

            # Criar engine SQLAlchemy
            conn_str_sqlalchemy = (
                f"mssql+pyodbc://{self.server}/{self.database}"
                f"?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes"
            )
            self.engine = create_engine(conn_str_sqlalchemy)

            logger.info(f"Conectado ao SQL Server: {self.server}/{self.database}")
            return True
        except Exception as e:
            logger.error(f"Erro ao conectar: {e}")
            return False

    def executar_sql(self, sql: str) -> bool:
        """
        Executa um comando SQL
        """
        try:
            conn = pyodbc.connect(self.connection_string)
            cursor = conn.cursor()
            cursor.execute(sql)
            conn.commit()
            cursor.close()
            conn.close()
            return True
        except Exception as e:
            logger.error(f"Erro SQL: {e}")
            return False

    def carregar_gestoras(self, df: pd.DataFrame) -> int:
        """
        Carrega dados de gestoras
        """
        logger.info("Carregando gestoras...")

        sql_insert = """
        INSERT INTO fundos.DimGestora (GestoraNome, GestoraCNPJ)
        SELECT ?, ?
        WHERE NOT EXISTS (
            SELECT 1 FROM fundos.DimGestora WHERE GestoraCNPJ = ?
        )
        """

        count = 0
        try:
            conn = pyodbc.connect(self.connection_string)
            cursor = conn.cursor()

            for _, row in df.iterrows():
                cursor.execute(sql_insert, (
                    row['GestoraNome'],
                    row.get('GestoraCNPJ', ''),
                    row.get('GestoraCNPJ', '')
                ))
                if cursor.rowcount > 0:
                    count += 1

            conn.commit()
            cursor.close()
            conn.close()

            logger.info(f"Gestoras inseridas: {count}")
            return count
        except Exception as e:
            logger.error(f"Erro ao carregar gestoras: {e}")
            return 0

    def carregar_fundos(self, df: pd.DataFrame) -> int:
        """
        Carrega dados de fundos
        """
        logger.info("Carregando fundos...")

        # Primeiro, buscar IDs das gestoras e categorias
        sql_get_gestora = "SELECT GestoraID, GestoraNome FROM fundos.DimGestora"
        sql_get_categoria = "SELECT CategoriaESGID, CategoriaNome FROM esg.DimCategoriaESG"
        sql_get_foco = "SELECT FocoESGID, FocoNome FROM esg.DimFocoESG"

        try:
            conn = pyodbc.connect(self.connection_string)

            # Mapear gestoras
            df_gestoras = pd.read_sql(sql_get_gestora, conn)
            gestora_map = dict(zip(df_gestoras['GestoraNome'], df_gestoras['GestoraID']))

            # Mapear categorias ESG
            df_categorias = pd.read_sql(sql_get_categoria, conn)
            categoria_map = dict(zip(df_categorias['CategoriaNome'], df_categorias['CategoriaESGID']))

            # Mapear focos ESG
            df_focos = pd.read_sql(sql_get_foco, conn)
            foco_map = dict(zip(df_focos['FocoNome'], df_focos['FocoESGID']))

            cursor = conn.cursor()
            count = 0

            for _, row in df.iterrows():
                # Buscar IDs
                gestora_id = gestora_map.get(row.get('GestoraNome'))
                categoria_id = categoria_map.get(row.get('CategoriaESG'))
                foco_id = foco_map.get(row.get('FocoESG'))

                sql_insert = """
                INSERT INTO fundos.FatoFundo (
                    FundoCNPJ, FundoNome, GestoraID, CategoriaESGID, FocoESGID,
                    SufixoIS, ESGIntegrado
                )
                SELECT ?, ?, ?, ?, ?, ?, ?
                WHERE NOT EXISTS (
                    SELECT 1 FROM fundos.FatoFundo WHERE FundoCNPJ = ?
                )
                """

                sufixo_is = 1 if row.get('CategoriaESG', '').startswith('IS') else 0
                esg_integrado = 1 if 'ESG' in row.get('CategoriaESG', '') else 0

                cursor.execute(sql_insert, (
                    row['FundoCNPJ'],
                    row['FundoNome'],
                    gestora_id,
                    categoria_id,
                    foco_id,
                    sufixo_is,
                    esg_integrado,
                    row['FundoCNPJ']
                ))

                if cursor.rowcount > 0:
                    count += 1

            conn.commit()
            cursor.close()
            conn.close()

            logger.info(f"Fundos inseridos: {count}")
            return count
        except Exception as e:
            logger.error(f"Erro ao carregar fundos: {e}")
            return 0

    def carregar_resumo_mensal(self, df: pd.DataFrame) -> int:
        """
        Carrega resumo mensal ESG
        """
        logger.info("Carregando resumo mensal...")

        try:
            conn = pyodbc.connect(self.connection_string)

            # Mapear categorias
            sql_get_categoria = "SELECT CategoriaESGID, CategoriaNome FROM esg.DimCategoriaESG"
            df_categorias = pd.read_sql(sql_get_categoria, conn)
            categoria_map = dict(zip(df_categorias['CategoriaNome'], df_categorias['CategoriaESGID']))

            cursor = conn.cursor()
            count = 0

            for _, row in df.iterrows():
                categoria_id = categoria_map.get(row.get('CategoriaESG'))

                sql_insert = """
                INSERT INTO esg.FatoResumoMensalESG (
                    AnoMes, Ano, Mes, CategoriaESGID,
                    TotalFundos, PatrimonioLiquidoTotal, CaptacaoLiquidaTotal, TotalCotistas
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """

                cursor.execute(sql_insert, (
                    row['AnoMes'],
                    row['Ano'],
                    row['Mes'],
                    categoria_id,
                    row.get('TotalFundos', 0),
                    row.get('PatrimonioLiquidoTotal', 0),
                    row.get('CaptacaoLiquidaTotal', 0),
                    row.get('TotalCotistas', 0)
                ))
                count += 1

            conn.commit()
            cursor.close()
            conn.close()

            logger.info(f"Registros mensais inseridos: {count}")
            return count
        except Exception as e:
            logger.error(f"Erro ao carregar resumo mensal: {e}")
            return 0

    def carregar_dados_completos(self, dados: Dict[str, pd.DataFrame]) -> Dict[str, int]:
        """
        Carrega todos os dados no banco
        """
        resultado = {}

        if 'gestoras' in dados:
            resultado['gestoras'] = self.carregar_gestoras(dados['gestoras'])

        if 'fundos' in dados:
            resultado['fundos'] = self.carregar_fundos(dados['fundos'])

        if 'resumo_mensal' in dados:
            resultado['resumo_mensal'] = self.carregar_resumo_mensal(dados['resumo_mensal'])

        return resultado


def carregar_dados_csv(pasta: str = None) -> Dict[str, pd.DataFrame]:
    """
    Carrega dados dos arquivos CSV mais recentes
    """
    if pasta is None:
        pasta = DATA_DIR

    dados = {}

    # Encontrar arquivos mais recentes
    arquivos = {
        'gestoras': None,
        'fundos': None,
        'resumo_mensal': None,
        'indicadores': None
    }

    for arquivo in os.listdir(pasta):
        if arquivo.endswith('.csv'):
            for tipo in arquivos.keys():
                if tipo in arquivo.lower():
                    filepath = os.path.join(pasta, arquivo)
                    if arquivos[tipo] is None or os.path.getmtime(filepath) > os.path.getmtime(arquivos[tipo]):
                        arquivos[tipo] = filepath

    # Carregar DataFrames
    for tipo, filepath in arquivos.items():
        if filepath and os.path.exists(filepath):
            dados[tipo] = pd.read_csv(filepath, encoding='utf-8-sig')
            logger.info(f"Carregado {tipo}: {len(dados[tipo])} registros")

    return dados


def carregar_dados_json(arquivo_json: str = None) -> Dict[str, pd.DataFrame]:
    """
    Carrega dados do arquivo JSON do dashboard para DataFrames
    """
    import json

    if arquivo_json is None:
        arquivo_json = os.path.join(DATA_DIR, 'dados_dashboard.json')

    if not os.path.exists(arquivo_json):
        logger.error(f"Arquivo JSON nao encontrado: {arquivo_json}")
        return {}

    with open(arquivo_json, 'r', encoding='utf-8') as f:
        dados_json = json.load(f)

    dados = {}

    # Extrair gestoras unicas
    gestoras_set = set()
    for fundo in dados_json.get('fundos_is', []) + dados_json.get('fundos_esg', []):
        gestora = fundo.get('Gestora')
        if gestora:
            gestoras_set.add(gestora)

    if gestoras_set:
        dados['gestoras'] = pd.DataFrame([{'GestoraNome': g, 'GestoraCNPJ': ''} for g in gestoras_set])
        logger.info(f"Extraidas {len(dados['gestoras'])} gestoras do JSON")

    # Converter fundos IS
    fundos_list = []
    for fundo in dados_json.get('fundos_is', []):
        cnpj = str(fundo.get('identificador_fundo', '')).zfill(14)
        cnpj_formatado = f"{cnpj[:2]}.{cnpj[2:5]}.{cnpj[5:8]}/{cnpj[8:12]}-{cnpj[12:14]}"
        fundos_list.append({
            'FundoCNPJ': cnpj_formatado,
            'FundoNome': fundo.get('razao_social_fundo', ''),
            'GestoraNome': fundo.get('Gestora', ''),
            'CategoriaESG': 'IS - Investimento Sustentavel',
            'FocoESG': fundo.get('FocoESG', 'Multi-tema'),
            'TipoAtivo': fundo.get('TipoAtivo', '')
        })

    # Converter fundos ESG Integrado
    for fundo in dados_json.get('fundos_esg', []):
        cnpj = str(fundo.get('identificador_fundo', '')).zfill(14)
        cnpj_formatado = f"{cnpj[:2]}.{cnpj[2:5]}.{cnpj[5:8]}/{cnpj[8:12]}-{cnpj[12:14]}"
        fundos_list.append({
            'FundoCNPJ': cnpj_formatado,
            'FundoNome': fundo.get('razao_social_fundo', ''),
            'GestoraNome': fundo.get('Gestora', ''),
            'CategoriaESG': 'ESG Integrado',
            'FocoESG': fundo.get('FocoESG', 'Multi-tema'),
            'TipoAtivo': fundo.get('TipoAtivo', '')
        })

    if fundos_list:
        dados['fundos'] = pd.DataFrame(fundos_list)
        logger.info(f"Convertidos {len(dados['fundos'])} fundos do JSON")

    # Criar resumo mensal a partir dos dados agregados
    data_atualizacao = dados_json.get('data_atualizacao', datetime.now().strftime('%d/%m/%Y'))
    try:
        data = datetime.strptime(data_atualizacao.split()[0], '%d/%m/%Y')
    except:
        data = datetime.now()

    ano_mes = data.strftime('%Y-%m')

    resumo_list = []
    for categoria, total in dados_json.get('por_categoria', {}).items():
        resumo_list.append({
            'AnoMes': ano_mes,
            'Ano': data.year,
            'Mes': data.month,
            'CategoriaESG': categoria,
            'TotalFundos': total,
            'PatrimonioLiquidoTotal': 0,
            'CaptacaoLiquidaTotal': 0,
            'TotalCotistas': 0
        })

    if resumo_list:
        dados['resumo_mensal'] = pd.DataFrame(resumo_list)
        logger.info(f"Criados {len(dados['resumo_mensal'])} registros de resumo mensal")

    return dados


def main():
    """
    Funcao principal
    """
    print("=" * 60)
    print("ETL - CARGA DE DADOS ANBIMA NO SQL SERVER")
    print("=" * 60)

    # Configuracao do servidor
    server = input("Servidor SQL (default: localhost): ").strip() or 'localhost'
    database = 'ANBIMA_ESG'

    loader = SQLServerLoader(server=server, database=database)

    if not loader.conectar():
        print("\nERRO: Nao foi possivel conectar ao banco de dados.")
        print("Verifique se:")
        print("  1. O SQL Server esta rodando")
        print("  2. O banco ANBIMA_ESG foi criado (execute os scripts SQL)")
        print("  3. O ODBC Driver 17 for SQL Server esta instalado")
        return

    # Escolher fonte de dados
    print("\nFonte de dados:")
    print("  1. JSON (dados_dashboard.json)")
    print("  2. CSV (arquivos CSV)")
    fonte = input("Escolha (1 ou 2, default: 1): ").strip() or '1'

    if fonte == '1':
        dados = carregar_dados_json()
    else:
        dados = carregar_dados_csv()

    if not dados:
        print("\nNenhum dado encontrado para carregar.")
        print("Verifique se os arquivos existem em data/anbima/")
        return

    # Executar carga
    resultado = loader.carregar_dados_completos(dados)

    # Resumo
    print("\n" + "=" * 60)
    print("RESULTADO DA CARGA")
    print("=" * 60)
    for tabela, count in resultado.items():
        print(f"  {tabela}: {count} registros")


if __name__ == '__main__':
    main()
