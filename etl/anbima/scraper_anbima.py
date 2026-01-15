"""
ANBIMA Data Scraper
===================
Script para coleta de dados de fundos ESG da ANBIMA

Fontes de dados:
- data.anbima.com.br - Portal de dados ANBIMA
- CVM (Dados publicos de fundos)

Autor: ETL Pipeline
Data: 2025
"""

import requests
import pandas as pd
import json
from datetime import datetime, timedelta
from typing import Optional, List, Dict
import time
import os
import logging

# Configuracao de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Diretorios
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DATA_DIR = os.path.join(BASE_DIR, 'data', 'anbima')
os.makedirs(DATA_DIR, exist_ok=True)


class AnbimaDataScraper:
    """
    Classe para scraping de dados da ANBIMA
    """

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'application/json, text/html',
            'Accept-Language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
        })

        # URLs base
        self.urls = {
            'anbima_data': 'https://data.anbima.com.br',
            'cvm_fundos': 'https://dados.cvm.gov.br/dados/FI/CAD/DADOS/',
            'cvm_diario': 'https://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/',
        }

    def get_fundos_cvm(self, ano: int = None, mes: int = None) -> pd.DataFrame:
        """
        Obtem dados de fundos da CVM (fonte publica)
        """
        if ano is None:
            ano = datetime.now().year
        if mes is None:
            mes = datetime.now().month

        logger.info(f"Buscando dados CVM para {ano}/{mes:02d}")

        # Arquivo de cadastro de fundos
        url_cad = f"{self.urls['cvm_fundos']}cad_fi.csv"

        try:
            df = pd.read_csv(url_cad, sep=';', encoding='latin-1')
            logger.info(f"Cadastro CVM: {len(df)} fundos encontrados")

            # Filtrar apenas fundos ativos
            if 'SIT' in df.columns:
                df = df[df['SIT'] == 'EM FUNCIONAMENTO NORMAL']

            return df
        except Exception as e:
            logger.error(f"Erro ao buscar cadastro CVM: {e}")
            return pd.DataFrame()

    def get_informe_diario_cvm(self, ano: int = None, mes: int = None) -> pd.DataFrame:
        """
        Obtem informes diarios de fundos da CVM
        """
        if ano is None:
            ano = datetime.now().year
        if mes is None:
            mes = datetime.now().month

        logger.info(f"Buscando informes diarios CVM para {ano}/{mes:02d}")

        url = f"{self.urls['cvm_diario']}inf_diario_fi_{ano}{mes:02d}.csv"

        try:
            df = pd.read_csv(url, sep=';', encoding='latin-1')
            logger.info(f"Informes diarios: {len(df)} registros encontrados")
            return df
        except Exception as e:
            logger.error(f"Erro ao buscar informes diarios CVM: {e}")
            return pd.DataFrame()

    def identificar_fundos_esg(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Identifica fundos ESG baseado em criterios ANBIMA
        """
        if df.empty:
            return df

        # Coluna de nome do fundo
        nome_col = None
        for col in ['DENOM_SOCIAL', 'NM_FANTASIA', 'NOME']:
            if col in df.columns:
                nome_col = col
                break

        if nome_col is None:
            logger.warning("Coluna de nome nao encontrada")
            return df

        # Termos que indicam fundo ESG/IS
        termos_is = [' IS ', ' IS$', 'INVESTIMENTO SUSTENTAVEL', 'SUSTENTAVEL']
        termos_esg = ['ESG', 'ASG', 'SUSTENTAB', 'VERDE', 'GREEN', 'SOCIAL',
                      'IMPACTO', 'CLIMATICO', 'RENOVAVEL', 'LIMPA']

        def classificar_esg(nome):
            if pd.isna(nome):
                return 'Convencional', False

            nome_upper = str(nome).upper()

            # Verificar se e fundo IS (sufixo IS)
            for termo in termos_is:
                if termo in nome_upper:
                    return 'IS - Investimento Sustentavel', True

            # Verificar se menciona ESG
            for termo in termos_esg:
                if termo in nome_upper:
                    return 'ESG Integrado', False

            return 'Convencional', False

        df['CategoriaESG'], df['SufixoIS'] = zip(*df[nome_col].apply(classificar_esg))

        # Estatisticas
        esg_counts = df['CategoriaESG'].value_counts()
        logger.info(f"Classificacao ESG:\n{esg_counts}")

        return df

    def identificar_foco_esg(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Identifica o foco ESG (Ambiental, Social, Governanca)
        """
        if df.empty:
            return df

        # Coluna de nome do fundo
        nome_col = None
        for col in ['DENOM_SOCIAL', 'NM_FANTASIA', 'NOME']:
            if col in df.columns:
                nome_col = col
                break

        if nome_col is None:
            return df

        # Termos por foco
        termos_ambiental = ['VERDE', 'GREEN', 'CLIMATICO', 'CLIMA', 'RENOVAVEL',
                           'ENERGIA LIMPA', 'SOLAR', 'EOLICA', 'CARBONO', 'CO2']
        termos_social = ['SOCIAL', 'IMPACTO', 'INCLUSAO', 'HABITACAO',
                        'EDUCACAO', 'SAUDE', 'DIVERSIDADE']
        termos_governanca = ['GOVERNANCA', 'GOVERNANCE', 'ETICA']

        def identificar_foco(nome):
            if pd.isna(nome):
                return None

            nome_upper = str(nome).upper()

            focos = []
            for termo in termos_ambiental:
                if termo in nome_upper:
                    focos.append('Ambiental')
                    break

            for termo in termos_social:
                if termo in nome_upper:
                    focos.append('Social')
                    break

            for termo in termos_governanca:
                if termo in nome_upper:
                    focos.append('Governanca')
                    break

            if len(focos) > 1:
                return 'Multi-tema'
            elif len(focos) == 1:
                return focos[0]
            else:
                return 'Ambiental'  # Default para fundos ESG e majoritariamente ambiental

        # Aplicar apenas para fundos ESG
        df['FocoESG'] = df.apply(
            lambda x: identificar_foco(x[nome_col]) if x['CategoriaESG'] != 'Convencional' else None,
            axis=1
        )

        return df

    def processar_dados_fundos(self, df_cadastro: pd.DataFrame,
                               df_diario: pd.DataFrame) -> pd.DataFrame:
        """
        Processa e combina dados de cadastro com dados diarios
        """
        if df_cadastro.empty:
            return df_diario

        # Identificar coluna CNPJ
        cnpj_col_cad = 'CNPJ_FUNDO' if 'CNPJ_FUNDO' in df_cadastro.columns else 'CNPJ'
        cnpj_col_dia = 'CNPJ_FUNDO' if 'CNPJ_FUNDO' in df_diario.columns else 'CNPJ'

        # Classificar fundos ESG
        df_cadastro = self.identificar_fundos_esg(df_cadastro)
        df_cadastro = self.identificar_foco_esg(df_cadastro)

        # Merge se tiver dados diarios
        if not df_diario.empty:
            df = pd.merge(
                df_diario,
                df_cadastro[[cnpj_col_cad, 'CategoriaESG', 'SufixoIS', 'FocoESG']].drop_duplicates(),
                left_on=cnpj_col_dia,
                right_on=cnpj_col_cad,
                how='left'
            )
            return df

        return df_cadastro

    def gerar_dados_exemplo(self) -> Dict[str, pd.DataFrame]:
        """
        Gera dados de exemplo para demonstracao do dashboard
        """
        logger.info("Gerando dados de exemplo...")

        # Gestoras
        gestoras = [
            {'GestoraID': 1, 'GestoraNome': 'BTG Pactual Asset Management', 'GestoraCNPJ': '11.455.086/0001-40'},
            {'GestoraID': 2, 'GestoraNome': 'Itau Asset Management', 'GestoraCNPJ': '62.144.175/0001-20'},
            {'GestoraID': 3, 'GestoraNome': 'BB Asset Management', 'GestoraCNPJ': '17.376.701/0001-70'},
            {'GestoraID': 4, 'GestoraNome': 'Bradesco Asset Management', 'GestoraCNPJ': '61.530.879/0001-23'},
            {'GestoraID': 5, 'GestoraNome': 'XP Asset Management', 'GestoraCNPJ': '24.312.505/0001-97'},
            {'GestoraID': 6, 'GestoraNome': 'JGP Asset Management', 'GestoraCNPJ': '39.240.618/0001-73'},
            {'GestoraID': 7, 'GestoraNome': 'Verde Asset Management', 'GestoraCNPJ': '08.439.605/0001-10'},
            {'GestoraID': 8, 'GestoraNome': 'Opportunity Asset Management', 'GestoraCNPJ': '09.274.350/0001-15'},
        ]
        df_gestoras = pd.DataFrame(gestoras)

        # Fundos ESG
        fundos = [
            # Fundos IS (Investimento Sustentavel)
            {'FundoCNPJ': '40.123.456/0001-01', 'FundoNome': 'BTG Pactual Credito Corporativo IS', 'GestoraID': 1,
             'CategoriaESG': 'IS - Investimento Sustentavel', 'FocoESG': 'Ambiental', 'TipoFundo': 'Renda Fixa',
             'PatrimonioLiquido': 5200000000, 'Cotistas': 12500, 'RentabilidadeAno': 12.5},
            {'FundoCNPJ': '40.123.456/0001-02', 'FundoNome': 'Itau Transicao Energetica IS', 'GestoraID': 2,
             'CategoriaESG': 'IS - Investimento Sustentavel', 'FocoESG': 'Ambiental', 'TipoFundo': 'Renda Variavel',
             'PatrimonioLiquido': 3800000000, 'Cotistas': 8900, 'RentabilidadeAno': 18.2},
            {'FundoCNPJ': '40.123.456/0001-03', 'FundoNome': 'BB Acoes Sustentabilidade IS', 'GestoraID': 3,
             'CategoriaESG': 'IS - Investimento Sustentavel', 'FocoESG': 'Multi-tema', 'TipoFundo': 'Renda Variavel',
             'PatrimonioLiquido': 2900000000, 'Cotistas': 15200, 'RentabilidadeAno': 15.8},
            {'FundoCNPJ': '40.123.456/0001-04', 'FundoNome': 'Bradesco FIA ISE IS', 'GestoraID': 4,
             'CategoriaESG': 'IS - Investimento Sustentavel', 'FocoESG': 'Multi-tema', 'TipoFundo': 'Renda Variavel',
             'PatrimonioLiquido': 4100000000, 'Cotistas': 22000, 'RentabilidadeAno': 14.3},
            {'FundoCNPJ': '40.123.456/0001-05', 'FundoNome': 'XP Debentures Incentivadas IS', 'GestoraID': 5,
             'CategoriaESG': 'IS - Investimento Sustentavel', 'FocoESG': 'Ambiental', 'TipoFundo': 'Renda Fixa',
             'PatrimonioLiquido': 6500000000, 'Cotistas': 35000, 'RentabilidadeAno': 11.9},
            {'FundoCNPJ': '40.123.456/0001-06', 'FundoNome': 'JGP Impacto Social IS', 'GestoraID': 6,
             'CategoriaESG': 'IS - Investimento Sustentavel', 'FocoESG': 'Social', 'TipoFundo': 'Multimercado',
             'PatrimonioLiquido': 1800000000, 'Cotistas': 4500, 'RentabilidadeAno': 13.7},

            # Fundos ESG Integrado
            {'FundoCNPJ': '40.123.456/0001-07', 'FundoNome': 'Verde AM ESG Selection', 'GestoraID': 7,
             'CategoriaESG': 'ESG Integrado', 'FocoESG': 'Multi-tema', 'TipoFundo': 'Multimercado',
             'PatrimonioLiquido': 2200000000, 'Cotistas': 6800, 'RentabilidadeAno': 16.1},
            {'FundoCNPJ': '40.123.456/0001-08', 'FundoNome': 'Opportunity ESG FIA', 'GestoraID': 8,
             'CategoriaESG': 'ESG Integrado', 'FocoESG': 'Ambiental', 'TipoFundo': 'Renda Variavel',
             'PatrimonioLiquido': 1500000000, 'Cotistas': 3200, 'RentabilidadeAno': 19.5},
            {'FundoCNPJ': '40.123.456/0001-09', 'FundoNome': 'BTG ESG Corporativo', 'GestoraID': 1,
             'CategoriaESG': 'ESG Integrado', 'FocoESG': 'Governanca', 'TipoFundo': 'Renda Fixa',
             'PatrimonioLiquido': 3100000000, 'Cotistas': 9500, 'RentabilidadeAno': 12.8},
            {'FundoCNPJ': '40.123.456/0001-10', 'FundoNome': 'Itau Governanca Responsavel', 'GestoraID': 2,
             'CategoriaESG': 'ESG Integrado', 'FocoESG': 'Governanca', 'TipoFundo': 'Renda Variavel',
             'PatrimonioLiquido': 2700000000, 'Cotistas': 7200, 'RentabilidadeAno': 17.3},
        ]
        df_fundos = pd.DataFrame(fundos)

        # Resumo Mensal ESG (ultimos 12 meses)
        resumo_mensal = []
        base_date = datetime(2025, 1, 1)
        pl_is = 24000000000  # 24 bi inicial
        pl_esg = 12000000000  # 12 bi inicial

        for i in range(12):
            mes = base_date - timedelta(days=30*i)
            ano_mes = mes.strftime('%Y-%m')

            # Crescimento mensal
            fator_is = 1 + (0.04 * (12-i)/12)  # Crescimento ao longo do ano
            fator_esg = 1 + (0.03 * (12-i)/12)

            resumo_mensal.append({
                'AnoMes': ano_mes,
                'Ano': mes.year,
                'Mes': mes.month,
                'CategoriaESG': 'IS - Investimento Sustentavel',
                'TotalFundos': 22 + i,
                'PatrimonioLiquidoTotal': pl_is / fator_is,
                'CaptacaoLiquidaTotal': 800000000 * (1 + i*0.05),
                'TotalCotistas': 150000 - (i * 5000),
            })
            resumo_mensal.append({
                'AnoMes': ano_mes,
                'Ano': mes.year,
                'Mes': mes.month,
                'CategoriaESG': 'ESG Integrado',
                'TotalFundos': 45 + i*2,
                'PatrimonioLiquidoTotal': pl_esg / fator_esg,
                'CaptacaoLiquidaTotal': 400000000 * (1 + i*0.03),
                'TotalCotistas': 80000 - (i * 2000),
            })

        df_resumo = pd.DataFrame(resumo_mensal)

        # Indicadores de Mercado (ISE, ICO2)
        indicadores = []
        for i in range(365):
            data = base_date - timedelta(days=i)
            if data.weekday() < 5:  # Apenas dias uteis
                indicadores.append({
                    'Indice': 'ISE',
                    'Data': data.strftime('%Y-%m-%d'),
                    'ValorFechamento': 4500 + (365-i) * 2.5 + (i % 30) * 10,
                    'VariacaoDia': round((0.5 - (i % 10) * 0.1), 4)
                })
                indicadores.append({
                    'Indice': 'ICO2',
                    'Data': data.strftime('%Y-%m-%d'),
                    'ValorFechamento': 3200 + (365-i) * 1.8 + (i % 20) * 8,
                    'VariacaoDia': round((0.3 - (i % 8) * 0.08), 4)
                })
                indicadores.append({
                    'Indice': 'IBOV',
                    'Data': data.strftime('%Y-%m-%d'),
                    'ValorFechamento': 125000 + (365-i) * 50 + (i % 50) * 100,
                    'VariacaoDia': round((0.8 - (i % 15) * 0.1), 4)
                })

        df_indicadores = pd.DataFrame(indicadores)

        return {
            'gestoras': df_gestoras,
            'fundos': df_fundos,
            'resumo_mensal': df_resumo,
            'indicadores': df_indicadores
        }

    def salvar_dados(self, dados: Dict[str, pd.DataFrame], formato: str = 'csv'):
        """
        Salva os dados coletados em arquivos
        """
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

        for nome, df in dados.items():
            if df.empty:
                continue

            if formato == 'csv':
                filepath = os.path.join(DATA_DIR, f'{nome}_{timestamp}.csv')
                df.to_csv(filepath, index=False, encoding='utf-8-sig')
            elif formato == 'json':
                filepath = os.path.join(DATA_DIR, f'{nome}_{timestamp}.json')
                df.to_json(filepath, orient='records', force_ascii=False, indent=2)
            elif formato == 'excel':
                filepath = os.path.join(DATA_DIR, f'{nome}_{timestamp}.xlsx')
                df.to_excel(filepath, index=False)

            logger.info(f"Dados salvos: {filepath}")

    def executar_coleta_completa(self):
        """
        Executa coleta completa de dados
        """
        logger.info("=" * 60)
        logger.info("INICIANDO COLETA DE DADOS ANBIMA/CVM")
        logger.info("=" * 60)

        # Tentar dados reais da CVM
        df_cadastro = self.get_fundos_cvm()
        df_diario = self.get_informe_diario_cvm()

        if not df_cadastro.empty:
            df_processado = self.processar_dados_fundos(df_cadastro, df_diario)

            # Filtrar apenas fundos ESG
            df_esg = df_processado[df_processado['CategoriaESG'] != 'Convencional']
            logger.info(f"Total de fundos ESG identificados: {len(df_esg)}")

            self.salvar_dados({
                'cadastro_fundos': df_cadastro,
                'fundos_esg': df_esg,
                'informes_diarios': df_diario
            })
        else:
            logger.warning("Nao foi possivel obter dados da CVM, gerando dados de exemplo")

        # Sempre gerar dados de exemplo para o dashboard
        dados_exemplo = self.gerar_dados_exemplo()
        self.salvar_dados(dados_exemplo)

        logger.info("=" * 60)
        logger.info("COLETA FINALIZADA")
        logger.info("=" * 60)

        return dados_exemplo


def main():
    """
    Funcao principal
    """
    scraper = AnbimaDataScraper()
    dados = scraper.executar_coleta_completa()

    # Resumo
    print("\n" + "=" * 60)
    print("RESUMO DOS DADOS COLETADOS")
    print("=" * 60)

    for nome, df in dados.items():
        print(f"\n{nome.upper()}:")
        print(f"  - Registros: {len(df)}")
        print(f"  - Colunas: {list(df.columns)[:5]}...")


if __name__ == '__main__':
    main()
