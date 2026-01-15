"""
ANBIMA API Scraper - Dados Reais
================================
Script para coleta de dados REAIS de fundos ESG via API oficial ANBIMA

Usa autenticacao OAuth2 com credenciais oficiais.
NAO GERA DADOS INVENTADOS - apenas dados reais da API.

Autor: ETL Pipeline
Data: 2026
"""

import requests
import pandas as pd
import json
import base64
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any
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


class AnbimaAPIClient:
    """
    Cliente oficial da API ANBIMA usando OAuth2
    """

    def __init__(self, client_id: str, client_secret: str):
        """
        Inicializa o cliente com credenciais OAuth2

        Args:
            client_id: ID do cliente ANBIMA
            client_secret: Secret do cliente ANBIMA
        """
        self.client_id = client_id
        self.client_secret = client_secret
        self.access_token = None
        self.token_expires_at = None

        # URLs da API ANBIMA
        self.base_url = "https://api.anbima.com.br"
        self.sandbox_url = "https://api-sandbox.anbima.com.br"
        self.token_url = f"{self.base_url}/oauth/access-token"

        # Session com headers padrao
        self.session = requests.Session()
        self.session.headers.update({
            'Accept': 'application/json',
            'Content-Type': 'application/json',
        })

    def _get_basic_auth(self) -> str:
        """Gera header Basic Auth em base64"""
        credentials = f"{self.client_id}:{self.client_secret}"
        encoded = base64.b64encode(credentials.encode('utf-8')).decode('utf-8')
        return f"Basic {encoded}"

    def authenticate(self) -> bool:
        """
        Autentica na API ANBIMA via OAuth2

        Returns:
            bool: True se autenticacao foi bem sucedida
        """
        logger.info("Autenticando na API ANBIMA...")

        headers = {
            'Authorization': self._get_basic_auth(),
            'Content-Type': 'application/json',
        }

        payload = {
            'grant_type': 'client_credentials'
        }

        try:
            response = self.session.post(
                self.token_url,
                headers=headers,
                json=payload,
                timeout=30
            )

            logger.info(f"Status autenticacao: {response.status_code}")

            # Status 200 ou 201 sao sucesso
            if response.status_code in [200, 201]:
                data = response.json()
                self.access_token = data.get('access_token')
                expires_in = data.get('expires_in', 3600)
                self.token_expires_at = datetime.now() + timedelta(seconds=expires_in)

                # Atualizar headers da sessao (API requer client_id no header)
                self.session.headers.update({
                    'Authorization': f'Bearer {self.access_token}',
                    'client_id': self.client_id,
                    'access_token': self.access_token
                })

                logger.info(f"Autenticacao bem sucedida! Token: {self.access_token[:8]}... expira em {expires_in}s")
                return True
            else:
                logger.error(f"Erro na autenticacao: {response.status_code}")
                logger.error(f"Resposta: {response.text}")
                return False

        except Exception as e:
            logger.error(f"Excecao na autenticacao: {e}")
            return False

    def _ensure_authenticated(self):
        """Garante que temos um token valido"""
        if self.access_token is None or \
           (self.token_expires_at and datetime.now() >= self.token_expires_at):
            if not self.authenticate():
                raise Exception("Falha na autenticacao com a API ANBIMA")

    def _api_request(self, endpoint: str, params: dict = None, use_sandbox: bool = False) -> Optional[Dict]:
        """
        Faz requisicao para a API ANBIMA

        Args:
            endpoint: Caminho do endpoint (ex: /feed/fundos/v1/fundos)
            params: Parametros de query string
            use_sandbox: Se True, usa ambiente sandbox

        Returns:
            Dict com resposta da API ou None em caso de erro
        """
        self._ensure_authenticated()

        base = self.sandbox_url if use_sandbox else self.base_url
        url = f"{base}{endpoint}"

        try:
            logger.info(f"Requisicao: GET {url}")
            response = self.session.get(url, params=params, timeout=60)

            logger.info(f"Status: {response.status_code}")

            if response.status_code == 200:
                return response.json()
            elif response.status_code == 401:
                # Token expirado, tentar reautenticar
                logger.warning("Token expirado, reautenticando...")
                self.access_token = None
                self._ensure_authenticated()
                response = self.session.get(url, params=params, timeout=60)
                if response.status_code == 200:
                    return response.json()

            logger.error(f"Erro na API: {response.status_code} - {response.text}")
            return None

        except Exception as e:
            logger.error(f"Excecao na requisicao: {e}")
            return None

    def get_fundos(self, page: int = 1, page_size: int = 100, use_sandbox: bool = True) -> Optional[Dict]:
        """
        Obtem lista de fundos

        Args:
            page: Numero da pagina
            page_size: Quantidade de registros por pagina
            use_sandbox: Usar ambiente sandbox
        """
        endpoint = "/feed/fundos/v1/fundos"
        params = {
            'page': page,
            'page_size': page_size
        }
        return self._api_request(endpoint, params, use_sandbox=use_sandbox)

    def get_fundos_v2(self, page: int = 1, page_size: int = 100, use_sandbox: bool = True) -> Optional[Dict]:
        """
        Obtem lista de fundos (API v2 - RCVM 175)
        """
        endpoint = "/feed/fundos/v2/fundos"
        params = {
            'page': page,
            'page_size': page_size
        }
        return self._api_request(endpoint, params, use_sandbox=use_sandbox)

    def get_fundos_beta(self, page: int = 1, page_size: int = 100) -> Optional[Dict]:
        """
        Obtem lista de fundos via API Beta (sandbox)
        """
        endpoint = "/beta/feed/fundos/v2/fundos"
        params = {
            'page': page,
            'page_size': page_size
        }
        return self._api_request(endpoint, params, use_sandbox=True)

    def get_fundo_detalhes(self, cnpj: str, use_sandbox: bool = True) -> Optional[Dict]:
        """
        Obtem detalhes de um fundo especifico

        Args:
            cnpj: CNPJ do fundo (apenas numeros)
            use_sandbox: Usar ambiente sandbox
        """
        cnpj_limpo = ''.join(filter(str.isdigit, cnpj))
        endpoint = f"/feed/fundos/v1/fundos/{cnpj_limpo}"
        return self._api_request(endpoint, use_sandbox=use_sandbox)

    def get_fundo_historico(self, cnpj: str, data_inicio: str = None, data_fim: str = None, use_sandbox: bool = True) -> Optional[Dict]:
        """
        Obtem historico de cotas de um fundo

        Args:
            cnpj: CNPJ do fundo
            data_inicio: Data inicial (YYYY-MM-DD)
            data_fim: Data final (YYYY-MM-DD)
            use_sandbox: Usar ambiente sandbox
        """
        cnpj_limpo = ''.join(filter(str.isdigit, cnpj))
        endpoint = f"/feed/fundos/v1/fundos/{cnpj_limpo}/historico"
        params = {}
        if data_inicio:
            params['data_inicio'] = data_inicio
        if data_fim:
            params['data_fim'] = data_fim
        return self._api_request(endpoint, params, use_sandbox=use_sandbox)

    def get_instituicoes(self, use_sandbox: bool = True) -> Optional[Dict]:
        """Obtem lista de instituicoes/gestoras"""
        endpoint = "/feed/fundos/v1/fundos/instituicoes"
        return self._api_request(endpoint, use_sandbox=use_sandbox)

    def get_classes_anbima(self, use_sandbox: bool = True) -> Optional[Dict]:
        """Obtem lista de classes ANBIMA"""
        endpoint = "/feed/fundos/v1/fundos/classes-anbima"
        return self._api_request(endpoint, use_sandbox=use_sandbox)

    def get_indices(self, indice: str = None, use_sandbox: bool = True) -> Optional[Dict]:
        """
        Obtem dados de indices (ISE, ICO2, etc)

        Args:
            indice: Codigo do indice (opcional)
            use_sandbox: Usar ambiente sandbox
        """
        endpoint = "/feed/precos-indices/v1/indices"
        if indice:
            endpoint = f"{endpoint}/{indice}"
        return self._api_request(endpoint, use_sandbox=use_sandbox)

    def testar_endpoints(self) -> Dict[str, bool]:
        """
        Testa todos os endpoints disponiveis para ver quais funcionam
        """
        logger.info("Testando endpoints disponiveis...")

        endpoints_para_testar = [
            # Sandbox endpoints
            ("/feed/fundos/v1/fundos", True),
            ("/feed/fundos/v2/fundos", True),
            ("/beta/feed/fundos/v2/fundos", True),
            ("/feed/fundos/v1/fundos/instituicoes", True),
            ("/feed/fundos/v1/fundos/classes-anbima", True),
            ("/feed/precos-indices/v1/indices", True),
            # Production endpoints
            ("/feed/fundos/v1/fundos", False),
            ("/feed/fundos/v2/fundos", False),
        ]

        resultados = {}

        for endpoint, sandbox in endpoints_para_testar:
            env = "sandbox" if sandbox else "prod"
            key = f"{env}:{endpoint}"

            try:
                resultado = self._api_request(endpoint, use_sandbox=sandbox)
                if resultado is not None:
                    resultados[key] = True
                    logger.info(f"OK: {key}")
                else:
                    resultados[key] = False
                    logger.warning(f"FALHA: {key}")
            except Exception as e:
                resultados[key] = False
                logger.error(f"ERRO: {key} - {e}")

            time.sleep(0.3)

        return resultados


class AnbimaDataCollector:
    """
    Coletor de dados reais da ANBIMA
    """

    def __init__(self, client_id: str, client_secret: str):
        self.api = AnbimaAPIClient(client_id, client_secret)
        self.dados_coletados = {}

    def coletar_todos_fundos(self, max_pages: int = 100) -> pd.DataFrame:
        """
        Coleta todos os fundos disponiveis na API

        Args:
            max_pages: Numero maximo de paginas para coletar

        Returns:
            DataFrame com todos os fundos
        """
        logger.info("Coletando todos os fundos da API ANBIMA...")

        todos_fundos = []
        page = 1

        # Primeiro, testar qual endpoint funciona
        logger.info("Testando endpoints disponiveis...")

        # Tentar diferentes endpoints
        endpoints_tentativas = [
            ("sandbox v1", lambda p: self.api.get_fundos(page=p, page_size=100, use_sandbox=True)),
            ("sandbox v2", lambda p: self.api.get_fundos_v2(page=p, page_size=100, use_sandbox=True)),
            ("sandbox beta", lambda p: self.api.get_fundos_beta(page=p, page_size=100)),
            ("prod v1", lambda p: self.api.get_fundos(page=p, page_size=100, use_sandbox=False)),
            ("prod v2", lambda p: self.api.get_fundos_v2(page=p, page_size=100, use_sandbox=False)),
        ]

        get_fundos_func = None
        endpoint_nome = None

        for nome, func in endpoints_tentativas:
            logger.info(f"Tentando endpoint: {nome}...")
            try:
                resultado = func(1)
                if resultado is not None:
                    dados = resultado.get('content', resultado.get('data', resultado.get('fundos', resultado.get('items', []))))
                    if dados:
                        logger.info(f"Endpoint {nome} funcionou! {len(dados)} fundos na primeira pagina")
                        get_fundos_func = func
                        endpoint_nome = nome
                        todos_fundos.extend(dados)
                        break
                    else:
                        logger.info(f"Endpoint {nome} retornou resposta mas sem dados")
            except Exception as e:
                logger.warning(f"Endpoint {nome} falhou: {e}")
            time.sleep(0.3)

        if get_fundos_func is None:
            logger.error("Nenhum endpoint de fundos disponivel")
            return pd.DataFrame()

        # Continuar coletando paginas
        page = 2
        while page <= max_pages:
            logger.info(f"Coletando pagina {page} via {endpoint_nome}...")

            try:
                resultado = get_fundos_func(page)

                if resultado is None:
                    logger.warning(f"Nenhum dado retornado na pagina {page}")
                    break

                # Extrair fundos do resultado (API ANBIMA usa 'content')
                fundos = resultado.get('content', resultado.get('data', resultado.get('fundos', resultado.get('items', []))))

                if not fundos:
                    logger.info(f"Fim dos dados na pagina {page}")
                    break

                todos_fundos.extend(fundos)
                logger.info(f"Pagina {page}: {len(fundos)} fundos coletados (total: {len(todos_fundos)})")

                # Verificar se ha mais paginas
                total_pages = resultado.get('total_pages', resultado.get('totalPages', max_pages))
                if page >= total_pages:
                    break

                page += 1
                time.sleep(0.5)  # Rate limiting

            except Exception as e:
                logger.error(f"Erro na pagina {page}: {e}")
                break

        if todos_fundos:
            df = pd.DataFrame(todos_fundos)
            logger.info(f"Total de fundos coletados: {len(df)}")
            return df

        return pd.DataFrame()

    def filtrar_fundos_esg(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Filtra apenas fundos ESG/IS do DataFrame

        Criterios ANBIMA:
        - Fundos IS: tem sufixo "IS" no nome (Investimento Sustentavel)
        - Fundos ESG Integrado: integram questoes ESG na gestao
        """
        if df.empty:
            return df

        logger.info("Filtrando fundos ESG...")
        logger.info(f"Colunas disponiveis: {df.columns.tolist()}")

        # Identificar coluna de nome (API ANBIMA usa razao_social_fundo ou nome_comercial_fundo)
        nome_cols = ['razao_social_fundo', 'nome_comercial_fundo', 'razao_social', 'nome', 'nome_fundo',
                     'denominacao', 'DENOM_SOCIAL', 'NM_FANTASIA']
        nome_col = None
        for col in nome_cols:
            if col in df.columns:
                nome_col = col
                break

        if nome_col is None:
            logger.warning(f"Coluna de nome nao encontrada. Colunas disponiveis: {df.columns.tolist()}")
            return df

        logger.info(f"Usando coluna de nome: {nome_col}")

        # Termos para identificar fundos ESG
        termos_is = [' IS', ' IS ', 'INVESTIMENTO SUSTENTAVEL', 'SUSTENTÁVEL']
        termos_esg = ['ESG', 'ASG', 'SUSTENTAB', 'VERDE', 'GREEN', 'AMBIENTAL',
                      'SOCIAL', 'IMPACTO', 'CLIMATICO', 'CLIMA', 'RENOVAVEL',
                      'ENERGIA LIMPA', 'CARBONO', 'TRANSICAO ENERGETICA',
                      'BIODIVERSIDADE', 'ISE', 'ICO2']

        def classificar_fundo(nome):
            if pd.isna(nome):
                return None, None

            nome_upper = str(nome).upper()

            # Verificar sufixo IS (obrigatorio desde jan/2022 para fundos 100% sustentaveis)
            if nome_upper.endswith(' IS') or ' IS ' in nome_upper or 'INVESTIMENTO SUSTENT' in nome_upper:
                return 'IS - Investimento Sustentavel', identificar_foco(nome_upper)

            # Verificar termos ESG
            for termo in termos_esg:
                if termo in nome_upper:
                    return 'ESG Integrado', identificar_foco(nome_upper)

            return None, None

        def identificar_foco(nome_upper):
            focos = []

            termos_ambiental = ['VERDE', 'GREEN', 'CLIMATICO', 'CLIMA', 'RENOVAVEL',
                               'ENERGIA', 'SOLAR', 'EOLICA', 'CARBONO', 'CO2', 'ICO2',
                               'AMBIENTAL', 'TRANSICAO ENERGETICA', 'BIODIVERSIDADE']
            termos_social = ['SOCIAL', 'IMPACTO', 'INCLUSAO', 'HABITACAO',
                            'EDUCACAO', 'SAUDE', 'DIVERSIDADE']
            termos_governanca = ['GOVERNANCA', 'GOVERNANCE', 'ETICA', 'ISE']

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
                return 'Multi-tema'  # Default para ESG sem foco especifico

        # Aplicar classificacao
        df['CategoriaESG'], df['FocoESG'] = zip(*df[nome_col].apply(classificar_fundo))

        # Filtrar apenas fundos ESG (categoria nao nula)
        df_esg = df[df['CategoriaESG'].notna()].copy()

        # Estatisticas
        if not df_esg.empty:
            logger.info(f"Fundos ESG encontrados: {len(df_esg)}")
            logger.info(f"Por categoria:\n{df_esg['CategoriaESG'].value_counts()}")
            logger.info(f"Por foco:\n{df_esg['FocoESG'].value_counts()}")
        else:
            logger.warning("Nenhum fundo ESG encontrado nos dados")

        return df_esg

    def coletar_instituicoes(self) -> pd.DataFrame:
        """Coleta lista de gestoras/instituicoes"""
        logger.info("Coletando instituicoes...")

        resultado = self.api.get_instituicoes()

        if resultado:
            data = resultado.get('data', resultado.get('instituicoes', []))
            if data:
                df = pd.DataFrame(data)
                logger.info(f"Instituicoes coletadas: {len(df)}")
                return df

        return pd.DataFrame()

    def coletar_indices(self) -> pd.DataFrame:
        """Coleta dados de indices ESG (ISE, ICO2)"""
        logger.info("Coletando indices...")

        resultado = self.api.get_indices()

        if resultado:
            data = resultado.get('data', resultado.get('indices', []))
            if data:
                df = pd.DataFrame(data)
                logger.info(f"Indices coletados: {len(df)}")
                return df

        return pd.DataFrame()

    def coletar_historico_fundos(self, cnpjs: List[str], dias: int = 365) -> pd.DataFrame:
        """
        Coleta historico de cotas para lista de fundos

        Args:
            cnpjs: Lista de CNPJs dos fundos
            dias: Numero de dias de historico
        """
        logger.info(f"Coletando historico de {len(cnpjs)} fundos...")

        data_fim = datetime.now().strftime('%Y-%m-%d')
        data_inicio = (datetime.now() - timedelta(days=dias)).strftime('%Y-%m-%d')

        todos_historicos = []

        for i, cnpj in enumerate(cnpjs[:50]):  # Limitar para evitar rate limiting
            logger.info(f"Historico {i+1}/{min(len(cnpjs), 50)}: {cnpj}")

            resultado = self.api.get_fundo_historico(cnpj, data_inicio, data_fim)

            if resultado:
                historico = resultado.get('data', resultado.get('historico', []))
                for item in historico:
                    item['cnpj'] = cnpj
                todos_historicos.extend(historico)

            time.sleep(0.3)  # Rate limiting

        if todos_historicos:
            df = pd.DataFrame(todos_historicos)
            logger.info(f"Registros de historico coletados: {len(df)}")
            return df

        return pd.DataFrame()

    def executar_coleta_completa(self) -> Dict[str, pd.DataFrame]:
        """
        Executa coleta completa de dados reais da ANBIMA

        Returns:
            Dict com DataFrames de dados coletados
        """
        logger.info("=" * 60)
        logger.info("INICIANDO COLETA DE DADOS REAIS - API ANBIMA")
        logger.info("=" * 60)

        resultados = {}

        # 1. Coletar todos os fundos
        df_fundos = self.coletar_todos_fundos()
        if not df_fundos.empty:
            resultados['fundos_todos'] = df_fundos

            # 2. Filtrar fundos ESG
            df_esg = self.filtrar_fundos_esg(df_fundos)
            if not df_esg.empty:
                resultados['fundos_esg'] = df_esg

        # 3. Coletar instituicoes/gestoras
        df_instituicoes = self.coletar_instituicoes()
        if not df_instituicoes.empty:
            resultados['instituicoes'] = df_instituicoes

        # 4. Coletar indices
        df_indices = self.coletar_indices()
        if not df_indices.empty:
            resultados['indices'] = df_indices

        # 5. Coletar historico dos fundos ESG (se existirem)
        if 'fundos_esg' in resultados and not resultados['fundos_esg'].empty:
            # Identificar coluna de CNPJ
            cnpj_cols = ['cnpj', 'cnpj_fundo', 'CNPJ', 'CNPJ_FUNDO']
            cnpj_col = None
            for col in cnpj_cols:
                if col in resultados['fundos_esg'].columns:
                    cnpj_col = col
                    break

            if cnpj_col:
                cnpjs = resultados['fundos_esg'][cnpj_col].unique().tolist()
                df_historico = self.coletar_historico_fundos(cnpjs)
                if not df_historico.empty:
                    resultados['historico'] = df_historico

        logger.info("=" * 60)
        logger.info("COLETA FINALIZADA")
        logger.info(f"Datasets coletados: {list(resultados.keys())}")
        logger.info("=" * 60)

        self.dados_coletados = resultados
        return resultados

    def salvar_dados(self, dados: Dict[str, pd.DataFrame] = None, formato: str = 'csv'):
        """
        Salva os dados coletados em arquivos

        Args:
            dados: Dict com DataFrames (usa dados_coletados se None)
            formato: Formato de saida ('csv', 'json', 'excel')
        """
        if dados is None:
            dados = self.dados_coletados

        if not dados:
            logger.warning("Nenhum dado para salvar")
            return

        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        arquivos_salvos = []

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

            arquivos_salvos.append(filepath)
            logger.info(f"Dados salvos: {filepath} ({len(df)} registros)")

        return arquivos_salvos


def main():
    """
    Funcao principal - coleta dados reais da API ANBIMA
    """
    # Credenciais da API ANBIMA
    CLIENT_ID = "pGmu4mG1hH2m"
    CLIENT_SECRET = "FbzvGiGARejH"

    print("=" * 60)
    print("ANBIMA API Scraper - Dados Reais")
    print("=" * 60)
    print(f"Client ID: {CLIENT_ID}")
    print(f"Data: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    # Inicializar coletor
    coletor = AnbimaDataCollector(CLIENT_ID, CLIENT_SECRET)

    # Executar coleta completa
    dados = coletor.executar_coleta_completa()

    # Salvar dados
    if dados:
        arquivos = coletor.salvar_dados(dados, formato='csv')

        print("\n" + "=" * 60)
        print("RESUMO DA COLETA")
        print("=" * 60)

        for nome, df in dados.items():
            print(f"\n{nome.upper()}:")
            print(f"  - Registros: {len(df)}")
            if not df.empty:
                print(f"  - Colunas: {df.columns.tolist()[:5]}...")

        print("\n" + "=" * 60)
        print("ARQUIVOS SALVOS:")
        for arq in arquivos:
            print(f"  - {arq}")
        print("=" * 60)
    else:
        print("\nNENHUM DADO FOI COLETADO")
        print("Verifique as credenciais e a conexao com a API")


if __name__ == '__main__':
    main()
