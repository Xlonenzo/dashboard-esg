"""
Script para carregar dados da CVM (Comissao de Valores Mobiliarios) no banco PostgreSQL
Fonte: https://dados.cvm.gov.br/dataset/fi-cad (Cadastro de Fundos)
       https://dados.cvm.gov.br/dataset/fi-doc-inf_diario (Informes Diarios)
"""
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import os
import requests
from datetime import datetime, timedelta
from io import StringIO
import zipfile
from io import BytesIO

# Configuracao do banco - Render Cloud
DB_CONFIG = {
    'host': os.getenv('PG_HOST', 'dpg-d5l3gka4d50c73d6fnfg-a.oregon-postgres.render.com'),
    'port': os.getenv('PG_PORT', '5432'),
    'database': os.getenv('PG_DATABASE', 'esg_bv'),
    'user': os.getenv('PG_USER', 'esg_user'),
    'password': os.getenv('PG_PASSWORD', 'hkKOlx2eLV8f1ud7q94Znjxokxj9WRU7'),
}

# URLs da CVM
CVM_BASE_URL = "https://dados.cvm.gov.br/dados/FI"
CVM_CAD_URL = f"{CVM_BASE_URL}/CAD/DADOS/cad_fi.csv"
CVM_INF_DIARIO_URL = f"{CVM_BASE_URL}/DOC/INF_DIARIO/DADOS"

def get_connection():
    return psycopg2.connect(**DB_CONFIG)

def download_csv(url, encoding='latin-1'):
    """Baixa um arquivo CSV da CVM"""
    print(f"Baixando: {url}")
    response = requests.get(url, timeout=120)
    response.raise_for_status()
    return pd.read_csv(StringIO(response.content.decode(encoding)), sep=';')

def download_zip_csv(url, encoding='latin-1'):
    """Baixa um arquivo ZIP da CVM e extrai o CSV"""
    print(f"Baixando ZIP: {url}")
    response = requests.get(url, timeout=180)
    response.raise_for_status()

    with zipfile.ZipFile(BytesIO(response.content)) as z:
        # Pegar o primeiro CSV do ZIP
        csv_name = [n for n in z.namelist() if n.endswith('.csv')][0]
        with z.open(csv_name) as f:
            return pd.read_csv(f, sep=';', encoding=encoding)

def carregar_cadastro_fundos():
    """Carrega cadastro de fundos da CVM"""
    print("\n" + "="*60)
    print("CARREGANDO CADASTRO DE FUNDOS CVM")
    print("="*60)

    try:
        df = download_csv(CVM_CAD_URL)
    except Exception as e:
        print(f"Erro ao baixar cadastro: {e}")
        return 0

    print(f"Registros baixados: {len(df):,}")

    # Colunas esperadas do cadastro CVM
    # CNPJ_FUNDO, DENOM_SOCIAL, DT_REG, DT_CONST, DT_CANCEL, SIT, DT_INI_SIT,
    # DT_INI_ATIV, DT_INI_EXERC, DT_FIM_EXERC, CLASSE, DT_INI_CLASSE, RENTAB_FUNDO,
    # CONDOM, FUNDO_COTAS, FUNDO_EXCLUSIVO, TRIB_LPRAZO, INVEST_QUALIF, TAXA_PERFM,
    # INF_TAXA_PERFM, TAXA_ADM, INF_TAXA_ADM, VL_PATRIM_LIQ, DT_PATRIM_LIQ,
    # DIRETOR, CNPJ_ADMIN, ADMIN, PF_PJ_GESTOR, CPF_CNPJ_GESTOR, GESTOR, CNPJ_AUDITOR, AUDITOR

    # Renomear colunas para o padrao do banco
    column_mapping = {
        'CNPJ_FUNDO': 'cnpj',
        'DENOM_SOCIAL': 'nome',
        'DT_REG': 'data_registro',
        'DT_CONST': 'data_constituicao',
        'DT_CANCEL': 'data_cancelamento',
        'SIT': 'situacao',
        'DT_INI_SIT': 'data_inicio_situacao',
        'DT_INI_ATIV': 'data_inicio_atividade',
        'DT_INI_EXERC': 'data_inicio_exercicio',
        'DT_FIM_EXERC': 'data_fim_exercicio',
        'CLASSE': 'classe',
        'DT_INI_CLASSE': 'data_inicio_classe',
        'RENTAB_FUNDO': 'rentabilidade_fundo',
        'CONDOM': 'condominio',
        'FUNDO_COTAS': 'fundo_cotas',
        'FUNDO_EXCLUSIVO': 'fundo_exclusivo',
        'TRIB_LPRAZO': 'tributacao_longo_prazo',
        'INVEST_QUALIF': 'investidor_qualificado',
        'TAXA_PERFM': 'taxa_performance',
        'INF_TAXA_PERFM': 'info_taxa_performance',
        'TAXA_ADM': 'taxa_administracao',
        'INF_TAXA_ADM': 'info_taxa_administracao',
        'VL_PATRIM_LIQ': 'patrimonio_liquido',
        'DT_PATRIM_LIQ': 'data_patrimonio_liquido',
        'DIRETOR': 'diretor',
        'CNPJ_ADMIN': 'cnpj_administrador',
        'ADMIN': 'administrador',
        'PF_PJ_GESTOR': 'tipo_gestor',
        'CPF_CNPJ_GESTOR': 'cpf_cnpj_gestor',
        'GESTOR': 'gestor',
        'CNPJ_AUDITOR': 'cnpj_auditor',
        'AUDITOR': 'auditor'
    }

    # Renomear apenas colunas que existem
    existing_cols = {k: v for k, v in column_mapping.items() if k in df.columns}
    df = df.rename(columns=existing_cols)

    # Limpar CNPJ
    if 'cnpj' in df.columns:
        df['cnpj'] = df['cnpj'].astype(str).str.replace(r'[^\d]', '', regex=True)

    # Converter datas
    date_cols = ['data_registro', 'data_constituicao', 'data_cancelamento',
                 'data_inicio_situacao', 'data_inicio_atividade', 'data_inicio_classe',
                 'data_patrimonio_liquido']
    for col in date_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')

    # Converter booleanos (S/N)
    bool_cols = ['fundo_cotas', 'fundo_exclusivo', 'tributacao_longo_prazo', 'investidor_qualificado']
    for col in bool_cols:
        if col in df.columns:
            df[col] = df[col].apply(lambda x: True if str(x).upper() in ['S', 'SIM', 'TRUE', '1'] else False)

    # Converter numericos
    numeric_cols = ['taxa_performance', 'taxa_administracao', 'patrimonio_liquido']
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col].astype(str).str.replace(',', '.'), errors='coerce')

    # Remover duplicatas
    df = df.drop_duplicates(subset=['cnpj'], keep='first')
    print(f"Registros apos remover duplicatas: {len(df):,}")

    conn = get_connection()
    cur = conn.cursor()

    try:
        # Criar schema se nao existir
        cur.execute("CREATE SCHEMA IF NOT EXISTS cvm")

        # Criar tabela
        cur.execute("""
            DROP TABLE IF EXISTS cvm.cadastro_fundos CASCADE;
            CREATE TABLE cvm.cadastro_fundos (
                id SERIAL PRIMARY KEY,
                cnpj VARCHAR(20) UNIQUE,
                nome VARCHAR(500),
                data_registro DATE,
                data_constituicao DATE,
                data_cancelamento DATE,
                situacao VARCHAR(50),
                data_inicio_situacao DATE,
                data_inicio_atividade DATE,
                data_inicio_exercicio VARCHAR(20),
                data_fim_exercicio VARCHAR(20),
                classe VARCHAR(200),
                data_inicio_classe DATE,
                rentabilidade_fundo VARCHAR(200),
                condominio VARCHAR(50),
                fundo_cotas BOOLEAN DEFAULT FALSE,
                fundo_exclusivo BOOLEAN DEFAULT FALSE,
                tributacao_longo_prazo BOOLEAN DEFAULT FALSE,
                investidor_qualificado BOOLEAN DEFAULT FALSE,
                taxa_performance NUMERIC,
                info_taxa_performance TEXT,
                taxa_administracao NUMERIC,
                info_taxa_administracao TEXT,
                patrimonio_liquido NUMERIC,
                data_patrimonio_liquido DATE,
                diretor VARCHAR(300),
                cnpj_administrador VARCHAR(20),
                administrador VARCHAR(300),
                tipo_gestor VARCHAR(20),
                cpf_cnpj_gestor VARCHAR(20),
                gestor VARCHAR(300),
                cnpj_auditor VARCHAR(20),
                auditor VARCHAR(300),
                data_carga TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Preparar dados para insert
        columns = ['cnpj', 'nome', 'data_registro', 'data_constituicao', 'data_cancelamento',
                   'situacao', 'data_inicio_situacao', 'data_inicio_atividade',
                   'data_inicio_exercicio', 'data_fim_exercicio', 'classe', 'data_inicio_classe',
                   'rentabilidade_fundo', 'condominio', 'fundo_cotas', 'fundo_exclusivo',
                   'tributacao_longo_prazo', 'investidor_qualificado', 'taxa_performance',
                   'info_taxa_performance', 'taxa_administracao', 'info_taxa_administracao',
                   'patrimonio_liquido', 'data_patrimonio_liquido', 'diretor',
                   'cnpj_administrador', 'administrador', 'tipo_gestor', 'cpf_cnpj_gestor',
                   'gestor', 'cnpj_auditor', 'auditor']

        # Pegar apenas colunas que existem
        available_cols = [c for c in columns if c in df.columns]

        # Preparar valores
        values = []
        for _, row in df.iterrows():
            row_values = []
            for col in available_cols:
                val = row[col]
                if pd.isna(val):
                    row_values.append(None)
                elif isinstance(val, pd.Timestamp):
                    row_values.append(val.date() if not pd.isna(val) else None)
                else:
                    row_values.append(val)
            values.append(tuple(row_values))

        # Insert em batch
        cols_str = ', '.join(available_cols)
        placeholders = ', '.join(['%s'] * len(available_cols))

        execute_values(
            cur,
            f"INSERT INTO cvm.cadastro_fundos ({cols_str}) VALUES %s ON CONFLICT (cnpj) DO NOTHING",
            values,
            page_size=1000
        )

        conn.commit()
        print(f"Cadastro CVM carregado com sucesso: {len(values):,} registros")

        # Estatisticas
        cur.execute("SELECT COUNT(*) FROM cvm.cadastro_fundos")
        total = cur.fetchone()[0]

        cur.execute("SELECT COUNT(*) FROM cvm.cadastro_fundos WHERE situacao = 'EM FUNCIONAMENTO NORMAL'")
        ativos = cur.fetchone()[0]

        cur.execute("SELECT COUNT(DISTINCT classe) FROM cvm.cadastro_fundos")
        classes = cur.fetchone()[0]

        print(f"\nEstatisticas:")
        print(f"  Total de fundos: {total:,}")
        print(f"  Fundos ativos: {ativos:,}")
        print(f"  Classes distintas: {classes:,}")

        return total

    except Exception as e:
        conn.rollback()
        print(f"Erro ao carregar cadastro: {e}")
        raise
    finally:
        cur.close()
        conn.close()

def carregar_informes_diarios(ano=None, mes=None):
    """Carrega informes diarios da CVM para um mes especifico"""
    if ano is None:
        ano = datetime.now().year
    if mes is None:
        mes = datetime.now().month

    print("\n" + "="*60)
    print(f"CARREGANDO INFORMES DIARIOS CVM - {mes:02d}/{ano}")
    print("="*60)

    # URL do arquivo (pode ser CSV ou ZIP dependendo do periodo)
    filename = f"inf_diario_fi_{ano}{mes:02d}.csv"
    url = f"{CVM_INF_DIARIO_URL}/{filename}"

    try:
        df = download_csv(url)
    except:
        # Tentar ZIP se CSV falhar
        try:
            filename = f"inf_diario_fi_{ano}{mes:02d}.zip"
            url = f"{CVM_INF_DIARIO_URL}/{filename}"
            df = download_zip_csv(url)
        except Exception as e:
            print(f"Erro ao baixar informes: {e}")
            return 0

    print(f"Registros baixados: {len(df):,}")

    # Colunas esperadas: CNPJ_FUNDO_CLASSE (novo) ou CNPJ_FUNDO (antigo), DT_COMPTC, VL_TOTAL, VL_QUOTA, VL_PATRIM_LIQ,
    # CAPTC_DIA, RESG_DIA, NR_COTST

    column_mapping = {
        'CNPJ_FUNDO': 'cnpj',
        'CNPJ_FUNDO_CLASSE': 'cnpj',  # Novo formato
        'DT_COMPTC': 'data_competencia',
        'VL_TOTAL': 'valor_total',
        'VL_QUOTA': 'valor_cota',
        'VL_PATRIM_LIQ': 'patrimonio_liquido',
        'CAPTC_DIA': 'captacao_dia',
        'RESG_DIA': 'resgate_dia',
        'NR_COTST': 'numero_cotistas'
    }

    existing_cols = {k: v for k, v in column_mapping.items() if k in df.columns}
    df = df.rename(columns=existing_cols)

    # Verificar se tem coluna cnpj
    if 'cnpj' not in df.columns:
        print(f"Colunas encontradas: {df.columns.tolist()}")
        raise ValueError("Coluna CNPJ nao encontrada no arquivo")

    # Limpar CNPJ
    df['cnpj'] = df['cnpj'].astype(str).str.replace(r'[^\d]', '', regex=True)

    # Converter data
    df['data_competencia'] = pd.to_datetime(df['data_competencia'], errors='coerce')

    # Converter numericos
    numeric_cols = ['valor_total', 'valor_cota', 'patrimonio_liquido', 'captacao_dia', 'resgate_dia', 'numero_cotistas']
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col].astype(str).str.replace(',', '.'), errors='coerce')

    # Pegar apenas ultima data de cada fundo para evitar duplicatas
    df = df.sort_values('data_competencia', ascending=False)
    df = df.drop_duplicates(subset=['cnpj'], keep='first')
    print(f"Registros apos pegar ultima data por fundo: {len(df):,}")

    conn = get_connection()
    cur = conn.cursor()

    try:
        # Criar tabela
        cur.execute("""
            CREATE TABLE IF NOT EXISTS cvm.informes_diarios (
                id SERIAL PRIMARY KEY,
                cnpj VARCHAR(20),
                data_competencia DATE,
                valor_total NUMERIC,
                valor_cota NUMERIC,
                patrimonio_liquido NUMERIC,
                captacao_dia NUMERIC,
                resgate_dia NUMERIC,
                numero_cotistas INTEGER,
                ano_mes VARCHAR(7),
                data_carga TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(cnpj, data_competencia)
            )
        """)

        # Adicionar coluna ano_mes
        df['ano_mes'] = f"{ano}-{mes:02d}"

        # Preparar valores
        columns = ['cnpj', 'data_competencia', 'valor_total', 'valor_cota',
                   'patrimonio_liquido', 'captacao_dia', 'resgate_dia', 'numero_cotistas', 'ano_mes']

        values = []
        for _, row in df.iterrows():
            row_values = []
            for col in columns:
                val = row.get(col)
                if pd.isna(val):
                    row_values.append(None)
                elif isinstance(val, pd.Timestamp):
                    row_values.append(val.date())
                elif col == 'numero_cotistas':
                    row_values.append(int(val) if not pd.isna(val) else None)
                else:
                    row_values.append(val)
            values.append(tuple(row_values))

        # Insert em batch
        cols_str = ', '.join(columns)

        execute_values(
            cur,
            f"""INSERT INTO cvm.informes_diarios ({cols_str}) VALUES %s
                ON CONFLICT (cnpj, data_competencia) DO UPDATE SET
                valor_total = EXCLUDED.valor_total,
                valor_cota = EXCLUDED.valor_cota,
                patrimonio_liquido = EXCLUDED.patrimonio_liquido,
                captacao_dia = EXCLUDED.captacao_dia,
                resgate_dia = EXCLUDED.resgate_dia,
                numero_cotistas = EXCLUDED.numero_cotistas,
                data_carga = CURRENT_TIMESTAMP
            """,
            values,
            page_size=1000
        )

        conn.commit()
        print(f"Informes diarios carregados: {len(values):,} registros")

        # Estatisticas
        cur.execute("""
            SELECT
                COUNT(*) as total,
                SUM(patrimonio_liquido) as pl_total,
                SUM(numero_cotistas) as cotistas_total
            FROM cvm.informes_diarios
            WHERE ano_mes = %s
        """, (f"{ano}-{mes:02d}",))

        stats = cur.fetchone()
        print(f"\nEstatisticas {mes:02d}/{ano}:")
        print(f"  Total de fundos: {stats[0]:,}")
        print(f"  PL Total: R$ {float(stats[1])/1e12:.2f} Trilhoes" if stats[1] else "  PL Total: N/A")
        print(f"  Cotistas Total: {int(stats[2]):,}" if stats[2] else "  Cotistas Total: N/A")

        return len(values)

    except Exception as e:
        conn.rollback()
        print(f"Erro ao carregar informes: {e}")
        raise
    finally:
        cur.close()
        conn.close()

def carregar_carteira_fundos(ano=None, mes=None):
    """
    Carrega carteira de fundos da CVM (CDA - Composicao e Diversificacao das Aplicacoes)
    Foca no BLC_6 que contem titulos de credito privado (Debentures, CRA, CRI)
    """
    if ano is None:
        ano = datetime.now().year
    if mes is None:
        mes = datetime.now().month

    print("\n" + "="*60)
    print(f"CARREGANDO CARTEIRA CVM (CDA) - {mes:02d}/{ano}")
    print("="*60)

    # URL do arquivo CDA
    url = f"{CVM_BASE_URL}/DOC/CDA/DADOS/cda_fi_{ano}{mes:02d}.zip"

    try:
        print(f"Baixando: {url}")
        response = requests.get(url, timeout=300)
        response.raise_for_status()
    except Exception as e:
        print(f"Erro ao baixar CDA: {e}")
        return 0

    conn = get_connection()
    cur = conn.cursor()

    try:
        # Criar tabela de carteira
        cur.execute("""
            CREATE TABLE IF NOT EXISTS cvm.carteira_fundos (
                id SERIAL PRIMARY KEY,
                cnpj_fundo VARCHAR(20),
                nome_fundo VARCHAR(500),
                tipo_fundo VARCHAR(100),
                data_competencia DATE,
                tipo_aplicacao VARCHAR(200),
                tipo_ativo VARCHAR(200),
                emissor VARCHAR(500),
                cnpj_emissor VARCHAR(20),
                quantidade_posicao NUMERIC,
                valor_mercado NUMERIC,
                valor_custo NUMERIC,
                data_vencimento DATE,
                indexador VARCHAR(100),
                taxa_indexador NUMERIC,
                taxa_prefixada NUMERIC,
                titulo_cetip VARCHAR(50),
                ano_mes VARCHAR(7),
                bloco VARCHAR(10),
                data_carga TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Criar indice para performance
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_carteira_cnpj_fundo
            ON cvm.carteira_fundos(cnpj_fundo)
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_carteira_tipo_ativo
            ON cvm.carteira_fundos(tipo_ativo)
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_carteira_emissor
            ON cvm.carteira_fundos(cnpj_emissor)
        """)

        conn.commit()

        total_registros = 0

        with zipfile.ZipFile(BytesIO(response.content)) as z:
            arquivos = z.namelist()
            print(f"Arquivos no ZIP: {len(arquivos)}")

            # Carregar blocos relevantes (BLC_4 a BLC_8 - ativos diversos e credito privado)
            blocos_interesse = ['BLC_4', 'BLC_5', 'BLC_6', 'BLC_7', 'BLC_8']

            for bloco in blocos_interesse:
                csv_files = [f for f in arquivos if bloco in f and f.endswith('.csv')]

                for csv_name in csv_files:
                    print(f"\nProcessando: {csv_name}")

                    try:
                        with z.open(csv_name) as f:
                            df = pd.read_csv(f, sep=';', encoding='latin-1')

                        print(f"  Registros: {len(df):,}")

                        if len(df) == 0:
                            continue

                        # Mapear colunas (variam por bloco)
                        column_mapping = {
                            'CNPJ_FUNDO_CLASSE': 'cnpj_fundo',
                            'CNPJ_FUNDO': 'cnpj_fundo',
                            'DENOM_SOCIAL': 'nome_fundo',
                            'TP_FUNDO_CLASSE': 'tipo_fundo',
                            'DT_COMPTC': 'data_competencia',
                            'TP_APLIC': 'tipo_aplicacao',
                            'TP_ATIVO': 'tipo_ativo',
                            'EMISSOR': 'emissor',
                            'CPF_CNPJ_EMISSOR': 'cnpj_emissor',
                            'QT_POS_FINAL': 'quantidade_posicao',
                            'VL_MERC_POS_FINAL': 'valor_mercado',
                            'VL_CUSTO_POS_FINAL': 'valor_custo',
                            'DT_VENC': 'data_vencimento',
                            'DS_INDEXADOR_POSFX': 'indexador',
                            'PR_INDEXADOR_POSFX': 'taxa_indexador',
                            'PR_TAXA_PREFX': 'taxa_prefixada',
                            'TITULO_CETIP': 'titulo_cetip',
                            # Colunas alternativas
                            'DS_ATIVO': 'tipo_ativo_descricao',
                            'CD_ATIVO': 'codigo_ativo',
                        }

                        existing_cols = {k: v for k, v in column_mapping.items() if k in df.columns}
                        df = df.rename(columns=existing_cols)

                        # Verificar se tem cnpj do fundo
                        if 'cnpj_fundo' not in df.columns:
                            print(f"  Pulando - sem coluna CNPJ")
                            continue

                        # Limpar CNPJ
                        df['cnpj_fundo'] = df['cnpj_fundo'].astype(str).str.replace(r'[^\d]', '', regex=True)

                        if 'cnpj_emissor' in df.columns:
                            df['cnpj_emissor'] = df['cnpj_emissor'].astype(str).str.replace(r'[^\d]', '', regex=True)
                            df['cnpj_emissor'] = df['cnpj_emissor'].replace('nan', None)

                        # Converter data
                        if 'data_competencia' in df.columns:
                            df['data_competencia'] = pd.to_datetime(df['data_competencia'], errors='coerce')
                        if 'data_vencimento' in df.columns:
                            df['data_vencimento'] = pd.to_datetime(df['data_vencimento'], errors='coerce')

                        # Converter numericos
                        numeric_cols = ['quantidade_posicao', 'valor_mercado', 'valor_custo', 'taxa_indexador', 'taxa_prefixada']
                        for col in numeric_cols:
                            if col in df.columns:
                                df[col] = pd.to_numeric(df[col].astype(str).str.replace(',', '.'), errors='coerce')

                        # Adicionar metadados
                        df['ano_mes'] = f"{ano}-{mes:02d}"
                        df['bloco'] = bloco

                        # Selecionar colunas para inserir
                        insert_cols = ['cnpj_fundo', 'nome_fundo', 'tipo_fundo', 'data_competencia',
                                      'tipo_aplicacao', 'tipo_ativo', 'emissor', 'cnpj_emissor',
                                      'quantidade_posicao', 'valor_mercado', 'valor_custo',
                                      'data_vencimento', 'indexador', 'taxa_indexador', 'taxa_prefixada',
                                      'titulo_cetip', 'ano_mes', 'bloco']

                        available_cols = [c for c in insert_cols if c in df.columns]

                        # Preparar valores
                        values = []
                        for _, row in df.iterrows():
                            row_values = []
                            for col in available_cols:
                                val = row[col] if col in row else None
                                if pd.isna(val):
                                    row_values.append(None)
                                elif isinstance(val, pd.Timestamp):
                                    row_values.append(val.date() if not pd.isna(val) else None)
                                else:
                                    row_values.append(val)
                            values.append(tuple(row_values))

                        if values:
                            # Limpar dados anteriores do mesmo periodo
                            cur.execute("""
                                DELETE FROM cvm.carteira_fundos
                                WHERE ano_mes = %s AND bloco = %s
                            """, (f"{ano}-{mes:02d}", bloco))

                            # Inserir novos dados
                            cols_str = ', '.join(available_cols)
                            placeholders = ', '.join(['%s'] * len(available_cols))

                            execute_values(
                                cur,
                                f"INSERT INTO cvm.carteira_fundos ({cols_str}) VALUES %s",
                                values,
                                page_size=5000
                            )

                            total_registros += len(values)
                            print(f"  Inseridos: {len(values):,}")

                    except Exception as e:
                        print(f"  Erro no arquivo {csv_name}: {e}")
                        continue

        conn.commit()

        # Estatisticas finais
        print(f"\n{'='*40}")
        print(f"CARTEIRA CVM CARREGADA: {total_registros:,} posicoes")

        cur.execute("""
            SELECT tipo_ativo, COUNT(*), SUM(valor_mercado)
            FROM cvm.carteira_fundos
            WHERE ano_mes = %s
            GROUP BY tipo_ativo
            ORDER BY SUM(valor_mercado) DESC NULLS LAST
            LIMIT 15
        """, (f"{ano}-{mes:02d}",))

        print(f"\nPrincipais tipos de ativos:")
        for row in cur.fetchall():
            tipo = row[0] or 'N/A'
            count = row[1]
            valor = float(row[2]) if row[2] else 0
            print(f"  {tipo[:50]}: {count:,} posicoes (R$ {valor/1e9:.2f} Bi)")

        return total_registros

    except Exception as e:
        conn.rollback()
        print(f"Erro ao carregar carteira: {e}")
        raise
    finally:
        cur.close()
        conn.close()

def criar_view_unificada():
    """Cria view unificada com dados CVM + ANBIMA"""
    print("\n" + "="*60)
    print("CRIANDO VIEW UNIFICADA CVM + ANBIMA")
    print("="*60)

    conn = get_connection()
    cur = conn.cursor()

    try:
        cur.execute("""
            CREATE OR REPLACE VIEW fundos.fundos_consolidados AS

            -- Fundos que estao na ANBIMA (com ou sem dados CVM)
            SELECT
                'ANBIMA' as fonte_principal,
                a.cnpj_classe as cnpj,
                a.nome,
                a.categoria_anbima,
                a.tipo_anbima,
                a.fundo_esg,
                a.gestor as gestor_anbima,
                a.administrador as administrador_anbima,
                COALESCE(p.pl_atual, cvm.patrimonio_liquido, i.patrimonio_liquido) as patrimonio_liquido,
                COALESCE(p.qtd_cotistas, i.numero_cotistas) as numero_cotistas,
                COALESCE(p.valor_cota, i.valor_cota) as valor_cota,
                a.foco_atuacao,
                a.nivel1_categoria,
                a.nivel2_categoria,
                cvm.classe as classe_cvm,
                cvm.situacao as situacao_cvm,
                cvm.data_registro as data_registro_cvm,
                i.captacao_dia,
                i.resgate_dia,
                CASE WHEN cvm.cnpj IS NOT NULL THEN TRUE ELSE FALSE END as tem_dados_cvm,
                a.data_carga as data_atualizacao_anbima,
                i.data_competencia as data_atualizacao_cvm
            FROM fundos.fundos_anbima a
            LEFT JOIN fundos.fundos_anbima_periodicos p ON a.cnpj_classe = p.cnpj_classe
            LEFT JOIN cvm.cadastro_fundos cvm ON a.cnpj_classe = cvm.cnpj
            LEFT JOIN cvm.informes_diarios i ON a.cnpj_classe = i.cnpj

            UNION ALL

            -- Fundos que estao APENAS na CVM (nao estao na ANBIMA)
            SELECT
                'CVM' as fonte_principal,
                cvm.cnpj,
                cvm.nome,
                cvm.classe as categoria_anbima,
                NULL as tipo_anbima,
                FALSE as fundo_esg,
                cvm.gestor as gestor_anbima,
                cvm.administrador as administrador_anbima,
                COALESCE(i.patrimonio_liquido, cvm.patrimonio_liquido) as patrimonio_liquido,
                i.numero_cotistas,
                i.valor_cota,
                NULL as foco_atuacao,
                NULL as nivel1_categoria,
                NULL as nivel2_categoria,
                cvm.classe as classe_cvm,
                cvm.situacao as situacao_cvm,
                cvm.data_registro as data_registro_cvm,
                i.captacao_dia,
                i.resgate_dia,
                TRUE as tem_dados_cvm,
                cvm.data_carga as data_atualizacao_anbima,
                i.data_competencia as data_atualizacao_cvm
            FROM cvm.cadastro_fundos cvm
            LEFT JOIN cvm.informes_diarios i ON cvm.cnpj = i.cnpj
            WHERE NOT EXISTS (
                SELECT 1 FROM fundos.fundos_anbima a
                WHERE a.cnpj_classe = cvm.cnpj
            )
        """)

        conn.commit()
        print("View fundos.fundos_consolidados criada com sucesso!")

        # Estatisticas da view
        cur.execute("""
            SELECT
                fonte_principal,
                COUNT(*) as total,
                COUNT(CASE WHEN tem_dados_cvm THEN 1 END) as com_cvm,
                SUM(patrimonio_liquido) as pl_total
            FROM fundos.fundos_consolidados
            GROUP BY fonte_principal
        """)

        for row in cur.fetchall():
            print(f"\n  Fonte {row[0]}:")
            print(f"    Total: {row[1]:,}")
            print(f"    Com dados CVM: {row[2]:,}")
            if row[3]:
                print(f"    PL Total: R$ {float(row[3])/1e12:.2f} Trilhoes")

        # Total geral
        cur.execute("SELECT COUNT(*) FROM fundos.fundos_consolidados")
        total = cur.fetchone()[0]
        print(f"\n  TOTAL CONSOLIDADO: {total:,} fundos")

    except Exception as e:
        conn.rollback()
        print(f"Erro ao criar view: {e}")
        raise
    finally:
        cur.close()
        conn.close()

def main(incluir_carteira=False):
    """Executa ETL completo"""
    print("="*60)
    print("ETL CVM - DADOS DE FUNDOS")
    print(f"Inicio: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*60)

    # 1. Carregar cadastro de fundos
    total_cadastro = carregar_cadastro_fundos()

    # 2. Carregar informes diarios (ultimos 3 meses)
    hoje = datetime.now()
    total_informes = 0

    for i in range(3):
        data = hoje - timedelta(days=30*i)
        try:
            total_informes += carregar_informes_diarios(data.year, data.month)
        except Exception as e:
            print(f"Erro ao carregar {data.month:02d}/{data.year}: {e}")

    # 3. Carregar carteira de fundos (CDA) - opcional pois é grande
    total_carteira = 0
    if incluir_carteira:
        # Tenta mes atual, se nao existir tenta mes anterior
        for tentativa in range(3):
            data_carteira = hoje - timedelta(days=30*tentativa)
            try:
                total_carteira = carregar_carteira_fundos(data_carteira.year, data_carteira.month)
                if total_carteira > 0:
                    break
            except Exception as e:
                print(f"CDA {data_carteira.month:02d}/{data_carteira.year} nao disponivel: {e}")

    # 4. Criar view unificada
    criar_view_unificada()

    print("\n" + "="*60)
    print("ETL CVM CONCLUIDO")
    print(f"Fim: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Cadastro: {total_cadastro:,} fundos")
    print(f"Informes: {total_informes:,} registros")
    if incluir_carteira:
        print(f"Carteira: {total_carteira:,} posicoes")
    print("="*60)

if __name__ == "__main__":
    import sys
    incluir_carteira = '--carteira' in sys.argv or '-c' in sys.argv
    if incluir_carteira:
        print("Modo: ETL completo COM carteira de fundos (CDA)")
    else:
        print("Modo: ETL basico (use --carteira ou -c para incluir CDA)")
    main(incluir_carteira=incluir_carteira)
