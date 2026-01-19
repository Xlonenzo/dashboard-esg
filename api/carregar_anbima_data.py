"""
Script para carregar dados da ANBIMA Data no banco PostgreSQL
"""
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import os
from datetime import datetime

# Configuracao do banco - Render Cloud
DB_CONFIG = {
    'host': os.getenv('PG_HOST', 'dpg-d5l3gka4d50c73d6fnfg-a.oregon-postgres.render.com'),
    'port': os.getenv('PG_PORT', '5432'),
    'database': os.getenv('PG_DATABASE', 'esg_bv'),
    'user': os.getenv('PG_USER', 'esg_user'),
    'password': os.getenv('PG_PASSWORD', 'hkKOlx2eLV8f1ud7q94Znjxokxj9WRU7'),
}

DATA_DIR = r"C:\Users\Cliente\Development\powerbi-bv\Dataset- anbima"

def get_connection():
    return psycopg2.connect(**DB_CONFIG)

def carregar_fundos_175():
    """Carrega dados de Fundos 175 Caracteristicas"""
    print("\n" + "="*60)
    print("CARREGANDO FUNDOS 175 - CARACTERISTICAS")
    print("="*60)

    file_path = os.path.join(DATA_DIR, "FUNDOS-175-CARACTERISTICAS-PUBLICO.xlsx")
    print(f"Lendo arquivo: {file_path}")

    df = pd.read_excel(file_path)
    print(f"Registros lidos: {len(df):,}")

    # Renomear colunas para o padrao do banco
    df.columns = [
        'codigo_anbima', 'estrutura', 'nome', 'cnpj_classe', 'cnpj_fundo',
        'status', 'data_inicio', 'qtd_subclasses', 'categoria_anbima', 'tipo_anbima',
        'composicao', 'aberto_estatutariamente', 'fundo_esg', 'tributacao_alvo',
        'administrador', 'gestor', 'primeiro_aporte', 'tipo_investidor',
        'caracteristica_investidor', 'cota_abertura', 'aplicacao_minima',
        'prazo_resgate_dias', 'adaptado_175', 'codigo_cvm_subclasse',
        'foco_atuacao', 'nivel1_categoria', 'nivel2_categoria', 'nivel3_subcategoria'
    ]

    # Limpar CNPJs
    df['cnpj_classe'] = df['cnpj_classe'].astype(str).str.replace(r'[^\d]', '', regex=True)
    df['cnpj_fundo'] = df['cnpj_fundo'].astype(str).str.replace(r'[^\d]', '', regex=True)

    # Remover duplicatas por cnpj_classe (manter primeira ocorrencia)
    df = df.drop_duplicates(subset=['cnpj_classe'], keep='first')
    print(f"Registros apos remover duplicatas: {len(df):,}")

    # Converter datas
    for col in ['data_inicio', 'primeiro_aporte']:
        df[col] = pd.to_datetime(df[col], errors='coerce')

    # Converter booleanos
    df['fundo_esg'] = df['fundo_esg'].apply(lambda x: True if str(x).upper() in ['SIM', 'S', 'TRUE', '1'] else False)
    df['adaptado_175'] = df['adaptado_175'].apply(lambda x: True if str(x).upper() in ['SIM', 'S', 'TRUE', '1'] else False)

    conn = get_connection()
    cur = conn.cursor()

    try:
        # Criar tabela se nao existir
        cur.execute("""
            CREATE TABLE IF NOT EXISTS fundos.fundos_anbima (
                id SERIAL PRIMARY KEY,
                codigo_anbima VARCHAR(50),
                estrutura VARCHAR(100),
                nome VARCHAR(500),
                cnpj_classe VARCHAR(20),
                cnpj_fundo VARCHAR(20),
                status VARCHAR(50),
                data_inicio DATE,
                qtd_subclasses INTEGER,
                categoria_anbima VARCHAR(200),
                tipo_anbima VARCHAR(200),
                composicao VARCHAR(200),
                aberto_estatutariamente VARCHAR(50),
                fundo_esg BOOLEAN DEFAULT FALSE,
                tributacao_alvo VARCHAR(100),
                administrador VARCHAR(300),
                gestor VARCHAR(300),
                primeiro_aporte DATE,
                tipo_investidor VARCHAR(100),
                caracteristica_investidor VARCHAR(100),
                cota_abertura VARCHAR(50),
                aplicacao_minima NUMERIC,
                prazo_resgate_dias INTEGER,
                adaptado_175 BOOLEAN DEFAULT FALSE,
                codigo_cvm_subclasse VARCHAR(50),
                foco_atuacao VARCHAR(200),
                nivel1_categoria VARCHAR(200),
                nivel2_categoria VARCHAR(200),
                nivel3_subcategoria VARCHAR(200),
                data_carga TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(cnpj_classe)
            )
        """)

        # Limpar dados antigos
        cur.execute("TRUNCATE TABLE fundos.fundos_anbima RESTART IDENTITY")
        print("Tabela limpa")

        # Preparar dados para insert
        colunas = [
            'codigo_anbima', 'estrutura', 'nome', 'cnpj_classe', 'cnpj_fundo',
            'status', 'data_inicio', 'qtd_subclasses', 'categoria_anbima', 'tipo_anbima',
            'composicao', 'aberto_estatutariamente', 'fundo_esg', 'tributacao_alvo',
            'administrador', 'gestor', 'primeiro_aporte', 'tipo_investidor',
            'caracteristica_investidor', 'cota_abertura', 'aplicacao_minima',
            'prazo_resgate_dias', 'adaptado_175', 'codigo_cvm_subclasse',
            'foco_atuacao', 'nivel1_categoria', 'nivel2_categoria', 'nivel3_subcategoria'
        ]

        # Converter para lista de tuplas
        valores = []
        for _, row in df.iterrows():
            tupla = []
            for col in colunas:
                val = row[col]
                if pd.isna(val):
                    tupla.append(None)
                elif isinstance(val, pd.Timestamp):
                    tupla.append(val.to_pydatetime())
                else:
                    tupla.append(val)
            valores.append(tuple(tupla))

        # Insert em lote
        insert_sql = f"""
            INSERT INTO fundos.fundos_anbima ({', '.join(colunas)})
            VALUES %s
            ON CONFLICT (cnpj_classe) DO UPDATE SET
                nome = EXCLUDED.nome,
                status = EXCLUDED.status,
                categoria_anbima = EXCLUDED.categoria_anbima,
                fundo_esg = EXCLUDED.fundo_esg,
                data_carga = CURRENT_TIMESTAMP
        """

        execute_values(cur, insert_sql, valores, page_size=1000)
        conn.commit()

        # Contar registros
        cur.execute("SELECT COUNT(*) FROM fundos.fundos_anbima")
        total = cur.fetchone()[0]
        print(f"Fundos carregados: {total:,}")

        # Estatisticas
        cur.execute("SELECT COUNT(*) FROM fundos.fundos_anbima WHERE fundo_esg = TRUE")
        esg = cur.fetchone()[0]
        print(f"Fundos ESG: {esg:,}")

    except Exception as e:
        conn.rollback()
        print(f"ERRO: {e}")
        raise
    finally:
        cur.close()
        conn.close()

def carregar_cricra():
    """Carrega dados de CRI/CRA com precos"""
    print("\n" + "="*60)
    print("CARREGANDO CRI/CRA - PRECOS")
    print("="*60)

    file_path = os.path.join(DATA_DIR, "certificados-recebiveis-precos-19-01-2026-09-23-41.xls")
    print(f"Lendo arquivo: {file_path}")

    df = pd.read_excel(file_path)
    print(f"Registros lidos: {len(df):,}")

    # Renomear colunas
    df.columns = [
        'data_referencia', 'tipo', 'codigo', 'emissor', 'devedor',
        'tipo_remuneracao', 'taxa_correcao', 'serie', 'emissao',
        'data_vencimento', 'taxa_compra', 'taxa_venda', 'taxa_indicativa',
        'pu_indicativo', 'desvio_padrao', 'duration_dias', 'pct_pu_par',
        'pct_vne', 'pct_reune', 'referencia_ntnb'
    ]

    conn = get_connection()
    cur = conn.cursor()

    try:
        # Criar tabela
        cur.execute("""
            CREATE TABLE IF NOT EXISTS titulos.cricra_anbima (
                id SERIAL PRIMARY KEY,
                data_referencia DATE,
                tipo VARCHAR(10),
                codigo VARCHAR(50),
                emissor VARCHAR(300),
                devedor VARCHAR(300),
                tipo_remuneracao VARCHAR(100),
                taxa_correcao VARCHAR(50),
                serie VARCHAR(20),
                emissao VARCHAR(20),
                data_vencimento DATE,
                taxa_compra NUMERIC,
                taxa_venda NUMERIC,
                taxa_indicativa NUMERIC,
                pu_indicativo NUMERIC,
                desvio_padrao NUMERIC,
                duration_dias NUMERIC,
                pct_pu_par NUMERIC,
                pct_vne NUMERIC,
                pct_reune NUMERIC,
                referencia_ntnb VARCHAR(50),
                data_carga TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(codigo, data_referencia)
            )
        """)

        # Limpar dados antigos
        cur.execute("TRUNCATE TABLE titulos.cricra_anbima RESTART IDENTITY")
        print("Tabela limpa")

        # Converter datas
        df['data_referencia'] = pd.to_datetime(df['data_referencia'], errors='coerce')
        df['data_vencimento'] = pd.to_datetime(df['data_vencimento'], errors='coerce')

        # Preparar dados
        colunas = [
            'data_referencia', 'tipo', 'codigo', 'emissor', 'devedor',
            'tipo_remuneracao', 'taxa_correcao', 'serie', 'emissao',
            'data_vencimento', 'taxa_compra', 'taxa_venda', 'taxa_indicativa',
            'pu_indicativo', 'desvio_padrao', 'duration_dias', 'pct_pu_par',
            'pct_vne', 'pct_reune', 'referencia_ntnb'
        ]

        valores = []
        for _, row in df.iterrows():
            tupla = []
            for col in colunas:
                val = row[col]
                if pd.isna(val):
                    tupla.append(None)
                elif isinstance(val, pd.Timestamp):
                    tupla.append(val.to_pydatetime())
                else:
                    tupla.append(val)
            valores.append(tuple(tupla))

        insert_sql = f"""
            INSERT INTO titulos.cricra_anbima ({', '.join(colunas)})
            VALUES %s
            ON CONFLICT (codigo, data_referencia) DO UPDATE SET
                taxa_indicativa = EXCLUDED.taxa_indicativa,
                pu_indicativo = EXCLUDED.pu_indicativo,
                data_carga = CURRENT_TIMESTAMP
        """

        execute_values(cur, insert_sql, valores, page_size=500)
        conn.commit()

        cur.execute("SELECT COUNT(*) FROM titulos.cricra_anbima")
        total = cur.fetchone()[0]
        print(f"CRI/CRA carregados: {total:,}")

        cur.execute("SELECT tipo, COUNT(*) FROM titulos.cricra_anbima GROUP BY tipo")
        for row in cur.fetchall():
            print(f"  - {row[0]}: {row[1]:,}")

    except Exception as e:
        conn.rollback()
        print(f"ERRO: {e}")
        raise
    finally:
        cur.close()
        conn.close()

def carregar_titulos_publicos():
    """Carrega dados de Titulos Publicos"""
    print("\n" + "="*60)
    print("CARREGANDO TITULOS PUBLICOS - PRECOS")
    print("="*60)

    file_path = os.path.join(DATA_DIR, "titulos-publicos-precos-19-01-2026-09-23-58.xls")
    print(f"Lendo arquivo: {file_path}")

    df = pd.read_excel(file_path)
    print(f"Registros lidos: {len(df):,}")

    # Renomear colunas
    df.columns = [
        'data_referencia', 'tipo_titulo', 'codigo_selic', 'data_vencimento',
        'codigo_isin', 'data_emissao', 'taxa_compra', 'taxa_venda',
        'taxa_indicativa', 'pu_indicativo', 'desvio_padrao',
        'intervalo_min_d0', 'intervalo_max_d0', 'intervalo_min_d1', 'intervalo_max_d1'
    ]

    conn = get_connection()
    cur = conn.cursor()

    try:
        # Criar tabela
        cur.execute("""
            CREATE TABLE IF NOT EXISTS titulos.titulos_publicos_anbima (
                id SERIAL PRIMARY KEY,
                data_referencia DATE,
                tipo_titulo VARCHAR(20),
                codigo_selic VARCHAR(20),
                data_vencimento DATE,
                codigo_isin VARCHAR(20),
                data_emissao DATE,
                taxa_compra NUMERIC,
                taxa_venda NUMERIC,
                taxa_indicativa NUMERIC,
                pu_indicativo NUMERIC,
                desvio_padrao NUMERIC,
                intervalo_min_d0 NUMERIC,
                intervalo_max_d0 NUMERIC,
                intervalo_min_d1 NUMERIC,
                intervalo_max_d1 NUMERIC,
                data_carga TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(codigo_selic, data_referencia)
            )
        """)

        # Limpar dados antigos
        cur.execute("TRUNCATE TABLE titulos.titulos_publicos_anbima RESTART IDENTITY")
        print("Tabela limpa")

        # Converter datas
        for col in ['data_referencia', 'data_vencimento', 'data_emissao']:
            df[col] = pd.to_datetime(df[col], errors='coerce')

        # Remover duplicatas
        df = df.drop_duplicates(subset=['codigo_selic', 'data_referencia'], keep='first')
        print(f"Registros apos remover duplicatas: {len(df):,}")

        # Preparar dados
        colunas = [
            'data_referencia', 'tipo_titulo', 'codigo_selic', 'data_vencimento',
            'codigo_isin', 'data_emissao', 'taxa_compra', 'taxa_venda',
            'taxa_indicativa', 'pu_indicativo', 'desvio_padrao',
            'intervalo_min_d0', 'intervalo_max_d0', 'intervalo_min_d1', 'intervalo_max_d1'
        ]

        valores = []
        for _, row in df.iterrows():
            tupla = []
            for col in colunas:
                val = row[col]
                if pd.isna(val):
                    tupla.append(None)
                elif isinstance(val, pd.Timestamp):
                    tupla.append(val.to_pydatetime())
                else:
                    tupla.append(val)
            valores.append(tuple(tupla))

        insert_sql = f"""
            INSERT INTO titulos.titulos_publicos_anbima ({', '.join(colunas)})
            VALUES %s
            ON CONFLICT (codigo_selic, data_referencia) DO UPDATE SET
                taxa_indicativa = EXCLUDED.taxa_indicativa,
                pu_indicativo = EXCLUDED.pu_indicativo,
                data_carga = CURRENT_TIMESTAMP
        """

        execute_values(cur, insert_sql, valores, page_size=500)
        conn.commit()

        cur.execute("SELECT COUNT(*) FROM titulos.titulos_publicos_anbima")
        total = cur.fetchone()[0]
        print(f"Titulos Publicos carregados: {total:,}")

        cur.execute("SELECT tipo_titulo, COUNT(*) FROM titulos.titulos_publicos_anbima GROUP BY tipo_titulo ORDER BY 2 DESC")
        for row in cur.fetchall():
            print(f"  - {row[0]}: {row[1]:,}")

    except Exception as e:
        conn.rollback()
        print(f"ERRO: {e}")
        raise
    finally:
        cur.close()
        conn.close()

def carregar_fundos_175_periodicos():
    """Carrega dados periodicos dos Fundos 175 (PL, cotistas, valor da cota)"""
    print("\n" + "="*60)
    print("CARREGANDO FUNDOS 175 - DADOS PERIODICOS")
    print("="*60)

    file_path = os.path.join(DATA_DIR, "FUNDOS-175-PERIODICOS-PUBLICO.xlsx")
    print(f"Lendo arquivo: {file_path}")

    df = pd.read_excel(file_path)
    print(f"Registros lidos: {len(df):,}")

    # Renomear colunas para o padrao do banco
    df.columns = [
        'codigo_anbima', 'estrutura', 'nome', 'cnpj_classe', 'cnpj_fundo',
        'status', 'data_inicio', 'qtd_subclasses', 'data_referencia',
        'pl_atual', 'qtd_cotistas', 'valor_cota', 'foco_atuacao',
        'nivel1_categoria', 'nivel2_categoria', 'nivel3_subcategoria'
    ]

    # Limpar CNPJs
    df['cnpj_classe'] = df['cnpj_classe'].astype(str).str.replace(r'[^\d]', '', regex=True)
    df['cnpj_fundo'] = df['cnpj_fundo'].astype(str).str.replace(r'[^\d]', '', regex=True)

    # Converter datas
    for col in ['data_inicio', 'data_referencia']:
        df[col] = pd.to_datetime(df[col], errors='coerce')

    # Remover duplicatas (manter primeira ocorrencia)
    df = df.drop_duplicates(subset=['cnpj_classe', 'data_referencia'], keep='first')
    print(f"Registros apos remover duplicatas: {len(df):,}")

    conn = get_connection()
    cur = conn.cursor()

    try:
        # Criar tabela se nao existir
        cur.execute("""
            CREATE TABLE IF NOT EXISTS fundos.fundos_anbima_periodicos (
                id SERIAL PRIMARY KEY,
                codigo_anbima VARCHAR(50),
                estrutura VARCHAR(100),
                nome VARCHAR(500),
                cnpj_classe VARCHAR(20),
                cnpj_fundo VARCHAR(20),
                status VARCHAR(50),
                data_inicio DATE,
                qtd_subclasses INTEGER,
                data_referencia DATE,
                pl_atual NUMERIC,
                qtd_cotistas INTEGER,
                valor_cota NUMERIC,
                foco_atuacao VARCHAR(200),
                nivel1_categoria VARCHAR(200),
                nivel2_categoria VARCHAR(200),
                nivel3_subcategoria VARCHAR(200),
                data_carga TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(cnpj_classe, data_referencia)
            )
        """)

        # Criar indices para performance
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_fundos_periodicos_cnpj ON fundos.fundos_anbima_periodicos(cnpj_classe);
            CREATE INDEX IF NOT EXISTS idx_fundos_periodicos_data ON fundos.fundos_anbima_periodicos(data_referencia);
            CREATE INDEX IF NOT EXISTS idx_fundos_periodicos_pl ON fundos.fundos_anbima_periodicos(pl_atual DESC);
        """)

        # Limpar dados antigos
        cur.execute("TRUNCATE TABLE fundos.fundos_anbima_periodicos RESTART IDENTITY")
        print("Tabela limpa")

        # Preparar dados para insert
        colunas = [
            'codigo_anbima', 'estrutura', 'nome', 'cnpj_classe', 'cnpj_fundo',
            'status', 'data_inicio', 'qtd_subclasses', 'data_referencia',
            'pl_atual', 'qtd_cotistas', 'valor_cota', 'foco_atuacao',
            'nivel1_categoria', 'nivel2_categoria', 'nivel3_subcategoria'
        ]

        # Converter para lista de tuplas
        valores = []
        for _, row in df.iterrows():
            tupla = []
            for col in colunas:
                val = row[col]
                if pd.isna(val):
                    tupla.append(None)
                elif isinstance(val, pd.Timestamp):
                    tupla.append(val.to_pydatetime())
                else:
                    tupla.append(val)
            valores.append(tuple(tupla))

        # Insert em lote
        insert_sql = f"""
            INSERT INTO fundos.fundos_anbima_periodicos ({', '.join(colunas)})
            VALUES %s
            ON CONFLICT (cnpj_classe, data_referencia) DO UPDATE SET
                pl_atual = EXCLUDED.pl_atual,
                qtd_cotistas = EXCLUDED.qtd_cotistas,
                valor_cota = EXCLUDED.valor_cota,
                data_carga = CURRENT_TIMESTAMP
        """

        execute_values(cur, insert_sql, valores, page_size=1000)
        conn.commit()

        # Contar registros
        cur.execute("SELECT COUNT(*) FROM fundos.fundos_anbima_periodicos")
        total = cur.fetchone()[0]
        print(f"Fundos periodicos carregados: {total:,}")

        # Estatisticas
        cur.execute("""
            SELECT
                COUNT(*) as total,
                SUM(pl_atual) as pl_total,
                SUM(qtd_cotistas) as cotistas_total,
                AVG(valor_cota) as cota_media
            FROM fundos.fundos_anbima_periodicos
            WHERE pl_atual > 0
        """)
        stats = cur.fetchone()
        print(f"Fundos com PL > 0: {stats[0]:,}")
        print(f"PL Total: R$ {stats[1]:,.2f}" if stats[1] else "PL Total: N/A")
        print(f"Total Cotistas: {int(stats[2]):,}" if stats[2] else "Total Cotistas: N/A")
        print(f"Valor Cota Medio: R$ {stats[3]:,.4f}" if stats[3] else "Valor Cota Medio: N/A")

        # Top 10 por PL
        cur.execute("""
            SELECT nome, pl_atual
            FROM fundos.fundos_anbima_periodicos
            WHERE pl_atual IS NOT NULL
            ORDER BY pl_atual DESC LIMIT 5
        """)
        print("\nTop 5 por Patrimonio Liquido:")
        for row in cur.fetchall():
            print(f"  - {row[0][:50]}: R$ {row[1]:,.2f}")

    except Exception as e:
        conn.rollback()
        print(f"ERRO: {e}")
        raise
    finally:
        cur.close()
        conn.close()

def main():
    print("="*60)
    print("CARREGAMENTO DE DADOS ANBIMA")
    print(f"Inicio: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*60)

    carregar_fundos_175()
    carregar_fundos_175_periodicos()
    carregar_cricra()
    carregar_titulos_publicos()

    print("\n" + "="*60)
    print("CARREGAMENTO CONCLUIDO!")
    print(f"Fim: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*60)

if __name__ == "__main__":
    main()
