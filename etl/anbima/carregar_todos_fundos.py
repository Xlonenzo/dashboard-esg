"""
Carrega TODOS os fundos (100k) para o SQL Server
"""

import pandas as pd
import pyodbc
import ast
from datetime import datetime

def main():
    print("=" * 60)
    print("CARREGANDO TODOS OS FUNDOS PARA SQL SERVER")
    print("=" * 60)

    conn = pyodbc.connect(
        'DRIVER={ODBC Driver 17 for SQL Server};'
        'SERVER=localhost;'
        'DATABASE=ANBIMA_ESG;'
        'Trusted_Connection=yes;'
    )
    cursor = conn.cursor()

    # Criar tabela
    print("\nCriando tabela fundos.TodosFundos...")
    cursor.execute('''
    IF EXISTS (SELECT * FROM sys.tables WHERE name = 'TodosFundos' AND schema_id = SCHEMA_ID('fundos'))
        DROP TABLE fundos.TodosFundos
    ''')
    conn.commit()

    cursor.execute('''
    CREATE TABLE fundos.TodosFundos (
        FundoID INT IDENTITY(1,1) PRIMARY KEY,
        CodigoFundo NVARCHAR(20),
        CNPJ NVARCHAR(20),
        RazaoSocial NVARCHAR(200),
        NomeComercial NVARCHAR(200),
        TipoFundo NVARCHAR(20),
        Categoria NVARCHAR(50),
        CategoriaESG NVARCHAR(50),
        FocoESG NVARCHAR(50),
        DataVigencia DATE,
        Ativo BIT DEFAULT 1,
        DataCarga DATETIME DEFAULT GETDATE()
    )
    ''')
    conn.commit()

    # Carregar CSV
    print("\nCarregando CSV...")
    df = pd.read_csv('C:/Users/Cliente/Development/powerbi-bv/data/anbima/fundos_todos_20260106_123039.csv')
    print(f"  {len(df)} registros")

    # Processar categorias
    print("\nProcessando categorias...")
    categorias = []
    for classes in df['classes']:
        cat = None
        try:
            if pd.notna(classes):
                parsed = ast.literal_eval(classes)
                if parsed and len(parsed) > 0:
                    cat = parsed[0].get('nivel1_categoria', '')
        except:
            pass
        categorias.append(cat)

    df['Categoria_Parsed'] = categorias

    # Estatisticas de categorias
    print("\nCategorias encontradas:")
    for cat, count in df['Categoria_Parsed'].value_counts().head(10).items():
        print(f"  {cat}: {count}")

    # Inserir em lotes usando executemany
    print("\nInserindo dados em lotes...")

    batch_size = 5000
    total = len(df)

    for start in range(0, total, batch_size):
        end = min(start + batch_size, total)
        batch = df.iloc[start:end]

        rows = []
        for _, row in batch.iterrows():
            cnpj = None
            if pd.notna(row.get('identificador_fundo')):
                try:
                    cnpj_raw = str(int(row['identificador_fundo'])).zfill(14)
                    cnpj = f'{cnpj_raw[:2]}.{cnpj_raw[2:5]}.{cnpj_raw[5:8]}/{cnpj_raw[8:12]}-{cnpj_raw[12:14]}'
                except:
                    pass

            rows.append((
                row.get('codigo_fundo'),
                cnpj,
                str(row.get('razao_social_fundo', ''))[:200] if pd.notna(row.get('razao_social_fundo')) else None,
                str(row.get('nome_comercial_fundo', ''))[:200] if pd.notna(row.get('nome_comercial_fundo')) else None,
                row.get('tipo_fundo'),
                row.get('Categoria_Parsed'),
                row.get('CategoriaESG') if pd.notna(row.get('CategoriaESG')) else None,
                row.get('FocoESG') if pd.notna(row.get('FocoESG')) else None,
                str(row.get('data_vigencia'))[:10] if pd.notna(row.get('data_vigencia')) else None
            ))

        cursor.executemany('''
            INSERT INTO fundos.TodosFundos
            (CodigoFundo, CNPJ, RazaoSocial, NomeComercial, TipoFundo, Categoria, CategoriaESG, FocoESG, DataVigencia)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', rows)
        conn.commit()

        print(f"  {end}/{total} inseridos...")

    # Estatisticas finais
    print("\n" + "=" * 60)
    print("CARGA CONCLUIDA!")
    print("=" * 60)

    cursor.execute('SELECT COUNT(*) FROM fundos.TodosFundos')
    total_db = cursor.fetchone()[0]
    print(f"\nTotal de fundos: {total_db}")

    cursor.execute('''
        SELECT Categoria, COUNT(*) as Qtd
        FROM fundos.TodosFundos
        GROUP BY Categoria
        ORDER BY Qtd DESC
    ''')
    print("\nPor categoria:")
    for row in cursor.fetchall():
        print(f"  {row[0]}: {row[1]}")

    conn.close()

if __name__ == '__main__':
    main()
