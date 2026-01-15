"""
Carrega os fundos do CSV para o SQL Server (sem duplicatas)
"""
import pandas as pd
import pyodbc
import ast

print("=" * 60)
print("CARREGANDO FUNDOS PARA SQL SERVER")
print("=" * 60)

# Conectar ao banco
conn = pyodbc.connect(
    'DRIVER={ODBC Driver 17 for SQL Server};'
    'SERVER=localhost;'
    'DATABASE=ANBIMA_ESG;'
    'Trusted_Connection=yes;'
)
cursor = conn.cursor()

# Carregar CSV (arquivo mais recente)
print("\nCarregando CSV...")
csv_path = r'C:\Users\Cliente\Development\powerbi-bv\data\anbima\fundos_todos_20260112_105758.csv'
df = pd.read_csv(csv_path)
print(f"  Total de linhas no CSV: {len(df)}")

# Verificar duplicatas
duplicados = df.duplicated(subset=['codigo_fundo'], keep='first').sum()
print(f"  Registros duplicados: {duplicados}")

# Remover duplicatas pelo codigo_fundo (se houver)
if duplicados > 0:
    df = df.drop_duplicates(subset=['codigo_fundo'], keep='first')
print(f"  Fundos unicos: {len(df)}")

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

# Inserir em lotes
print("\nInserindo dados...")
batch_size = 5000
total = len(df)
inseridos = 0

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
    inseridos += len(rows)
    print(f"  {inseridos}/{total} inseridos...")

# Verificar resultado
cursor.execute('SELECT COUNT(*) FROM fundos.TodosFundos')
total_db = cursor.fetchone()[0]

cursor.execute('SELECT COUNT(DISTINCT CodigoFundo) FROM fundos.TodosFundos')
unicos_db = cursor.fetchone()[0]

print("\n" + "=" * 60)
print("CARGA CONCLUIDA!")
print("=" * 60)
print(f"Total no banco: {total_db}")
print(f"Fundos unicos: {unicos_db}")

# Estatisticas por categoria
cursor.execute('''
    SELECT TOP 5 Categoria, COUNT(*) as Qtd
    FROM fundos.TodosFundos
    GROUP BY Categoria
    ORDER BY Qtd DESC
''')
print("\nPor categoria:")
for row in cursor.fetchall():
    print(f"  {row[0]}: {row[1]}")

conn.close()
print("\nPronto!")
