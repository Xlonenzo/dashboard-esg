"""
Carrega CRI/CRA do JSON para o SQL Server
"""
import json
import pyodbc

print("=" * 60)
print("CARREGANDO CRI/CRA PARA SQL SERVER")
print("=" * 60)

# Conectar ao banco
conn = pyodbc.connect(
    'DRIVER={ODBC Driver 17 for SQL Server};'
    'SERVER=localhost;'
    'DATABASE=ANBIMA_ESG;'
    'Trusted_Connection=yes;'
)
cursor = conn.cursor()

# Carregar JSON
print("\nCarregando JSON...")
with open(r'C:\Users\Cliente\Development\powerbi-bv\data\anbima\todos_titulos_20260107_002911.json', 'r', encoding='utf-8') as f:
    dados = json.load(f)

cri_cra = dados.get('cri_cra', [])
print(f"  CRI/CRA encontrados: {len(cri_cra)}")

if len(cri_cra) == 0:
    print("Nenhum CRI/CRA para carregar")
    conn.close()
    exit()

# Recriar tabela
print("\nRecriando tabela...")
cursor.execute('''
IF EXISTS (SELECT * FROM sys.tables WHERE name = 'CRICRA' AND schema_id = SCHEMA_ID('titulos'))
    DROP TABLE titulos.CRICRA
''')
conn.commit()

cursor.execute('''
CREATE TABLE titulos.CRICRA (
    ID INT IDENTITY(1,1) PRIMARY KEY,
    DataReferencia DATE,
    Emissor NVARCHAR(200),
    Originador NVARCHAR(200),
    OriginadorCredito NVARCHAR(200),
    Serie NVARCHAR(20),
    Emissao NVARCHAR(20),
    CodigoAtivo NVARCHAR(20),
    TipoContrato NVARCHAR(10),
    DataVencimento DATE,
    TaxaCompra DECIMAL(18,4),
    TaxaVenda DECIMAL(18,4),
    TaxaIndicativa DECIMAL(18,4),
    DesvioPadrao DECIMAL(18,4),
    PU DECIMAL(18,6),
    PercentPUPar DECIMAL(18,2),
    Duration DECIMAL(18,2),
    TipoRemuneracao NVARCHAR(20),
    TaxaCorrecao DECIMAL(18,4)
)
''')
conn.commit()

# Inserir dados
print("\nInserindo dados...")
for item in cri_cra:
    try:
        cursor.execute('''
            INSERT INTO titulos.CRICRA
            (DataReferencia, Emissor, Originador, OriginadorCredito, Serie, Emissao,
             CodigoAtivo, TipoContrato, DataVencimento, TaxaCompra, TaxaVenda,
             TaxaIndicativa, DesvioPadrao, PU, PercentPUPar, Duration, TipoRemuneracao, TaxaCorrecao)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            item.get('data_referencia'),
            item.get('emissor', '')[:200],
            item.get('originador', '')[:200],
            item.get('originador_credito', '')[:200],
            item.get('serie'),
            item.get('emissao'),
            item.get('codigo_ativo'),
            item.get('tipo_contrato'),
            item.get('data_vencimento'),
            item.get('taxa_compra'),
            item.get('taxa_venda'),
            item.get('taxa_indicativa'),
            item.get('desvio_padrao'),
            item.get('pu') or item.get('vl_pu'),
            item.get('percent_pu_par'),
            item.get('duration'),
            item.get('tipo_remuneracao'),
            item.get('taxa_correcao')
        ))
    except Exception as e:
        print(f"  Erro: {e}")

conn.commit()

# Verificar
cursor.execute('SELECT COUNT(*) FROM titulos.CRICRA')
total = cursor.fetchone()[0]

cursor.execute('SELECT TipoContrato, COUNT(*) as Qtd FROM titulos.CRICRA GROUP BY TipoContrato')
print(f"\nTotal carregado: {total}")
print("\nPor tipo:")
for row in cursor.fetchall():
    print(f"  {row[0]}: {row[1]}")

conn.close()
print("\nPronto!")
