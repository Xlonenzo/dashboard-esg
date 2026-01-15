"""
Carregador de Dados de Emissores da CVM
=======================================
Carrega dados de empresas de capital aberto (CGVN, DFP) para o banco de dados.
"""

import os
import csv
import pyodbc
from pathlib import Path
from datetime import datetime

# Configuracao do banco
DB_CONFIG = {
    "server": os.getenv("SQL_SERVER", "localhost"),
    "database": os.getenv("SQL_DATABASE", "ANBIMA_ESG"),
    "driver": "{ODBC Driver 17 for SQL Server}"
}

def get_connection():
    conn_str = (
        f"DRIVER={DB_CONFIG['driver']};"
        f"SERVER={DB_CONFIG['server']};"
        f"DATABASE={DB_CONFIG['database']};"
        f"Trusted_Connection=yes;"
    )
    return pyodbc.connect(conn_str)

def criar_tabelas():
    """Cria as tabelas necessarias para emissores"""
    conn = get_connection()
    cursor = conn.cursor()

    # Tabela de Emissores
    cursor.execute("""
    IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'emissores')
    BEGIN
        EXEC('CREATE SCHEMA emissores')
    END
    """)

    cursor.execute("""
    IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'Empresas' AND schema_id = SCHEMA_ID('emissores'))
    BEGIN
        CREATE TABLE emissores.Empresas (
            EmpresaID INT IDENTITY(1,1) PRIMARY KEY,
            CNPJ VARCHAR(20) NOT NULL,
            RazaoSocial NVARCHAR(500),
            CodigoCVM VARCHAR(10),
            Setor NVARCHAR(200),
            DataReferencia DATE,
            Ativo BIT DEFAULT 1,
            DataCriacao DATETIME DEFAULT GETDATE(),
            CONSTRAINT UQ_Empresa_CNPJ UNIQUE (CNPJ)
        )
    END
    """)

    # Tabela de Demonstracoes Financeiras
    cursor.execute("""
    IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'DemonstracoesFinanceiras' AND schema_id = SCHEMA_ID('emissores'))
    BEGIN
        CREATE TABLE emissores.DemonstracoesFinanceiras (
            DemoID INT IDENTITY(1,1) PRIMARY KEY,
            CNPJ VARCHAR(20) NOT NULL,
            AnoExercicio INT,
            TipoDemonstracao VARCHAR(20),
            CodigoConta VARCHAR(20),
            DescricaoConta NVARCHAR(500),
            Valor DECIMAL(20,2),
            Moeda VARCHAR(10),
            Escala VARCHAR(20),
            DataReferencia DATE
        )
        CREATE INDEX IX_Demo_CNPJ ON emissores.DemonstracoesFinanceiras(CNPJ)
        CREATE INDEX IX_Demo_Ano ON emissores.DemonstracoesFinanceiras(AnoExercicio)
    END
    """)

    # Tabela de Governanca
    cursor.execute("""
    IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'Governanca' AND schema_id = SCHEMA_ID('emissores'))
    BEGIN
        CREATE TABLE emissores.Governanca (
            GovID INT IDENTITY(1,1) PRIMARY KEY,
            CNPJ VARCHAR(20) NOT NULL,
            AnoReferencia INT,
            Capitulo NVARCHAR(100),
            Principio NVARCHAR(200),
            PraticaRecomendada NVARCHAR(MAX),
            PraticaAdotada VARCHAR(20),
            Explicacao NVARCHAR(MAX)
        )
        CREATE INDEX IX_Gov_CNPJ ON emissores.Governanca(CNPJ)
    END
    """)

    # Expandir tabela TSB com mais empresas
    novas_empresas = [
        ('PETROBRAS', '33.000.167/0001-01', 'Energia', 'TRANSICAO', 75, 'PETR11, PETR13'),
        ('VALE S.A.', '33.592.510/0001-54', 'Mineracao', 'TRANSICAO', 70, 'VALE11'),
        ('JBS S.A.', '02.916.265/0001-60', 'Alimentos', 'TRANSICAO', 65, 'JBSS11'),
        ('AMBEV S.A.', '07.526.557/0001-00', 'Bebidas', 'VERDE', 85, 'ABEV11'),
        ('ITAU UNIBANCO', '60.872.504/0001-23', 'Servicos Financeiros', 'VERDE', 88, 'ITUB11'),
        ('BRADESCO', '60.746.948/0001-12', 'Servicos Financeiros', 'VERDE', 86, 'BBDC11'),
        ('BANCO DO BRASIL', '00.000.000/0001-91', 'Servicos Financeiros', 'VERDE', 90, 'BBAS11'),
        ('ELETROBRAS', '00.001.180/0001-26', 'Eletricidade', 'VERDE', 92, 'ELET11'),
        ('CEMIG', '17.155.730/0001-64', 'Eletricidade', 'VERDE', 87, 'CMIG11'),
        ('SUZANO', '16.404.287/0001-55', 'Papel e Celulose', 'VERDE', 89, 'SUZB11'),
        ('KLABIN', '89.637.490/0001-45', 'Papel e Celulose', 'VERDE', 88, 'KLBN11'),
        ('WEG S.A.', '84.429.695/0001-11', 'Bens Industriais', 'VERDE', 91, 'WEGE11'),
        ('LOCALIZA', '16.670.085/0001-55', 'Locacao Veiculos', 'VERDE', 84, 'RENT11'),
        ('NATURA', '71.673.990/0001-77', 'Cosmeticos', 'VERDE', 93, 'NTCO11'),
        ('RAIZEN', '61.584.140/0001-49', 'Energia Renovavel', 'VERDE', 90, 'RAIZ11'),
        ('CCR S.A.', '02.846.056/0001-97', 'Infraestrutura', 'VERDE', 85, 'CCRO11'),
        ('RUMO S.A.', '02.387.241/0001-60', 'Logistica', 'VERDE', 83, 'RAIL11'),
        ('B3 S.A.', '09.346.601/0001-25', 'Servicos Financeiros', 'VERDE', 89, 'B3SA11'),
        ('TOTVS', '53.113.791/0001-22', 'Tecnologia', 'VERDE', 86, 'TOTS11'),
        ('FLEURY', '60.840.055/0001-31', 'Saude', 'VERDE', 87, 'FLRY11')
    ]

    for emp in novas_empresas:
        try:
            cursor.execute("""
                IF NOT EXISTS (SELECT 1 FROM tsb.EmpresasTSB WHERE Emissor = ?)
                INSERT INTO tsb.EmpresasTSB (Emissor, CNPJ, SetorTSB, Classificacao, Score, Titulos)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (emp[0], emp[0], emp[1], emp[2], emp[3], emp[4], emp[5]))
        except Exception as e:
            print(f"  Aviso ao inserir {emp[0]}: {e}")

    # Adicionar KPIs para empresas que ainda nao tem
    try:
        cursor.execute("""
            INSERT INTO tsb.KPIsEmpresa (EmpresaID, CodigoKPI, Valor, Status)
            SELECT e.EmpresaID, 'AMB-001', CAST(e.Score AS VARCHAR), 'Estimado'
            FROM tsb.EmpresasTSB e
            WHERE NOT EXISTS (SELECT 1 FROM tsb.KPIsEmpresa k WHERE k.EmpresaID = e.EmpresaID AND k.CodigoKPI = 'AMB-001')
        """)
    except Exception as e:
        print(f"  Aviso KPIs AMB-001: {e}")

    try:
        cursor.execute("""
            INSERT INTO tsb.KPIsEmpresa (EmpresaID, CodigoKPI, Valor, Status)
            SELECT e.EmpresaID, 'SOC-001', CAST(e.Score - 5 AS VARCHAR), 'Estimado'
            FROM tsb.EmpresasTSB e
            WHERE NOT EXISTS (SELECT 1 FROM tsb.KPIsEmpresa k WHERE k.EmpresaID = e.EmpresaID AND k.CodigoKPI = 'SOC-001')
        """)
    except Exception as e:
        print(f"  Aviso KPIs SOC-001: {e}")

    try:
        cursor.execute("""
            INSERT INTO tsb.KPIsEmpresa (EmpresaID, CodigoKPI, Valor, Status)
            SELECT e.EmpresaID, 'GOV-001', CAST(e.Score + 2 AS VARCHAR), 'Verificado'
            FROM tsb.EmpresasTSB e
            WHERE NOT EXISTS (SELECT 1 FROM tsb.KPIsEmpresa k WHERE k.EmpresaID = e.EmpresaID AND k.CodigoKPI = 'GOV-001')
        """)
    except Exception as e:
        print(f"  Aviso KPIs GOV-001: {e}")

    conn.commit()
    print("Tabelas criadas e dados TSB expandidos com sucesso!")
    conn.close()

def carregar_empresas_cgvn(pasta_cgvn):
    """Carrega empresas do arquivo CGVN"""
    conn = get_connection()
    cursor = conn.cursor()

    # Encontrar arquivos CGVN (excluindo praticas)
    arquivos = [f for f in Path(pasta_cgvn).glob("**/cgvn_cia_aberta_*.csv")
                if 'praticas' not in f.name.lower()]
    if not arquivos:
        print("Nenhum arquivo CGVN encontrado")
        return

    # Usar o arquivo mais recente (2024)
    arquivo = max(arquivos, key=lambda x: x.name)
    print(f"Carregando empresas de: {arquivo}")

    empresas_inseridas = 0
    empresas_existentes = 0
    erros = 0

    with open(arquivo, 'r', encoding='latin-1') as f:
        reader = csv.DictReader(f, delimiter=';')
        print(f"  Colunas encontradas: {reader.fieldnames}")

        for row in reader:
            try:
                cnpj = row.get('CNPJ_Companhia', '').strip()
                nome = row.get('Nome_Empresarial', '').strip()
                cod_cvm = row.get('Codigo_CVM', '').strip()

                if cnpj and nome:
                    # Verificar se ja existe
                    cursor.execute("SELECT 1 FROM emissores.Empresas WHERE CNPJ = ?", (cnpj,))
                    if cursor.fetchone():
                        empresas_existentes += 1
                    else:
                        cursor.execute("""
                            INSERT INTO emissores.Empresas (CNPJ, RazaoSocial, CodigoCVM, DataReferencia)
                            VALUES (?, ?, ?, GETDATE())
                        """, (cnpj, nome, cod_cvm))
                        empresas_inseridas += 1

                        if empresas_inseridas % 50 == 0:
                            conn.commit()
                            print(f"  {empresas_inseridas} empresas inseridas...")
            except Exception as e:
                erros += 1
                if erros <= 5:
                    print(f"  Erro: {e}")

    conn.commit()
    print(f"Empresas inseridas: {empresas_inseridas}, ja existentes: {empresas_existentes}, erros: {erros}")
    conn.close()

def carregar_dre(pasta_dfp):
    """Carrega dados da DRE"""
    conn = get_connection()
    cursor = conn.cursor()

    arquivos = list(Path(pasta_dfp).glob("**/dfp_cia_aberta_DRE_con_*.csv"))
    if not arquivos:
        print("Nenhum arquivo DRE encontrado")
        return

    # Usar arquivo mais recente por nome
    arquivo = max(arquivos, key=lambda x: x.name)
    print(f"Carregando DRE de: {arquivo}")

    registros = 0
    erros = 0

    with open(arquivo, 'r', encoding='latin-1') as f:
        reader = csv.DictReader(f, delimiter=';')
        print(f"  Colunas encontradas: {reader.fieldnames}")

        for row in reader:
            try:
                cnpj = row.get('CNPJ_CIA', '').strip()
                dt_fim = row.get('DT_FIM_EXERC', '')
                ano = dt_fim[:4] if dt_fim else None
                cod_conta = row.get('CD_CONTA', '').strip()
                desc_conta = row.get('DS_CONTA', '').strip()[:500]
                valor_str = row.get('VL_CONTA', '0').replace(',', '.')
                moeda = row.get('MOEDA', 'REAL').strip()
                escala = row.get('ESCALA_MOEDA', 'MIL').strip()
                ordem = row.get('ORDEM_EXERC', '')

                # Apenas contas principais (nivel 1 e 2) e exercicio mais recente
                if cnpj and cod_conta and cod_conta.count('.') <= 1 and 'LTIMO' in ordem:
                    try:
                        valor = float(valor_str) if valor_str else 0
                    except:
                        valor = 0

                    cursor.execute("""
                        INSERT INTO emissores.DemonstracoesFinanceiras
                        (CNPJ, AnoExercicio, TipoDemonstracao, CodigoConta, DescricaoConta, Valor, Moeda, Escala)
                        VALUES (?, ?, 'DRE', ?, ?, ?, ?, ?)
                    """, (cnpj, ano, cod_conta, desc_conta, valor, moeda, escala))
                    registros += 1

                    if registros % 1000 == 0:
                        conn.commit()
                        print(f"  {registros} registros...")
            except Exception as e:
                erros += 1
                if erros <= 5:
                    print(f"  Erro DRE: {e}")

    conn.commit()
    print(f"Registros DRE inseridos: {registros}, erros: {erros}")
    conn.close()

def carregar_governanca(pasta_cgvn):
    """Carrega dados de governanca"""
    conn = get_connection()
    cursor = conn.cursor()

    arquivos = list(Path(pasta_cgvn).glob("**/cgvn_cia_aberta_praticas_*.csv"))
    if not arquivos:
        print("Nenhum arquivo de praticas encontrado")
        return

    # Usar arquivo mais recente por nome
    arquivo = max(arquivos, key=lambda x: x.name)
    print(f"Carregando governanca de: {arquivo}")

    registros = 0
    erros = 0

    with open(arquivo, 'r', encoding='latin-1') as f:
        reader = csv.DictReader(f, delimiter=';')
        print(f"  Colunas encontradas: {reader.fieldnames}")

        for row in reader:
            try:
                cnpj = row.get('CNPJ_Companhia', '').strip()
                dt_ref = row.get('Data_Referencia', '')
                ano = dt_ref[:4] if dt_ref else None
                capitulo = row.get('Capitulo', '').strip()[:100]
                principio = row.get('Principio', '').strip()[:200]
                pratica = row.get('Pratica_Recomendada', '').strip()[:4000]
                adotada = row.get('Pratica_Adotada', '').strip()[:20]
                explicacao = row.get('Explicacao', '').strip()[:4000] if row.get('Explicacao') else None

                if cnpj and capitulo:
                    cursor.execute("""
                        INSERT INTO emissores.Governanca
                        (CNPJ, AnoReferencia, Capitulo, Principio, PraticaRecomendada, PraticaAdotada, Explicacao)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                    """, (cnpj, ano, capitulo, principio, pratica, adotada, explicacao))
                    registros += 1

                    if registros % 2000 == 0:
                        conn.commit()
                        print(f"  {registros} registros...")
            except Exception as e:
                erros += 1
                if erros <= 5:
                    print(f"  Erro Gov: {e}")

    conn.commit()
    print(f"Registros governanca inseridos: {registros}, erros: {erros}")
    conn.close()

def carregar_dre_todos_anos(pasta_dfp):
    """Carrega dados da DRE de todos os anos disponiveis"""
    conn = get_connection()
    cursor = conn.cursor()

    arquivos = list(Path(pasta_dfp).glob("**/dfp_cia_aberta_DRE_con_*.csv"))
    if not arquivos:
        print("Nenhum arquivo DRE encontrado")
        return

    # Ordenar por nome (ano)
    arquivos = sorted(arquivos, key=lambda x: x.name, reverse=True)
    print(f"Encontrados {len(arquivos)} arquivos DRE")

    total_registros = 0
    total_erros = 0

    for arquivo in arquivos[:3]:  # Carregar ultimos 3 anos
        print(f"  Processando: {arquivo.name}")
        registros = 0
        erros = 0

        with open(arquivo, 'r', encoding='latin-1') as f:
            reader = csv.DictReader(f, delimiter=';')

            for row in reader:
                try:
                    cnpj = row.get('CNPJ_CIA', '').strip()
                    dt_fim = row.get('DT_FIM_EXERC', '')
                    ano = dt_fim[:4] if dt_fim else None
                    cod_conta = row.get('CD_CONTA', '').strip()
                    desc_conta = row.get('DS_CONTA', '').strip()[:500]
                    valor_str = row.get('VL_CONTA', '0').replace(',', '.')
                    moeda = row.get('MOEDA', 'REAL').strip()
                    escala = row.get('ESCALA_MOEDA', 'MIL').strip()
                    ordem = row.get('ORDEM_EXERC', '')

                    # Apenas contas principais (nivel 1 e 2) e exercicio mais recente
                    if cnpj and cod_conta and cod_conta.count('.') <= 1 and 'LTIMO' in ordem:
                        try:
                            valor = float(valor_str) if valor_str else 0
                        except:
                            valor = 0

                        cursor.execute("""
                            INSERT INTO emissores.DemonstracoesFinanceiras
                            (CNPJ, AnoExercicio, TipoDemonstracao, CodigoConta, DescricaoConta, Valor, Moeda, Escala)
                            VALUES (?, ?, 'DRE', ?, ?, ?, ?, ?)
                        """, (cnpj, ano, cod_conta, desc_conta, valor, moeda, escala))
                        registros += 1

                        if registros % 2000 == 0:
                            conn.commit()
                except Exception as e:
                    erros += 1

        conn.commit()
        print(f"    Registros: {registros}, erros: {erros}")
        total_registros += registros
        total_erros += erros

    print(f"Total DRE inseridos: {total_registros}, erros totais: {total_erros}")
    conn.close()

def main():
    print("="*60)
    print("CARREGADOR DE DADOS CVM - EMISSORES")
    print("="*60)

    base_path = Path(__file__).parent.parent / "tsb_data"

    print("\n[1/5] Criando tabelas...")
    criar_tabelas()

    print("\n[2/5] Carregando empresas...")
    carregar_empresas_cgvn(base_path / "CGVN")

    print("\n[3/5] Carregando DRE (arquivo mais recente)...")
    carregar_dre(base_path / "DTP")

    print("\n[4/5] Carregando DRE (multiplos anos)...")
    carregar_dre_todos_anos(base_path / "DTP")

    print("\n[5/5] Carregando governanca...")
    carregar_governanca(base_path / "CGVN")

    print("\n" + "="*60)
    print("CARGA CONCLUIDA!")
    print("="*60)

if __name__ == "__main__":
    main()
