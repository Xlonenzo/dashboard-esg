"""
Carrega TODOS os arquivos JSON para o SQL Server
=================================================
Este script:
1. Cria as tabelas necessarias no SQL Server
2. Carrega todos os dados JSON nas tabelas
"""

import os
import json
import pyodbc
import pandas as pd
from datetime import datetime

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DATA_DIR = os.path.join(BASE_DIR, 'data', 'anbima')


class SQLServerLoader:
    def __init__(self, server='localhost', database='ANBIMA_ESG'):
        self.server = server
        self.database = database
        self.conn = None

    def conectar(self):
        try:
            conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={self.server};DATABASE={self.database};Trusted_Connection=yes;"
            self.conn = pyodbc.connect(conn_str)
            print(f"Conectado ao SQL Server: {self.server}/{self.database}")
            return True
        except Exception as e:
            print(f"Erro ao conectar: {e}")
            return False

    def executar_sql(self, sql):
        try:
            cursor = self.conn.cursor()
            cursor.execute(sql)
            self.conn.commit()
            return True
        except Exception as e:
            print(f"Erro SQL: {e}")
            return False

    def criar_tabelas(self):
        """Cria todas as tabelas necessarias"""
        print("\nCriando tabelas...")

        # Tabela de Debentures
        sql_debentures = """
        IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'Debentures' AND schema_id = SCHEMA_ID('titulos'))
        BEGIN
            CREATE TABLE titulos.Debentures (
                DebentureID INT IDENTITY(1,1) PRIMARY KEY,
                Grupo NVARCHAR(50),
                CodigoAtivo NVARCHAR(20),
                DataReferencia DATE,
                DataVencimento DATE,
                PercentualTaxa NVARCHAR(50),
                TaxaCompra DECIMAL(10,4),
                TaxaVenda DECIMAL(10,4),
                TaxaIndicativa DECIMAL(10,4),
                DesvioPadrao DECIMAL(10,4),
                ValMinIntervalo DECIMAL(10,4),
                ValMaxIntervalo DECIMAL(10,4),
                PU DECIMAL(18,6),
                PercentPUPar DECIMAL(10,4),
                Duration INT,
                PercentReune NVARCHAR(20),
                Emissor NVARCHAR(200),
                ReferenciaNTNB NVARCHAR(20),
                DataCarga DATETIME DEFAULT GETDATE()
            )
        END
        """

        # Tabela de Titulos Publicos
        sql_titulos_publicos = """
        IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'TitulosPublicos' AND schema_id = SCHEMA_ID('titulos'))
        BEGIN
            CREATE TABLE titulos.TitulosPublicos (
                TituloID INT IDENTITY(1,1) PRIMARY KEY,
                Tipo NVARCHAR(20),
                DataVencimento DATE,
                TaxaCompra DECIMAL(10,4),
                TaxaVenda DECIMAL(10,4),
                TaxaIndicativa DECIMAL(10,4),
                PU DECIMAL(18,6),
                DataReferencia DATE,
                DataCarga DATETIME DEFAULT GETDATE()
            )
        END
        """

        # Tabela de CRI/CRA
        sql_cri_cra = """
        IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'CRICRA' AND schema_id = SCHEMA_ID('titulos'))
        BEGIN
            CREATE TABLE titulos.CRICRA (
                CRICRAID INT IDENTITY(1,1) PRIMARY KEY,
                Tipo NVARCHAR(10),
                CodigoAtivo NVARCHAR(20),
                Emissor NVARCHAR(200),
                DataVencimento DATE,
                Indexador NVARCHAR(20),
                Taxa NVARCHAR(50),
                PU DECIMAL(18,6),
                Duration INT,
                DataCarga DATETIME DEFAULT GETDATE()
            )
        END
        """

        # Tabela de Empresas TSB
        sql_empresas_tsb = """
        IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'EmpresasTSB' AND schema_id = SCHEMA_ID('tsb'))
        BEGIN
            CREATE TABLE tsb.EmpresasTSB (
                EmpresaID INT IDENTITY(1,1) PRIMARY KEY,
                Emissor NVARCHAR(200),
                CNPJ NVARCHAR(20),
                SetorTSB NVARCHAR(100),
                Classificacao NVARCHAR(20),
                Score INT,
                Titulos INT,
                DataCarga DATETIME DEFAULT GETDATE()
            )
        END
        """

        # Tabela de KPIs TSB
        sql_kpis_tsb = """
        IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'KPIsTSB' AND schema_id = SCHEMA_ID('tsb'))
        BEGIN
            CREATE TABLE tsb.KPIsTSB (
                KPIID INT IDENTITY(1,1) PRIMARY KEY,
                Setor NVARCHAR(100),
                CodigoKPI NVARCHAR(20),
                NomeKPI NVARCHAR(200),
                Unidade NVARCHAR(50),
                Frequencia NVARCHAR(50),
                Obrigatorio BIT,
                DataCarga DATETIME DEFAULT GETDATE()
            )
        END
        """

        # Tabela de KPIs por Empresa
        sql_kpis_empresa = """
        IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'KPIsEmpresa' AND schema_id = SCHEMA_ID('tsb'))
        BEGIN
            CREATE TABLE tsb.KPIsEmpresa (
                ID INT IDENTITY(1,1) PRIMARY KEY,
                EmpresaID INT,
                CodigoKPI NVARCHAR(20),
                Valor NVARCHAR(100),
                Status NVARCHAR(20),
                DataCarga DATETIME DEFAULT GETDATE()
            )
        END
        """

        # Criar schema titulos se nao existir
        self.executar_sql("IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'titulos') EXEC('CREATE SCHEMA titulos')")

        # Criar schema tsb se nao existir
        self.executar_sql("IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'tsb') EXEC('CREATE SCHEMA tsb')")

        # Criar tabelas
        self.executar_sql(sql_debentures)
        print("  - Tabela titulos.Debentures criada/verificada")

        self.executar_sql(sql_titulos_publicos)
        print("  - Tabela titulos.TitulosPublicos criada/verificada")

        self.executar_sql(sql_cri_cra)
        print("  - Tabela titulos.CRICRA criada/verificada")

        self.executar_sql(sql_empresas_tsb)
        print("  - Tabela tsb.EmpresasTSB criada/verificada")

        self.executar_sql(sql_kpis_tsb)
        print("  - Tabela tsb.KPIsTSB criada/verificada")

        self.executar_sql(sql_kpis_empresa)
        print("  - Tabela tsb.KPIsEmpresa criada/verificada")

    def limpar_tabelas(self):
        """Limpa as tabelas antes de carregar novos dados"""
        print("\nLimpando tabelas...")
        self.executar_sql("DELETE FROM titulos.Debentures")
        self.executar_sql("DELETE FROM titulos.TitulosPublicos")
        self.executar_sql("DELETE FROM titulos.CRICRA")
        self.executar_sql("DELETE FROM tsb.EmpresasTSB")
        self.executar_sql("DELETE FROM tsb.KPIsTSB")
        self.executar_sql("DELETE FROM tsb.KPIsEmpresa")
        print("  - Tabelas limpas")

    def carregar_debentures(self, arquivo):
        """Carrega debentures do JSON"""
        print(f"\nCarregando debentures de {arquivo}...")

        with open(arquivo, 'r', encoding='utf-8') as f:
            dados = json.load(f)

        debentures = dados.get('debentures', [])
        if not debentures:
            print("  - Nenhuma debenture encontrada")
            return 0

        cursor = self.conn.cursor()
        count = 0

        for deb in debentures:
            try:
                sql = """
                INSERT INTO titulos.Debentures
                (Grupo, CodigoAtivo, DataReferencia, DataVencimento, PercentualTaxa,
                TaxaCompra, TaxaVenda, TaxaIndicativa, DesvioPadrao, ValMinIntervalo,
                ValMaxIntervalo, PU, PercentPUPar, Duration, PercentReune, Emissor, ReferenciaNTNB)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """

                # Converter datas
                data_ref = deb.get('data_referencia')
                data_venc = deb.get('data_vencimento')

                cursor.execute(sql, (
                    deb.get('grupo'),
                    deb.get('codigo_ativo'),
                    data_ref,
                    data_venc,
                    deb.get('percentual_taxa'),
                    deb.get('taxa_compra'),
                    deb.get('taxa_venda'),
                    deb.get('taxa_indicativa'),
                    deb.get('desvio_padrao'),
                    deb.get('val_min_intervalo'),
                    deb.get('val_max_intervalo'),
                    deb.get('pu'),
                    deb.get('percent_pu_par'),
                    deb.get('duration'),
                    deb.get('percent_reune'),
                    deb.get('emissor'),
                    deb.get('referencia_ntnb')
                ))
                count += 1
            except Exception as e:
                pass  # Ignorar erros individuais

        self.conn.commit()
        print(f"  - {count} debentures carregadas")
        return count

    def carregar_titulos_publicos(self, arquivo):
        """Carrega titulos publicos do JSON"""
        print(f"\nCarregando titulos publicos de {arquivo}...")

        with open(arquivo, 'r', encoding='utf-8') as f:
            dados = json.load(f)

        titulos = dados.get('titulos_publicos', [])
        if not titulos:
            print("  - Nenhum titulo publico encontrado")
            return 0

        cursor = self.conn.cursor()
        count = 0

        for tit in titulos:
            try:
                sql = """
                INSERT INTO titulos.TitulosPublicos
                (Tipo, DataVencimento, TaxaCompra, TaxaVenda, TaxaIndicativa, PU, DataReferencia)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """

                cursor.execute(sql, (
                    tit.get('tipo_titulo'),  # Campo correto no JSON
                    tit.get('data_vencimento'),
                    tit.get('taxa_compra'),
                    tit.get('taxa_venda'),
                    tit.get('taxa_indicativa'),
                    tit.get('pu'),
                    tit.get('data_referencia')
                ))
                count += 1
            except Exception as e:
                pass

        self.conn.commit()
        print(f"  - {count} titulos publicos carregados")
        return count

    def carregar_cri_cra(self, arquivo):
        """Carrega CRI/CRA do JSON"""
        print(f"\nCarregando CRI/CRA de {arquivo}...")

        with open(arquivo, 'r', encoding='utf-8') as f:
            dados = json.load(f)

        cri = dados.get('cri', [])
        cra = dados.get('cra', [])

        cursor = self.conn.cursor()
        count = 0

        for item in cri:
            try:
                sql = """
                INSERT INTO titulos.CRICRA (Tipo, CodigoAtivo, Emissor, DataVencimento, Indexador, Taxa, PU, Duration)
                VALUES ('CRI', ?, ?, ?, ?, ?, ?, ?)
                """
                cursor.execute(sql, (
                    item.get('codigo_ativo'),
                    item.get('emissor'),
                    item.get('data_vencimento'),
                    item.get('indexador'),
                    item.get('taxa'),
                    item.get('pu'),
                    item.get('duration')
                ))
                count += 1
            except:
                pass

        for item in cra:
            try:
                sql = """
                INSERT INTO titulos.CRICRA (Tipo, CodigoAtivo, Emissor, DataVencimento, Indexador, Taxa, PU, Duration)
                VALUES ('CRA', ?, ?, ?, ?, ?, ?, ?)
                """
                cursor.execute(sql, (
                    item.get('codigo_ativo'),
                    item.get('emissor'),
                    item.get('data_vencimento'),
                    item.get('indexador'),
                    item.get('taxa'),
                    item.get('pu'),
                    item.get('duration')
                ))
                count += 1
            except:
                pass

        self.conn.commit()
        print(f"  - {count} CRI/CRA carregados")
        return count

    def carregar_empresas_tsb(self, arquivo):
        """Carrega empresas TSB do JSON"""
        print(f"\nCarregando empresas TSB de {arquivo}...")

        with open(arquivo, 'r', encoding='utf-8') as f:
            dados = json.load(f)

        empresas = dados.get('empresas', [])
        if not empresas:
            print("  - Nenhuma empresa TSB encontrada")
            return 0

        cursor = self.conn.cursor()
        count = 0

        for emp in empresas:
            try:
                sql = """
                INSERT INTO tsb.EmpresasTSB (Emissor, CNPJ, SetorTSB, Classificacao, Score, Titulos)
                VALUES (?, ?, ?, ?, ?, ?)
                """
                cursor.execute(sql, (
                    emp.get('emissor'),
                    emp.get('cnpj'),
                    emp.get('setor_tsb'),
                    emp.get('classificacao'),
                    emp.get('score'),
                    emp.get('titulos')
                ))
                count += 1
            except Exception as e:
                print(f"Erro: {e}")

        self.conn.commit()
        print(f"  - {count} empresas TSB carregadas")
        return count

    def carregar_kpis_tsb(self, arquivo):
        """Carrega KPIs TSB do JSON"""
        print(f"\nCarregando KPIs TSB de {arquivo}...")

        with open(arquivo, 'r', encoding='utf-8') as f:
            dados = json.load(f)

        kpis_setor = dados.get('kpis_obrigatorios_por_setor', {})
        if not kpis_setor:
            print("  - Nenhum KPI encontrado")
            return 0

        cursor = self.conn.cursor()
        count = 0

        for setor_key, setor_data in kpis_setor.items():
            setor_nome = setor_data.get('setor_nome', setor_key)
            kpis = setor_data.get('kpis', [])

            for kpi in kpis:
                try:
                    sql = """
                    INSERT INTO tsb.KPIsTSB (Setor, CodigoKPI, NomeKPI, Unidade, Frequencia, Obrigatorio)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """
                    cursor.execute(sql, (
                        setor_nome,
                        kpi.get('id'),
                        kpi.get('nome'),
                        kpi.get('unidade'),
                        kpi.get('frequencia'),
                        1 if kpi.get('obrigatorio') else 0
                    ))
                    count += 1
                except:
                    pass

        self.conn.commit()
        print(f"  - {count} KPIs TSB carregados")
        return count

    def fechar(self):
        if self.conn:
            self.conn.close()


def main():
    print("=" * 60)
    print("CARREGANDO TODOS OS JSON PARA O SQL SERVER")
    print("=" * 60)

    loader = SQLServerLoader()

    if not loader.conectar():
        print("\nERRO: Nao foi possivel conectar ao SQL Server")
        return

    try:
        # Criar tabelas
        loader.criar_tabelas()

        # Limpar tabelas existentes
        loader.limpar_tabelas()

        # Carregar debentures
        arquivo_titulos = os.path.join(DATA_DIR, 'todos_titulos_20260107_002911.json')
        if os.path.exists(arquivo_titulos):
            loader.carregar_debentures(arquivo_titulos)
            loader.carregar_titulos_publicos(arquivo_titulos)
            loader.carregar_cri_cra(arquivo_titulos)

        # Carregar TSB
        arquivo_tsb = os.path.join(DATA_DIR, 'tsb_kpis_empresas.json')
        if os.path.exists(arquivo_tsb):
            loader.carregar_empresas_tsb(arquivo_tsb)
            loader.carregar_kpis_tsb(arquivo_tsb)

        print("\n" + "=" * 60)
        print("CARGA CONCLUIDA!")
        print("=" * 60)

        # Mostrar resumo
        print("\nResumo das tabelas:")
        cursor = loader.conn.cursor()

        tabelas = [
            ('titulos.Debentures', 'Debentures'),
            ('titulos.TitulosPublicos', 'Titulos Publicos'),
            ('titulos.CRICRA', 'CRI/CRA'),
            ('tsb.EmpresasTSB', 'Empresas TSB'),
            ('tsb.KPIsTSB', 'KPIs TSB')
        ]

        for tabela, nome in tabelas:
            try:
                cursor.execute(f"SELECT COUNT(*) FROM {tabela}")
                count = cursor.fetchone()[0]
                print(f"  - {nome}: {count} registros")
            except:
                print(f"  - {nome}: erro ao contar")

    finally:
        loader.fechar()


if __name__ == '__main__':
    main()
