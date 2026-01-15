"""
ETL Principal - Modelagem ESG Banco Votorantim
===============================================

Este script executa o processo completo de ETL para carregar
os dados dos arquivos Excel para o Azure SQL Server.

Uso:
    python main.py              # Executa ETL completo
    python main.py --test       # Testa conexao
    python main.py --dim        # Apenas dimensoes
    python main.py --fato       # Apenas fatos
    python main.py --truncate   # Limpa tabelas antes de carregar
"""

import sys
import argparse
from datetime import datetime

# Imports locais
from config import DB_CONFIG, EXCEL_FILES, CONNECTION_MODE
from database import db
from etl_dimensoes import run_dimensoes
from etl_fatos import run_fatos


def print_header():
    """Imprime cabecalho do ETL."""
    print("\n" + "=" * 70)
    print("  ETL - MODELAGEM ESG BANCO VOTORANTIM")
    modo = "LOCAL" if CONNECTION_MODE == "local" else "AZURE"
    print(f"  Modo: {modo}")
    print("=" * 70)
    print(f"  Inicio: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  Servidor: {DB_CONFIG['server']}")
    print(f"  Database: {DB_CONFIG['database']}")
    auth = "Windows Auth" if CONNECTION_MODE == "local" and not DB_CONFIG.get("username") else "SQL Auth"
    print(f"  Autenticacao: {auth}")
    print("=" * 70)


def test_connection():
    """Testa a conexao com o banco."""
    print("\nTestando conexao com o Azure SQL Server...")

    if db.test_connection():
        print("Conexao OK!")

        # Verifica se o schema existe
        try:
            result = db.read_sql("SELECT COUNT(*) as cnt FROM sys.schemas WHERE name = 'esg'")
            if result['cnt'].iloc[0] > 0:
                print("Schema 'esg' encontrado.")

                # Lista tabelas
                tables = db.read_sql("""
                    SELECT TABLE_NAME
                    FROM INFORMATION_SCHEMA.TABLES
                    WHERE TABLE_SCHEMA = 'esg'
                    ORDER BY TABLE_NAME
                """)
                print(f"\nTabelas encontradas: {len(tables)}")
                for t in tables['TABLE_NAME']:
                    print(f"  - esg.{t}")
            else:
                print("AVISO: Schema 'esg' nao encontrado.")
                print("Execute os scripts SQL primeiro (01 a 06).")
        except Exception as e:
            print(f"Erro ao verificar schema: {e}")

        return True
    else:
        print("ERRO: Nao foi possivel conectar ao banco.")
        print("\nVerifique:")
        print("  1. As credenciais em config.py ou variaveis de ambiente")
        print("  2. Se o firewall permite acesso ao Azure SQL")
        print("  3. Se o ODBC Driver 18 esta instalado")
        return False


def check_files():
    """Verifica quais arquivos Excel existem."""
    print("\nVerificando arquivos Excel...")

    existentes = []
    faltando = []

    for nome, path in EXCEL_FILES.items():
        if path.exists():
            existentes.append(nome)
        else:
            faltando.append(nome)

    print(f"\nArquivos encontrados: {len(existentes)}")
    for f in existentes[:10]:
        print(f"  + {f}")
    if len(existentes) > 10:
        print(f"  ... e mais {len(existentes) - 10}")

    if faltando:
        print(f"\nArquivos nao encontrados: {len(faltando)}")
        for f in faltando[:5]:
            print(f"  - {f}")
        if len(faltando) > 5:
            print(f"  ... e mais {len(faltando) - 5}")

    return len(existentes) > 0


def truncate_all():
    """Limpa todas as tabelas de fato."""
    print("\nLimpando tabelas de fato...")

    tables = [
        "FatoCarteira",
        "FatoKPI",
        "FatoIndicadorSaneamento",
        "FatoIndicadorSaude",
        "FatoIndicadorEnergia",
        "FatoMeta2030",
        "ValidacaoEmpresa",
        "BridgeKPIODS",
        "BridgeEmpresaODS",
        "BridgeEmpresaCNAE",
    ]

    for table in tables:
        try:
            db.execute_query(f"DELETE FROM esg.{table}")
            print(f"  Tabela esg.{table} limpa.")
        except Exception as e:
            print(f"  Aviso: {table} - {e}")

    # Reseta identity das dimensoes se necessario
    dim_tables = ["DimEmpresa", "DimSubSetor", "DimProduto"]
    for table in dim_tables:
        try:
            db.execute_query(f"DELETE FROM esg.{table}")
            db.execute_query(f"DBCC CHECKIDENT ('esg.{table}', RESEED, 0)")
            print(f"  Tabela esg.{table} limpa e identity resetado.")
        except Exception as e:
            print(f"  Aviso: {table} - {e}")


def run_full_etl(truncate: bool = False):
    """Executa o ETL completo."""
    print_header()

    # Teste de conexao
    if not test_connection():
        sys.exit(1)

    # Verifica arquivos
    if not check_files():
        print("\nNenhum arquivo Excel encontrado!")
        sys.exit(1)

    # Truncate se solicitado
    if truncate:
        truncate_all()

    # Executa ETL
    print("\n" + "=" * 70)
    print("INICIANDO ETL")
    print("=" * 70)

    run_dimensoes()
    run_fatos()

    # Resumo final
    print("\n" + "=" * 70)
    print("ETL CONCLUIDO!")
    print("=" * 70)
    print(f"  Fim: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # Mostra estatisticas
    try:
        stats = db.read_sql("""
            SELECT
                'DimEmpresa' as Tabela, COUNT(*) as Registros FROM esg.DimEmpresa
            UNION ALL
            SELECT 'FatoCarteira', COUNT(*) FROM esg.FatoCarteira
            UNION ALL
            SELECT 'FatoKPI', COUNT(*) FROM esg.FatoKPI
            UNION ALL
            SELECT 'FatoIndicadorSaneamento', COUNT(*) FROM esg.FatoIndicadorSaneamento
            UNION ALL
            SELECT 'FatoIndicadorSaude', COUNT(*) FROM esg.FatoIndicadorSaude
        """)
        print("\nEstatisticas:")
        for _, row in stats.iterrows():
            print(f"  {row['Tabela']}: {row['Registros']} registros")
    except Exception as e:
        print(f"\nNao foi possivel obter estatisticas: {e}")

    print("\n" + "=" * 70)


def main():
    """Funcao principal."""
    parser = argparse.ArgumentParser(description="ETL ESG Banco Votorantim")
    parser.add_argument("--test", action="store_true", help="Apenas testar conexao")
    parser.add_argument("--dim", action="store_true", help="Apenas carregar dimensoes")
    parser.add_argument("--fato", action="store_true", help="Apenas carregar fatos")
    parser.add_argument("--truncate", action="store_true", help="Limpar tabelas antes de carregar")
    parser.add_argument("--check", action="store_true", help="Verificar arquivos")

    args = parser.parse_args()

    if args.test:
        test_connection()
    elif args.check:
        check_files()
    elif args.dim:
        print_header()
        if test_connection():
            if args.truncate:
                truncate_all()
            run_dimensoes()
    elif args.fato:
        print_header()
        if test_connection():
            run_fatos()
    else:
        run_full_etl(truncate=args.truncate)


if __name__ == "__main__":
    main()
