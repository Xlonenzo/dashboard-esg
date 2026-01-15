"""
Script para executar carga completa: JSON -> SQL Server -> Dashboard
"""

import os
import sys

# Adicionar path
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(BASE_DIR, 'etl', 'anbima'))

from etl_sql_server import SQLServerLoader, carregar_dados_json
from gerar_dashboard_sql import DashboardSQL, gerar_dashboard_html

DATA_DIR = os.path.join(BASE_DIR, 'data', 'anbima')
DASHBOARD_DIR = os.path.join(BASE_DIR, 'dashboard')


def main():
    print("=" * 60)
    print("CARGA COMPLETA: JSON -> SQL Server -> Dashboard")
    print("=" * 60)

    # Configuracao - usar localhost por padrao
    server = 'localhost'
    database = 'ANBIMA_ESG'

    # ===========================================
    # PASSO 1: Conectar ao SQL Server
    # ===========================================
    print("\n[1/3] Conectando ao SQL Server...")

    loader = SQLServerLoader(server=server, database=database)

    if not loader.conectar():
        print("\n" + "=" * 60)
        print("ERRO: Nao foi possivel conectar ao SQL Server")
        print("=" * 60)
        print("\nVerifique se:")
        print("  1. SQL Server esta rodando")
        print("  2. Banco ANBIMA_ESG foi criado")
        print("  3. ODBC Driver 17 for SQL Server instalado")
        print("\nPara criar o banco, execute:")
        print("  sqlcmd -S localhost -i sql_anbima/00_master_deploy.sql")
        return False

    print("  -> Conectado!")

    # ===========================================
    # PASSO 2: Carregar dados do JSON
    # ===========================================
    print("\n[2/3] Carregando dados do JSON...")

    dados = carregar_dados_json()

    if not dados:
        print("  -> ERRO: Nenhum dado encontrado no JSON")
        return False

    print(f"  -> Encontrados:")
    for tipo, df in dados.items():
        print(f"     - {tipo}: {len(df)} registros")

    # Executar carga
    print("\n  Inserindo no SQL Server...")
    resultado = loader.carregar_dados_completos(dados)

    print(f"  -> Resultado da carga:")
    for tabela, count in resultado.items():
        print(f"     - {tabela}: {count} registros inseridos")

    # ===========================================
    # PASSO 3: Gerar Dashboard
    # ===========================================
    print("\n[3/3] Gerando dashboard a partir do SQL Server...")

    dashboard = DashboardSQL(server=server, database=database)

    if not dashboard.conectar():
        print("  -> ERRO: Nao foi possivel conectar para gerar dashboard")
        return False

    try:
        dados_dashboard = dashboard.obter_dados_completos()

        if dados_dashboard['total_esg'] == 0:
            print("  -> AVISO: Nenhum fundo encontrado no banco")
            print("     Os dados podem nao ter sido inseridos corretamente")
        else:
            # Gerar HTML
            html = gerar_dashboard_html(dados_dashboard)

            # Salvar
            os.makedirs(DASHBOARD_DIR, exist_ok=True)
            output_file = os.path.join(DASHBOARD_DIR, 'dashboard_sql_server.html')
            with open(output_file, 'w', encoding='utf-8') as f:
                f.write(html)

            print(f"  -> Dashboard gerado: {output_file}")

    finally:
        dashboard.fechar()

    # ===========================================
    # RESUMO FINAL
    # ===========================================
    print("\n" + "=" * 60)
    print("CARGA COMPLETA FINALIZADA!")
    print("=" * 60)
    print(f"\nResumo:")
    print(f"  - Fundos carregados: {resultado.get('fundos', 0)}")
    print(f"  - Gestoras carregadas: {resultado.get('gestoras', 0)}")
    print(f"  - Dashboard: dashboard/dashboard_sql_server.html")

    return True


if __name__ == '__main__':
    success = main()
    sys.exit(0 if success else 1)
