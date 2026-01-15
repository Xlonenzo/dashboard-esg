"""
API Flask para Dashboard ANBIMA ESG
Conecta ao SQL Server e serve dados para o frontend
"""

import os
import pyodbc
from flask import Flask, jsonify, request
from flask_cors import CORS

app = Flask(__name__)
CORS(app)  # Permite requisições do dashboard

# Configuração do banco
DB_CONFIG = {
    "server": os.getenv("SQL_SERVER", "localhost"),
    "database": os.getenv("SQL_DATABASE", "ANBIMA_ESG"),
    "driver": "{ODBC Driver 17 for SQL Server}"
}


def get_connection():
    """Retorna conexão com o banco de dados."""
    conn_str = (
        f"DRIVER={DB_CONFIG['driver']};"
        f"SERVER={DB_CONFIG['server']};"
        f"DATABASE={DB_CONFIG['database']};"
        f"Trusted_Connection=yes;"
    )
    return pyodbc.connect(conn_str)


def query_to_dict(cursor):
    """Converte resultado do cursor para lista de dicionários."""
    columns = [column[0] for column in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


# ============================================================
# ENDPOINTS DE FUNDOS
# ============================================================

@app.route('/api/fundos', methods=['GET'])
def get_fundos():
    """
    Busca fundos com paginação e filtros.
    Inclui fundos de TodosFundos + GestorasSimilares (Riza, JGP, SPX, etc.)

    Query params:
    - page: número da página (default: 1)
    - per_page: itens por página (default: 50, max: 100)
    - search: termo de busca (nome, CNPJ, gestora)
    - categoria: filtrar por categoria
    - tipo: filtrar por tipo (IS, ESG, Convencional)
    - gestora: filtrar por gestora
    """
    try:
        # Parâmetros de paginação
        page = int(request.args.get('page', 1))
        per_page = min(int(request.args.get('per_page', 50)), 100)
        offset = (page - 1) * per_page

        # Parâmetros de filtro
        search = request.args.get('search', '').strip()
        categoria = request.args.get('categoria', '').strip()
        tipo = request.args.get('tipo', '').strip()

        conn = get_connection()
        cursor = conn.cursor()

        # Se tem busca, procurar em ambas as tabelas
        if search:
            search_param = f"%{search}%"

            # Query UNION para buscar em ambas as tabelas
            count_sql = """
                SELECT COUNT(*) FROM (
                    SELECT CNPJ FROM fundos.TodosFundos
                    WHERE Ativo = 1 AND (NomeComercial LIKE ? OR CNPJ LIKE ? OR RazaoSocial LIKE ?)
                    UNION ALL
                    SELECT CNPJ FROM fundos.GestorasSimilares
                    WHERE NomeCompleto LIKE ? OR CNPJ LIKE ? OR Gestora LIKE ?
                ) AS Combined
            """
            cursor.execute(count_sql, [search_param]*6)
            total = cursor.fetchone()[0]

            # Query de dados com UNION
            data_sql = """
                SELECT * FROM (
                    SELECT
                        CodigoFundo,
                        CNPJ,
                        RazaoSocial,
                        NomeComercial,
                        TipoFundo,
                        Categoria,
                        ISNULL(CategoriaESG, 'Convencional') as CategoriaESG,
                        ISNULL(FocoESG, 'Não aplicável') as FocoESG
                    FROM fundos.TodosFundos
                    WHERE Ativo = 1 AND (NomeComercial LIKE ? OR CNPJ LIKE ? OR RazaoSocial LIKE ?)

                    UNION ALL

                    SELECT
                        CAST(Id AS VARCHAR(20)) as CodigoFundo,
                        CNPJ,
                        NomeCompleto as RazaoSocial,
                        NomeCompleto as NomeComercial,
                        TipoFundo,
                        ClasseAnbima as Categoria,
                        'Gestora: ' + Gestora as CategoriaESG,
                        PublicoAlvo as FocoESG
                    FROM fundos.GestorasSimilares
                    WHERE NomeCompleto LIKE ? OR CNPJ LIKE ? OR Gestora LIKE ?
                ) AS Combined
                ORDER BY NomeComercial
                OFFSET ? ROWS FETCH NEXT ? ROWS ONLY
            """
            cursor.execute(data_sql, [search_param]*6 + [offset, per_page])
        else:
            # Sem busca - mostrar ambas as tabelas
            count_sql = """
                SELECT COUNT(*) FROM (
                    SELECT CNPJ FROM fundos.TodosFundos WHERE Ativo = 1
                    UNION ALL
                    SELECT CNPJ FROM fundos.GestorasSimilares
                ) AS Combined
            """
            cursor.execute(count_sql)
            total = cursor.fetchone()[0]

            data_sql = """
                SELECT * FROM (
                    SELECT
                        CodigoFundo,
                        CNPJ,
                        RazaoSocial,
                        NomeComercial,
                        TipoFundo,
                        Categoria,
                        ISNULL(CategoriaESG, 'Convencional') as CategoriaESG,
                        ISNULL(FocoESG, 'Não aplicável') as FocoESG
                    FROM fundos.TodosFundos
                    WHERE Ativo = 1

                    UNION ALL

                    SELECT
                        CAST(Id AS VARCHAR(20)) as CodigoFundo,
                        CNPJ,
                        NomeCompleto as RazaoSocial,
                        NomeCompleto as NomeComercial,
                        TipoFundo,
                        ClasseAnbima as Categoria,
                        'Gestora: ' + Gestora as CategoriaESG,
                        PublicoAlvo as FocoESG
                    FROM fundos.GestorasSimilares
                ) AS Combined
                ORDER BY NomeComercial
                OFFSET ? ROWS FETCH NEXT ? ROWS ONLY
            """
            cursor.execute(data_sql, [offset, per_page])

        fundos = query_to_dict(cursor)
        conn.close()

        return jsonify({
            "success": True,
            "data": fundos,
            "pagination": {
                "page": page,
                "per_page": per_page,
                "total": total,
                "total_pages": (total + per_page - 1) // per_page
            }
        })

    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route('/api/fundos/categorias', methods=['GET'])
def get_categorias():
    """Retorna lista de categorias disponíveis."""
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT DISTINCT Categoria, COUNT(*) as Qtd
            FROM fundos.TodosFundos
            WHERE Ativo = 1 AND Categoria IS NOT NULL
            GROUP BY Categoria
            ORDER BY Qtd DESC
        """)
        categorias = query_to_dict(cursor)
        conn.close()
        return jsonify({"success": True, "data": categorias})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route('/api/fundos/tipos', methods=['GET'])
def get_tipos():
    """Retorna lista de tipos ESG disponíveis."""
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT
                ISNULL(CategoriaESG, 'Convencional') as CategoriaESG,
                COUNT(*) as Qtd
            FROM fundos.TodosFundos
            WHERE Ativo = 1
            GROUP BY CategoriaESG
            ORDER BY Qtd DESC
        """)
        tipos = query_to_dict(cursor)
        conn.close()
        return jsonify({"success": True, "data": tipos})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route('/api/fundos/stats', methods=['GET'])
def get_stats():
    """Retorna estatísticas gerais dos fundos."""
    try:
        conn = get_connection()
        cursor = conn.cursor()

        # Total de fundos
        cursor.execute("SELECT COUNT(*) FROM fundos.TodosFundos WHERE Ativo = 1")
        total = cursor.fetchone()[0]

        # Por categoria ESG
        cursor.execute("""
            SELECT
                ISNULL(CategoriaESG, 'Convencional') as Tipo,
                COUNT(*) as Qtd
            FROM fundos.TodosFundos
            WHERE Ativo = 1
            GROUP BY CategoriaESG
        """)
        por_tipo = {row[0]: row[1] for row in cursor.fetchall()}

        # Por categoria
        cursor.execute("""
            SELECT TOP 10 Categoria, COUNT(*) as Qtd
            FROM fundos.TodosFundos
            WHERE Ativo = 1 AND Categoria IS NOT NULL
            GROUP BY Categoria
            ORDER BY Qtd DESC
        """)
        por_categoria = query_to_dict(cursor)

        conn.close()

        return jsonify({
            "success": True,
            "data": {
                "total": total,
                "total_is": por_tipo.get('IS - Investimento Sustentavel', 0),
                "total_esg": por_tipo.get('ESG Integrado', 0),
                "total_convencional": por_tipo.get('Convencional', 0),
                "por_categoria": por_categoria
            }
        })

    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route('/api/fundos/<codigo>', methods=['GET'])
def get_fundo_detalhe(codigo):
    """Retorna detalhes de um fundo específico."""
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT *
            FROM fundos.TodosFundos
            WHERE CodigoFundo = ? OR CNPJ = ?
        """, (codigo, codigo))
        result = query_to_dict(cursor)
        conn.close()

        if result:
            return jsonify({"success": True, "data": result[0]})
        else:
            return jsonify({"success": False, "error": "Fundo não encontrado"}), 404

    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


# ============================================================
# TSB - TAXONOMIA SUSTENTÁVEL BRASILEIRA
# ============================================================

@app.route('/api/tsb/empresas', methods=['GET'])
def get_tsb_empresas():
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT EmpresaID, Emissor, CNPJ, SetorTSB, Classificacao, Score, Titulos
            FROM tsb.EmpresasTSB ORDER BY SetorTSB, Emissor
        """)
        data = query_to_dict(cursor)
        cursor.execute("""
            SELECT SetorTSB, COUNT(*) as Qtd, AVG(Score) as ScoreMedio
            FROM tsb.EmpresasTSB GROUP BY SetorTSB
        """)
        por_setor = query_to_dict(cursor)
        cursor.execute("SELECT Classificacao, COUNT(*) as Qtd FROM tsb.EmpresasTSB GROUP BY Classificacao")
        por_class = {row[0]: row[1] for row in cursor.fetchall()}
        conn.close()
        return jsonify({
            "success": True, "data": data,
            "stats": {"total": len(data), "verde": por_class.get('VERDE', 0), "transicao": por_class.get('TRANSICAO', 0), "por_setor": por_setor}
        })
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route('/api/tsb/kpis', methods=['GET'])
def get_tsb_kpis():
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT KPIID, Setor, CodigoKPI, NomeKPI, Unidade, Frequencia, Obrigatorio FROM tsb.KPIsTSB ORDER BY Setor, CodigoKPI")
        data = query_to_dict(cursor)
        por_setor = {}
        for kpi in data:
            setor = kpi['Setor']
            if setor not in por_setor:
                por_setor[setor] = []
            por_setor[setor].append(kpi)
        conn.close()
        return jsonify({"success": True, "data": data, "por_setor": por_setor, "total": len(data)})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route('/api/tsb/empresa/<int:empresa_id>/kpis', methods=['GET'])
def get_tsb_empresa_kpis(empresa_id):
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT EmpresaID, Emissor, CNPJ, SetorTSB, Classificacao, Score, Titulos FROM tsb.EmpresasTSB WHERE EmpresaID = ?", (empresa_id,))
        empresa = query_to_dict(cursor)
        if not empresa:
            return jsonify({"success": False, "error": "Empresa não encontrada"}), 404
        empresa = empresa[0]
        setor = empresa['SetorTSB']
        setor_map = {'Energia': 'Eletricidade e Gas', 'Saneamento e Residuos': 'Agua, Esgoto, Residuos e Descontaminacao', 'Servicos Financeiros': 'Servicos Financeiros', 'Telecomunicacoes': 'Telecomunicacoes e TI'}
        setor_kpi = setor_map.get(setor, setor)
        cursor.execute("SELECT KPIID, Setor, CodigoKPI, NomeKPI, Unidade, Frequencia, Obrigatorio FROM tsb.KPIsTSB WHERE Setor = ? ORDER BY CodigoKPI", (setor_kpi,))
        kpis = query_to_dict(cursor)
        cursor.execute("SELECT CodigoKPI, Valor, Status FROM tsb.KPIsEmpresa WHERE EmpresaID = ?", (empresa_id,))
        valores = {row[0]: {'valor': row[1], 'status': row[2]} for row in cursor.fetchall()}
        for kpi in kpis:
            cod = kpi['CodigoKPI']
            if cod in valores:
                kpi['Valor'] = valores[cod]['valor']
                kpi['Status'] = valores[cod]['status']
            else:
                kpi['Valor'] = None
                kpi['Status'] = 'Pendente'
        conn.close()
        return jsonify({"success": True, "empresa": empresa, "kpis": kpis, "total_kpis": len(kpis), "kpis_preenchidos": len(valores)})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


# ============================================================
# EMISSORES
# ============================================================

@app.route('/api/emissores', methods=['GET'])
def get_emissores():
    try:
        search = request.args.get('search', '').strip()
        setor = request.args.get('setor', '').strip()
        classif = request.args.get('classificacao', '').strip()
        conn = get_connection()
        cursor = conn.cursor()
        where_clauses = ["1=1"]
        params = []
        if search:
            where_clauses.append("(Emissor LIKE ? OR CNPJ LIKE ?)")
            params.extend([f"%{search}%", f"%{search}%"])
        if setor:
            where_clauses.append("SetorTSB = ?")
            params.append(setor)
        if classif:
            where_clauses.append("Classificacao = ?")
            params.append(classif)
        where_sql = " AND ".join(where_clauses)
        cursor.execute(f"SELECT EmpresaID, CNPJ, Emissor as RazaoSocial, SetorTSB as Setor, Classificacao, Score, Titulos FROM tsb.EmpresasTSB WHERE {where_sql} ORDER BY Score DESC, Emissor", params)
        empresas = query_to_dict(cursor)
        conn.close()
        return jsonify({"success": True, "data": empresas, "total": len(empresas)})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route('/api/emissores/<path:cnpj>', methods=['GET'])
def get_emissor_detalhe(cnpj):
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT EmpresaID, CNPJ, Emissor as RazaoSocial, SetorTSB as Setor, Classificacao, Score, Titulos FROM tsb.EmpresasTSB WHERE CNPJ = ?", (cnpj,))
        result = query_to_dict(cursor)
        if not result:
            return jsonify({"success": False, "error": "Emissor não encontrado"}), 404
        empresa = result[0]
        conn.close()
        return jsonify({"success": True, "empresa": empresa, "demonstracoes": [], "governanca": [], "kpis": []})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route('/api/emissores/stats', methods=['GET'])
def get_emissores_stats():
    try:
        conn = get_connection()
        cursor = conn.cursor()
        stats = {}
        cursor.execute("SELECT COUNT(*) FROM tsb.EmpresasTSB")
        stats['total_tsb'] = cursor.fetchone()[0]
        cursor.execute("SELECT Classificacao, COUNT(*) as Qtd FROM tsb.EmpresasTSB GROUP BY Classificacao")
        stats['por_classificacao'] = {row[0]: row[1] for row in cursor.fetchall()}
        cursor.execute("SELECT TOP 10 SetorTSB, COUNT(*) as Qtd, AVG(Score) as ScoreMedio FROM tsb.EmpresasTSB GROUP BY SetorTSB ORDER BY Qtd DESC")
        stats['por_setor'] = query_to_dict(cursor)
        cursor.execute("SELECT AVG(Score) FROM tsb.EmpresasTSB")
        stats['score_medio'] = round(cursor.fetchone()[0] or 0, 1)
        conn.close()
        return jsonify({"success": True, "data": stats})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


# ============================================================
# HEALTH CHECK
# ============================================================

@app.route('/api/health', methods=['GET'])
def health_check():
    """Verifica se a API e o banco estão funcionando."""
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        cursor.fetchone()
        conn.close()
        return jsonify({"status": "healthy", "database": "connected"})
    except Exception as e:
        return jsonify({"status": "unhealthy", "database": "error", "message": str(e)}), 500


if __name__ == '__main__':
    print("=" * 60)
    print("API ANBIMA ESG - Dashboard")
    print("=" * 60)
    print(f"Servidor: {DB_CONFIG['server']}")
    print(f"Banco: {DB_CONFIG['database']}")
    print("=" * 60)
    print("Iniciando servidor em http://localhost:5000")
    print("=" * 60)
    app.run(debug=True, host='0.0.0.0', port=5000)
