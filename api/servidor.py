"""
Servidor simples para o Dashboard
Serve arquivos estáticos e redireciona API
"""
import os
from pathlib import Path
from flask import Flask, send_from_directory, jsonify, request
from flask_cors import CORS
import psycopg2
from dotenv import load_dotenv

# Carregar variáveis de ambiente do .env
load_dotenv(Path(__file__).parent / '.env')

# Caminho absoluto para a pasta dashboard
BASE_DIR = Path(__file__).resolve().parent.parent
DASHBOARD_DIR = BASE_DIR / "dashboard"

app = Flask(__name__, static_folder=str(DASHBOARD_DIR))
CORS(app)

# Import opcional do Groq (pode não estar instalado)
try:
    from groq import Groq
    GROQ_AVAILABLE = True
except ImportError:
    GROQ_AVAILABLE = False
    Groq = None

# Configuração da API Groq (defina GROQ_API_KEY como variável de ambiente)
GROQ_API_KEY = os.getenv("GROQ_API_KEY", "")
groq_client = Groq(api_key=GROQ_API_KEY) if (GROQ_AVAILABLE and GROQ_API_KEY) else None

# Configuração do banco PostgreSQL
DB_CONFIG = {
    "host": os.getenv("PG_HOST", "localhost"),
    "port": os.getenv("PG_PORT", "5432"),
    "database": os.getenv("PG_DATABASE", "anbima_esg"),
    "user": os.getenv("PG_USER", "postgres"),
    "password": os.getenv("PG_PASSWORD", ""),
}

def get_connection():
    return psycopg2.connect(**DB_CONFIG)

def query_to_dict(cursor):
    columns = [column[0] for column in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]

# Servir dashboard
@app.route('/')
def index():
    return send_from_directory(app.static_folder, 'dashboard_anbima_real.html')

@app.route('/<path:path>')
def static_files(path):
    return send_from_directory(app.static_folder, path)

# API endpoints
@app.route('/api/health')
def health():
    try:
        conn = get_connection()
        conn.close()
        return jsonify({"status": "healthy", "database": "connected"})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/api/fundos')
def get_fundos():
    try:
        page = int(request.args.get('page', 1))
        per_page = min(int(request.args.get('per_page', 50)), 100)
        offset = (page - 1) * per_page
        search = request.args.get('search', '').strip()
        categoria = request.args.get('categoria', '').strip()
        tipo = request.args.get('tipo', '').strip()
        fonte = request.args.get('fonte', '').strip()  # 'todosfundos', 'gestoras' ou vazio para todos

        conn = get_connection()
        cursor = conn.cursor()

        # Query UNION para combinar ambas as tabelas
        if fonte == 'todosfundos':
            # Apenas todosfundos
            where_clauses = ["ativo = true"]
            params = []
            if search:
                where_clauses.append("(nomecomercial ILIKE %s OR cnpj ILIKE %s OR razaosocial ILIKE %s)")
                params.extend([f"%{search}%", f"%{search}%", f"%{search}%"])
            if categoria:
                where_clauses.append("categoria = %s")
                params.append(categoria)
            where_sql = " AND ".join(where_clauses)

            cursor.execute(f"SELECT COUNT(*) FROM fundos.todosfundos WHERE {where_sql}", params)
            total = cursor.fetchone()[0]

            cursor.execute(f"""
                SELECT cnpj, razaosocial, nomecomercial, tipofundo, categoria,
                       COALESCE(categoriaesg, 'Convencional') as categoriaesg,
                       COALESCE(focoesg, 'N/A') as focoesg, NULL as gestora, 'ANBIMA' as fonte
                FROM fundos.todosfundos WHERE {where_sql}
                ORDER BY nomecomercial LIMIT %s OFFSET %s
            """, params + [per_page, offset])
        elif fonte == 'gestoras':
            # Apenas gestorassimilares
            where_clauses = ["1=1"]
            params = []
            if search:
                where_clauses.append("(nomecompleto ILIKE %s OR cnpj ILIKE %s OR gestora ILIKE %s)")
                params.extend([f"%{search}%", f"%{search}%", f"%{search}%"])
            if categoria:
                where_clauses.append("classeanbima = %s")
                params.append(categoria)
            where_sql = " AND ".join(where_clauses)

            cursor.execute(f"SELECT COUNT(*) FROM fundos.gestorassimilares WHERE {where_sql}", params)
            total = cursor.fetchone()[0]

            cursor.execute(f"""
                SELECT cnpj, nomecompleto as razaosocial, nomecompleto as nomecomercial,
                       tipofundo, classeanbima as categoria, 'Convencional' as categoriaesg,
                       publicoalvo as focoesg, gestora, 'CVM' as fonte
                FROM fundos.gestorassimilares WHERE {where_sql}
                ORDER BY nomecompleto LIMIT %s OFFSET %s
            """, params + [per_page, offset])
        else:
            # UNION de ambas as tabelas
            search_param = f"%{search}%" if search else None

            # Count total
            count_sql = """
                SELECT COUNT(*) FROM (
                    SELECT cnpj FROM fundos.todosfundos WHERE ativo = true
                    UNION ALL
                    SELECT cnpj FROM fundos.gestorassimilares
                ) combined
            """
            if search:
                count_sql = f"""
                    SELECT COUNT(*) FROM (
                        SELECT cnpj FROM fundos.todosfundos
                        WHERE ativo = true AND (nomecomercial ILIKE %s OR cnpj ILIKE %s OR razaosocial ILIKE %s)
                        UNION ALL
                        SELECT cnpj FROM fundos.gestorassimilares
                        WHERE nomecompleto ILIKE %s OR cnpj ILIKE %s OR gestora ILIKE %s
                    ) combined
                """
                cursor.execute(count_sql, [search_param]*6)
            else:
                cursor.execute(count_sql)
            total = cursor.fetchone()[0]

            # Query com UNION
            if search:
                cursor.execute("""
                    SELECT * FROM (
                        SELECT cnpj, razaosocial, nomecomercial, tipofundo, categoria,
                               COALESCE(categoriaesg, 'Convencional') as categoriaesg,
                               COALESCE(focoesg, 'N/A') as focoesg, NULL as gestora, 'ANBIMA' as fonte
                        FROM fundos.todosfundos
                        WHERE ativo = true AND (nomecomercial ILIKE %s OR cnpj ILIKE %s OR razaosocial ILIKE %s)
                        UNION ALL
                        SELECT cnpj, nomecompleto as razaosocial, nomecompleto as nomecomercial,
                               tipofundo, classeanbima as categoria, 'Convencional' as categoriaesg,
                               publicoalvo as focoesg, gestora, 'CVM' as fonte
                        FROM fundos.gestorassimilares
                        WHERE nomecompleto ILIKE %s OR cnpj ILIKE %s OR gestora ILIKE %s
                    ) combined ORDER BY nomecomercial LIMIT %s OFFSET %s
                """, [search_param]*6 + [per_page, offset])
            else:
                cursor.execute("""
                    SELECT * FROM (
                        SELECT cnpj, razaosocial, nomecomercial, tipofundo, categoria,
                               COALESCE(categoriaesg, 'Convencional') as categoriaesg,
                               COALESCE(focoesg, 'N/A') as focoesg, NULL as gestora, 'ANBIMA' as fonte
                        FROM fundos.todosfundos WHERE ativo = true
                        UNION ALL
                        SELECT cnpj, nomecompleto as razaosocial, nomecompleto as nomecomercial,
                               tipofundo, classeanbima as categoria, 'Convencional' as categoriaesg,
                               publicoalvo as focoesg, gestora, 'CVM' as fonte
                        FROM fundos.gestorassimilares
                    ) combined ORDER BY nomecomercial LIMIT %s OFFSET %s
                """, [per_page, offset])

        fundos = query_to_dict(cursor)
        conn.close()

        return jsonify({
            "success": True,
            "data": fundos,
            "pagination": {"page": page, "per_page": per_page, "total": total, "total_pages": (total + per_page - 1) // per_page}
        })
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/api/fundos/categorias')
def get_categorias():
    try:
        conn = get_connection()
        cursor = conn.cursor()
        # Combinar categorias de ambas as tabelas
        cursor.execute("""
            SELECT categoria, SUM(qtd) as qtd FROM (
                SELECT categoria, COUNT(*) as qtd FROM fundos.todosfundos
                WHERE ativo = true AND categoria IS NOT NULL GROUP BY categoria
                UNION ALL
                SELECT classeanbima as categoria, COUNT(*) as qtd FROM fundos.gestorassimilares
                WHERE classeanbima IS NOT NULL GROUP BY classeanbima
            ) combined
            GROUP BY categoria ORDER BY qtd DESC
        """)
        data = query_to_dict(cursor)
        conn.close()
        return jsonify({"success": True, "data": data})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/api/fundos/stats')
def get_stats():
    try:
        conn = get_connection()
        cursor = conn.cursor()

        # Total de fundos combinados
        cursor.execute("""
            SELECT COUNT(*) FROM (
                SELECT cnpj FROM fundos.todosfundos WHERE ativo = true
                UNION ALL
                SELECT cnpj FROM fundos.gestorassimilares
            ) combined
        """)
        total = cursor.fetchone()[0]

        # Totais por fonte
        cursor.execute("SELECT COUNT(*) FROM fundos.todosfundos WHERE ativo = true")
        total_anbima = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM fundos.gestorassimilares")
        total_cvm = cursor.fetchone()[0]

        # Por tipo ESG (apenas todosfundos)
        cursor.execute("""
            SELECT COALESCE(categoriaesg, 'Convencional') as tipo, COUNT(*) as qtd
            FROM fundos.todosfundos WHERE ativo = true GROUP BY categoriaesg
        """)
        por_tipo = {row[0]: row[1] for row in cursor.fetchall()}

        # Top categorias combinadas
        cursor.execute("""
            SELECT categoria, SUM(qtd) as qtd FROM (
                SELECT categoria, COUNT(*) as qtd FROM fundos.todosfundos
                WHERE ativo = true AND categoria IS NOT NULL GROUP BY categoria
                UNION ALL
                SELECT classeanbima as categoria, COUNT(*) as qtd FROM fundos.gestorassimilares
                WHERE classeanbima IS NOT NULL GROUP BY classeanbima
            ) combined GROUP BY categoria ORDER BY qtd DESC LIMIT 10
        """)
        por_categoria = query_to_dict(cursor)

        conn.close()
        return jsonify({
            "success": True,
            "data": {
                "total": total,
                "total_anbima": total_anbima,
                "total_cvm": total_cvm,
                "total_is": por_tipo.get('IS - Investimento Sustentavel', 0),
                "total_esg": por_tipo.get('ESG Integrado', 0),
                "total_convencional": por_tipo.get('Convencional', 0),
                "por_categoria": por_categoria
            }
        })
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

# ============================================================================
# GESTORAS
# ============================================================================
@app.route('/api/gestoras')
def get_gestoras():
    """Lista todas as gestoras com quantidade de fundos"""
    try:
        conn = get_connection()
        cursor = conn.cursor()

        # Buscar gestoras únicas com contagem de fundos
        cursor.execute("""
            SELECT gestora, COUNT(*) as qtd_fundos,
                   COUNT(DISTINCT classeanbima) as qtd_classes,
                   COUNT(DISTINCT publicoalvo) as qtd_publicos
            FROM fundos.gestorassimilares
            WHERE gestora IS NOT NULL AND gestora != ''
            GROUP BY gestora
            ORDER BY qtd_fundos DESC
        """)
        gestoras = query_to_dict(cursor)

        # Estatísticas gerais
        cursor.execute("SELECT COUNT(DISTINCT gestora) FROM fundos.gestorassimilares WHERE gestora IS NOT NULL")
        total_gestoras = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM fundos.gestorassimilares")
        total_fundos = cursor.fetchone()[0]

        # Top classes ANBIMA
        cursor.execute("""
            SELECT classeanbima, COUNT(*) as qtd
            FROM fundos.gestorassimilares
            WHERE classeanbima IS NOT NULL
            GROUP BY classeanbima
            ORDER BY qtd DESC LIMIT 10
        """)
        top_classes = query_to_dict(cursor)

        conn.close()
        return jsonify({
            "success": True,
            "data": gestoras,
            "stats": {
                "total_gestoras": total_gestoras,
                "total_fundos": total_fundos,
                "top_classes": top_classes
            }
        })
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/api/gestoras/<gestora>/fundos')
def get_fundos_gestora(gestora):
    """Lista fundos de uma gestora específica"""
    try:
        conn = get_connection()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT cnpj, nomecompleto, tipofundo, classeanbima, publicoalvo
            FROM fundos.gestorassimilares
            WHERE gestora = %s
            ORDER BY nomecompleto
        """, (gestora,))
        fundos = query_to_dict(cursor)

        # Estatísticas da gestora
        cursor.execute("""
            SELECT classeanbima, COUNT(*) as qtd
            FROM fundos.gestorassimilares
            WHERE gestora = %s AND classeanbima IS NOT NULL
            GROUP BY classeanbima
            ORDER BY qtd DESC
        """, (gestora,))
        por_classe = query_to_dict(cursor)

        cursor.execute("""
            SELECT publicoalvo, COUNT(*) as qtd
            FROM fundos.gestorassimilares
            WHERE gestora = %s AND publicoalvo IS NOT NULL
            GROUP BY publicoalvo
            ORDER BY qtd DESC
        """, (gestora,))
        por_publico = query_to_dict(cursor)

        conn.close()
        return jsonify({
            "success": True,
            "gestora": gestora,
            "fundos": fundos,
            "total": len(fundos),
            "stats": {
                "por_classe": por_classe,
                "por_publico": por_publico
            }
        })
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/api/gestoras/search')
def search_gestoras():
    """Busca gestoras por nome"""
    try:
        termo = request.args.get('q', '')
        conn = get_connection()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT gestora, COUNT(*) as qtd_fundos
            FROM fundos.gestorassimilares
            WHERE gestora ILIKE %s
            GROUP BY gestora
            ORDER BY qtd_fundos DESC
            LIMIT 50
        """, (f'%{termo}%',))
        gestoras = query_to_dict(cursor)

        conn.close()
        return jsonify({"success": True, "data": gestoras})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/api/cricra')
def get_cricra():
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT codigoativo, tipocontrato, emissor, originador, serie, emissao,
                   datavencimento, taxaindicativa, pu, duration, tiporemuneracao, taxacorrecao
            FROM titulos.cricra
            ORDER BY tipocontrato, emissor
        """)
        data = query_to_dict(cursor)

        # Estatísticas
        cursor.execute("SELECT tipocontrato, COUNT(*) as qtd FROM titulos.cricra GROUP BY tipocontrato")
        stats = {row[0]: row[1] for row in cursor.fetchall()}

        conn.close()
        return jsonify({
            "success": True,
            "data": data,
            "stats": {
                "total": len(data),
                "cri": stats.get('CRI', 0),
                "cra": stats.get('CRA', 0)
            }
        })
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/api/debentures')
def get_debentures():
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT codigoativo, emissor, grupo, percentualtaxa, taxaindicativa, pu, duration
            FROM titulos.debentures
            ORDER BY duration DESC
        """)
        data = query_to_dict(cursor)
        conn.close()
        return jsonify({"success": True, "data": data, "total": len(data)})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/api/titulos-publicos')
def get_titulos_publicos():
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM titulos.titulospublicos")
        data = query_to_dict(cursor)
        conn.close()
        return jsonify({"success": True, "data": data, "total": len(data)})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

# ============================================================
# TSB - TAXONOMIA SUSTENTÁVEL BRASILEIRA
# ============================================================

@app.route('/api/tsb/empresas')
def get_tsb_empresas():
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT empresaid, emissor, cnpj, setortsb, classificacao, score, titulos
            FROM tsb.empresastsb
            ORDER BY setortsb, emissor
        """)
        data = query_to_dict(cursor)

        # Estatísticas por setor
        cursor.execute("""
            SELECT setortsb, COUNT(*) as qtd, AVG(score) as scoremedio
            FROM tsb.empresastsb GROUP BY setortsb
        """)
        por_setor = query_to_dict(cursor)

        # Estatísticas por classificação
        cursor.execute("""
            SELECT classificacao, COUNT(*) as qtd
            FROM tsb.empresastsb GROUP BY classificacao
        """)
        por_class = {row[0]: row[1] for row in cursor.fetchall()}

        conn.close()
        return jsonify({
            "success": True,
            "data": data,
            "stats": {
                "total": len(data),
                "verde": por_class.get('VERDE', 0),
                "transicao": por_class.get('TRANSICAO', 0),
                "por_setor": por_setor
            }
        })
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/api/tsb/kpis')
def get_tsb_kpis():
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT kpiid, setor, codigokpi, nomekpi, unidade, frequencia, obrigatorio
            FROM tsb.kpistsb
            ORDER BY setor, codigokpi
        """)
        data = query_to_dict(cursor)

        # Agrupar por setor
        por_setor = {}
        for kpi in data:
            setor = kpi['setor']
            if setor not in por_setor:
                por_setor[setor] = []
            por_setor[setor].append(kpi)

        conn.close()
        return jsonify({
            "success": True,
            "data": data,
            "por_setor": por_setor,
            "total": len(data)
        })
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/api/tsb/empresa/<int:empresa_id>/kpis')
def get_tsb_empresa_kpis(empresa_id):
    try:
        conn = get_connection()
        cursor = conn.cursor()

        # Dados da empresa
        cursor.execute("""
            SELECT empresaid, emissor, cnpj, setortsb, classificacao, score, titulos
            FROM tsb.empresastsb WHERE empresaid = %s
        """, (empresa_id,))
        empresa = query_to_dict(cursor)

        if not empresa:
            return jsonify({"success": False, "error": "Empresa não encontrada"}), 404

        empresa = empresa[0]
        setor = empresa['setortsb']

        # Mapear setor para nome na tabela KPIs
        setor_map = {
            'Energia': 'Eletricidade e Gas',
            'Saneamento e Residuos': 'Agua, Esgoto, Residuos e Descontaminacao',
            'Servicos Financeiros': 'Servicos Financeiros',
            'Telecomunicacoes': 'Telecomunicacoes e TI'
        }
        setor_kpi = setor_map.get(setor, setor)

        # KPIs do setor
        cursor.execute("""
            SELECT kpiid, setor, codigokpi, nomekpi, unidade, frequencia, obrigatorio
            FROM tsb.kpistsb WHERE setor = %s
            ORDER BY codigokpi
        """, (setor_kpi,))
        kpis = query_to_dict(cursor)

        # KPIs preenchidos pela empresa (tabela pode não existir)
        valores = {}
        try:
            cursor.execute("""
                SELECT codigokpi, valor, status
                FROM tsb.kpisempresa WHERE empresaid = %s
            """, (empresa_id,))
            valores = {row[0]: {'valor': row[1], 'status': row[2]} for row in cursor.fetchall()}
        except Exception:
            # Tabela não existe, continuar sem valores preenchidos
            conn.rollback()

        # Juntar KPIs com valores
        for kpi in kpis:
            cod = kpi['codigokpi']
            if cod in valores:
                kpi['valor'] = valores[cod]['valor']
                kpi['status'] = valores[cod]['status']
            else:
                kpi['valor'] = None
                kpi['status'] = 'Pendente'

        conn.close()
        return jsonify({
            "success": True,
            "empresa": empresa,
            "kpis": kpis,
            "total_kpis": len(kpis),
            "kpis_preenchidos": len(valores)
        })
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

# ============================================================
# TSB - DADOS INTEGRADOS
# ============================================================

@app.route('/api/tsb/titulos-verdes')
def get_titulos_verdes():
    """Debêntures emitidas por empresas TSB classificadas como VERDE"""
    try:
        conn = get_connection()
        cursor = conn.cursor()

        # Debêntures de empresas TSB
        cursor.execute("""
            SELECT t.emissor, t.setortsb, t.classificacao, t.score,
                   d.codigoativo, d.grupo, d.percentualtaxa, d.taxaindicativa, d.pu, d.duration
            FROM tsb.empresastsb t
            JOIN titulos.debentures d ON LOWER(d.emissor) LIKE '%%' || LOWER(SUBSTRING(t.emissor, 1, 15)) || '%%'
            WHERE t.classificacao = 'VERDE'
            ORDER BY t.score DESC, d.duration DESC
        """)
        titulos_verde = query_to_dict(cursor)

        # Debêntures de empresas em transição
        cursor.execute("""
            SELECT t.emissor, t.setortsb, t.classificacao, t.score,
                   d.codigoativo, d.grupo, d.percentualtaxa, d.taxaindicativa, d.pu, d.duration
            FROM tsb.empresastsb t
            JOIN titulos.debentures d ON LOWER(d.emissor) LIKE '%%' || LOWER(SUBSTRING(t.emissor, 1, 15)) || '%%'
            WHERE t.classificacao = 'TRANSICAO'
            ORDER BY t.score DESC, d.duration DESC
        """)
        titulos_transicao = query_to_dict(cursor)

        # Estatísticas
        cursor.execute("""
            SELECT t.classificacao, COUNT(DISTINCT d.codigoativo) as qtd_titulos,
                   COUNT(DISTINCT t.emissor) as qtd_empresas,
                   AVG(t.score) as score_medio
            FROM tsb.empresastsb t
            JOIN titulos.debentures d ON LOWER(d.emissor) LIKE '%%' || LOWER(SUBSTRING(t.emissor, 1, 15)) || '%%'
            GROUP BY t.classificacao
        """)
        stats_raw = cursor.fetchall()
        stats = {row[0]: {'titulos': row[1], 'empresas': row[2], 'score_medio': float(row[3]) if row[3] else 0} for row in stats_raw}

        conn.close()
        return jsonify({
            "success": True,
            "titulos_verde": titulos_verde,
            "titulos_transicao": titulos_transicao,
            "stats": {
                "total_verde": len(titulos_verde),
                "total_transicao": len(titulos_transicao),
                "empresas_verde": stats.get('VERDE', {}).get('empresas', 0),
                "empresas_transicao": stats.get('TRANSICAO', {}).get('empresas', 0),
                "score_medio_verde": round(stats.get('VERDE', {}).get('score_medio', 0), 1),
                "score_medio_transicao": round(stats.get('TRANSICAO', {}).get('score_medio', 0), 1)
            }
        })
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/api/tsb/fundos-sustentaveis')
def get_fundos_sustentaveis():
    """Fundos com foco ESG/Sustentabilidade"""
    try:
        conn = get_connection()
        cursor = conn.cursor()

        # Fundos ESG/Sustentáveis
        cursor.execute("""
            SELECT cnpj, nomecompleto, tipofundo, classeanbima, gestora, publicoalvo
            FROM fundos.gestorassimilares
            WHERE LOWER(nomecompleto) LIKE '%%sustent%%'
               OR LOWER(nomecompleto) LIKE '%%esg%%'
               OR LOWER(nomecompleto) LIKE '%%verde%%'
               OR LOWER(nomecompleto) LIKE '%%social%%'
               OR LOWER(nomecompleto) LIKE '%%ambiental%%'
               OR LOWER(nomecompleto) LIKE '%%carbono%%'
               OR LOWER(nomecompleto) LIKE '%%clima%%'
            ORDER BY gestora, nomecompleto
        """)
        fundos = query_to_dict(cursor)

        # Por gestora
        cursor.execute("""
            SELECT gestora, COUNT(*) as qtd
            FROM fundos.gestorassimilares
            WHERE LOWER(nomecompleto) LIKE '%%sustent%%'
               OR LOWER(nomecompleto) LIKE '%%esg%%'
               OR LOWER(nomecompleto) LIKE '%%verde%%'
               OR LOWER(nomecompleto) LIKE '%%social%%'
               OR LOWER(nomecompleto) LIKE '%%ambiental%%'
               OR LOWER(nomecompleto) LIKE '%%carbono%%'
               OR LOWER(nomecompleto) LIKE '%%clima%%'
            GROUP BY gestora ORDER BY qtd DESC
        """)
        por_gestora = query_to_dict(cursor)

        # Por classe ANBIMA
        cursor.execute("""
            SELECT classeanbima, COUNT(*) as qtd
            FROM fundos.gestorassimilares
            WHERE (LOWER(nomecompleto) LIKE '%%sustent%%'
               OR LOWER(nomecompleto) LIKE '%%esg%%'
               OR LOWER(nomecompleto) LIKE '%%verde%%'
               OR LOWER(nomecompleto) LIKE '%%social%%'
               OR LOWER(nomecompleto) LIKE '%%ambiental%%')
               AND classeanbima IS NOT NULL
            GROUP BY classeanbima ORDER BY qtd DESC LIMIT 10
        """)
        por_classe = query_to_dict(cursor)

        conn.close()
        return jsonify({
            "success": True,
            "fundos": fundos,
            "total": len(fundos),
            "stats": {
                "por_gestora": por_gestora,
                "por_classe": por_classe,
                "total_gestoras": len(por_gestora)
            }
        })
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/api/tsb/empresa/<int:empresa_id>/investimentos')
def get_empresa_investimentos(empresa_id):
    """Visão integrada: empresa TSB com títulos e fundos relacionados"""
    try:
        conn = get_connection()
        cursor = conn.cursor()

        # Dados da empresa
        cursor.execute("""
            SELECT empresaid, emissor, cnpj, setortsb, classificacao, score, titulos
            FROM tsb.empresastsb WHERE empresaid = %s
        """, (empresa_id,))
        row = cursor.fetchone()
        if not row:
            return jsonify({"success": False, "error": "Empresa não encontrada"}), 404

        empresa = {
            'empresaid': row[0], 'emissor': row[1], 'cnpj': row[2],
            'setortsb': row[3], 'classificacao': row[4], 'score': float(row[5]) if row[5] else 0,
            'titulos': row[6]
        }

        # Debêntures relacionadas
        cursor.execute("""
            SELECT codigoativo, emissor, grupo, percentualtaxa, taxaindicativa, pu, duration
            FROM titulos.debentures
            WHERE LOWER(emissor) LIKE '%%' || LOWER(SUBSTRING(%s, 1, 15)) || '%%'
            ORDER BY duration DESC
        """, (empresa['emissor'],))
        debentures = query_to_dict(cursor)

        # CRI/CRA relacionados
        cursor.execute("""
            SELECT codigoativo, tipocontrato, emissor, serie, taxaindicativa, pu, duration
            FROM titulos.cricra
            WHERE LOWER(emissor) LIKE '%%' || LOWER(SUBSTRING(%s, 1, 15)) || '%%'
               OR LOWER(originador) LIKE '%%' || LOWER(SUBSTRING(%s, 1, 15)) || '%%'
        """, (empresa['emissor'], empresa['emissor']))
        cricra = query_to_dict(cursor)

        # Fundos que podem investir nesta empresa (pelo setor)
        cursor.execute("""
            SELECT DISTINCT cnpj, nomecompleto, gestora, classeanbima
            FROM fundos.gestorassimilares
            WHERE LOWER(nomecompleto) LIKE '%%' || LOWER(SUBSTRING(%s, 1, 10)) || '%%'
               OR (LOWER(nomecompleto) LIKE '%%sustent%%' AND %s = 'VERDE')
               OR (LOWER(nomecompleto) LIKE '%%energia%%' AND %s = 'Energia')
               OR (LOWER(nomecompleto) LIKE '%%infra%%' AND %s IN ('Energia', 'Transportes', 'Saneamento e Residuos'))
            LIMIT 20
        """, (empresa['emissor'], empresa['classificacao'], empresa['setortsb'], empresa['setortsb']))
        fundos_relacionados = query_to_dict(cursor)

        conn.close()
        return jsonify({
            "success": True,
            "empresa": empresa,
            "debentures": debentures,
            "cricra": cricra,
            "fundos_relacionados": fundos_relacionados,
            "stats": {
                "total_debentures": len(debentures),
                "total_cricra": len(cricra),
                "total_fundos": len(fundos_relacionados)
            }
        })
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/api/tsb/visao-geral')
def get_tsb_visao_geral():
    """Dashboard consolidado TSB com todas as métricas"""
    try:
        conn = get_connection()
        cursor = conn.cursor()

        # Total empresas TSB
        cursor.execute("SELECT COUNT(*), AVG(score) FROM tsb.empresastsb")
        row = cursor.fetchone()
        total_empresas = row[0]
        score_medio = float(row[1]) if row[1] else 0

        # Por classificação
        cursor.execute("SELECT classificacao, COUNT(*), AVG(score) FROM tsb.empresastsb GROUP BY classificacao")
        por_class = {r[0]: {'qtd': r[1], 'score': float(r[2]) if r[2] else 0} for r in cursor.fetchall()}

        # Por setor
        cursor.execute("""
            SELECT setortsb, COUNT(*) as qtd, AVG(score) as score_medio
            FROM tsb.empresastsb GROUP BY setortsb ORDER BY qtd DESC
        """)
        por_setor = query_to_dict(cursor)

        # Títulos vinculados
        cursor.execute("""
            SELECT COUNT(DISTINCT d.codigoativo)
            FROM tsb.empresastsb t
            JOIN titulos.debentures d ON LOWER(d.emissor) LIKE '%%' || LOWER(SUBSTRING(t.emissor, 1, 15)) || '%%'
        """)
        total_debentures_tsb = cursor.fetchone()[0]

        # Fundos sustentáveis
        cursor.execute("""
            SELECT COUNT(*) FROM fundos.gestorassimilares
            WHERE LOWER(nomecompleto) LIKE '%%sustent%%'
               OR LOWER(nomecompleto) LIKE '%%esg%%'
               OR LOWER(nomecompleto) LIKE '%%verde%%'
        """)
        total_fundos_esg = cursor.fetchone()[0]

        # Top empresas por score
        cursor.execute("""
            SELECT emissor, setortsb, classificacao, score
            FROM tsb.empresastsb ORDER BY score DESC LIMIT 10
        """)
        top_empresas = query_to_dict(cursor)

        conn.close()
        return jsonify({
            "success": True,
            "stats": {
                "total_empresas": total_empresas,
                "score_medio": round(score_medio, 1),
                "empresas_verde": por_class.get('VERDE', {}).get('qtd', 0),
                "empresas_transicao": por_class.get('TRANSICAO', {}).get('qtd', 0),
                "score_verde": round(por_class.get('VERDE', {}).get('score', 0), 1),
                "score_transicao": round(por_class.get('TRANSICAO', {}).get('score', 0), 1),
                "total_debentures_tsb": total_debentures_tsb,
                "total_fundos_esg": total_fundos_esg,
                "total_setores": len(por_setor)
            },
            "por_setor": por_setor,
            "top_empresas": top_empresas
        })
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

# ============================================================
# EMISSORES - Empresas de Capital Aberto
# ============================================================

@app.route('/api/emissores')
def get_emissores():
    """Lista emissores - prioriza TSB com dados completos"""
    try:
        search = request.args.get('search', '').strip()
        setor = request.args.get('setor', '').strip()
        classif = request.args.get('classificacao', '').strip()

        conn = get_connection()
        cursor = conn.cursor()

        # Buscar dados TSB (tem Classificacao, Score, etc)
        where_clauses = ["1=1"]
        params = []

        if search:
            where_clauses.append("(emissor ILIKE %s OR cnpj ILIKE %s)")
            params.extend([f"%{search}%", f"%{search}%"])
        if setor:
            where_clauses.append("setortsb = %s")
            params.append(setor)
        if classif:
            where_clauses.append("classificacao = %s")
            params.append(classif)

        where_sql = " AND ".join(where_clauses)

        cursor.execute(f"""
            SELECT empresaid, cnpj, emissor as razaosocial, setortsb as setor,
                   classificacao, score, titulos
            FROM tsb.empresastsb
            WHERE {where_sql}
            ORDER BY score DESC, emissor
        """, params)

        empresas = query_to_dict(cursor)
        total = len(empresas)

        conn.close()
        return jsonify({
            "success": True,
            "data": empresas,
            "total": total
        })
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/api/emissores/<path:cnpj>')
def get_emissor_detalhe(cnpj):
    """Detalhes de um emissor especifico"""
    try:
        conn = get_connection()
        cursor = conn.cursor()

        # Dados basicos - tenta emissores, depois TSB
        empresa = None
        try:
            cursor.execute("""
                SELECT empresaid, cnpj, razaosocial, codigocvm, setor
                FROM emissores.empresas WHERE cnpj = %s
            """, (cnpj,))
            result = query_to_dict(cursor)
            if result:
                empresa = result[0]
        except:
            pass

        if not empresa:
            cursor.execute("""
                SELECT empresaid, cnpj, emissor as razaosocial, setortsb as setor,
                       classificacao, score, titulos
                FROM tsb.empresastsb WHERE cnpj = %s
            """, (cnpj,))
            result = query_to_dict(cursor)
            if result:
                empresa = result[0]

        if not empresa:
            return jsonify({"success": False, "error": "Emissor nao encontrado"}), 404

        # Dados financeiros (DRE)
        dre = []
        try:
            cursor.execute("""
                SELECT codigoconta, descricaoconta, valor, anoexercicio
                FROM emissores.demonstracoesfinanceiras
                WHERE cnpj = %s AND tipodemonstracao = 'DRE'
                ORDER BY anoexercicio DESC, codigoconta
                LIMIT 20
            """, (cnpj,))
            dre = query_to_dict(cursor)
        except:
            pass

        # Governanca
        governanca = []
        try:
            cursor.execute("""
                SELECT capitulo, principio, praticaadotada
                FROM emissores.governanca
                WHERE cnpj = %s
                ORDER BY capitulo
                LIMIT 30
            """, (cnpj,))
            governanca = query_to_dict(cursor)
        except:
            pass

        # KPIs TSB
        kpis = []
        try:
            cursor.execute("""
                SELECT k.codigokpi, kd.nomekpi, k.valor, k.status, kd.unidade
                FROM tsb.kpisempresa k
                JOIN tsb.kpistsb kd ON k.codigokpi = kd.codigokpi
                JOIN tsb.empresastsb e ON k.empresaid = e.empresaid
                WHERE e.cnpj = %s
            """, (cnpj,))
            kpis = query_to_dict(cursor)
        except:
            pass

        conn.close()
        return jsonify({
            "success": True,
            "empresa": empresa,
            "demonstracoes": dre,
            "governanca": governanca,
            "kpis": kpis
        })
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/api/emissores/stats')
def get_emissores_stats():
    """Estatisticas dos emissores"""
    try:
        conn = get_connection()
        cursor = conn.cursor()

        stats = {}

        # Total TSB
        cursor.execute("SELECT COUNT(*) FROM tsb.empresastsb")
        stats['total_tsb'] = cursor.fetchone()[0]

        # Por classificacao
        cursor.execute("""
            SELECT classificacao, COUNT(*) as qtd
            FROM tsb.empresastsb GROUP BY classificacao
        """)
        stats['por_classificacao'] = {row[0]: row[1] for row in cursor.fetchall()}

        # Por setor
        cursor.execute("""
            SELECT setortsb, COUNT(*) as qtd, AVG(score) as scoremedio
            FROM tsb.empresastsb GROUP BY setortsb ORDER BY qtd DESC LIMIT 10
        """)
        stats['por_setor'] = query_to_dict(cursor)

        # Score medio
        cursor.execute("SELECT AVG(score) FROM tsb.empresastsb")
        stats['score_medio'] = round(cursor.fetchone()[0] or 0, 1)

        # Total emissores CVM (se existir)
        try:
            cursor.execute("SELECT COUNT(*) FROM emissores.empresas")
            stats['total_emissores_cvm'] = cursor.fetchone()[0]
        except:
            stats['total_emissores_cvm'] = 0

        conn.close()
        return jsonify({"success": True, "data": stats})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

# ============================================================
# CONSULTA IA - Assistente Inteligente
# ============================================================

@app.route('/api/ai/consulta', methods=['POST'])
def ai_consulta():
    """
    Endpoint para consultas de IA.
    Recebe: mensagem, tipo_resposta, contexto, historico
    Retorna: resposta formatada com análises e dados
    """
    try:
        data = request.get_json()
        mensagem = data.get('mensagem', '').strip()
        tipo_resposta = data.get('tipo_resposta', 'texto')
        contexto = data.get('contexto', 'todos')
        historico = data.get('historico', [])

        if not mensagem:
            return jsonify({"success": False, "error": "Mensagem vazia"}), 400

        # Analisar a mensagem e gerar resposta
        resposta = processar_consulta_ia(mensagem, tipo_resposta, contexto)

        return jsonify({
            "success": True,
            "resposta": resposta['texto'],
            "tipo": resposta.get('tipo', 'texto'),
            "dados_estruturados": resposta.get('dados')
        })
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

def obter_contexto_dados():
    """Obtém dados do banco para contextualizar a IA"""
    contexto = {}
    try:
        conn = get_connection()
        cursor = conn.cursor()

        # Estatísticas gerais de fundos
        cursor.execute("SELECT COUNT(*) FROM fundos.todosfundos WHERE ativo = true")
        contexto['total_fundos'] = cursor.fetchone()[0]

        cursor.execute("""
            SELECT categoriaesg, COUNT(*) as qtd
            FROM fundos.todosfundos
            WHERE ativo = true AND categoriaesg IS NOT NULL
            GROUP BY categoriaesg
        """)
        contexto['fundos_esg'] = {row[0]: row[1] for row in cursor.fetchall()}

        cursor.execute("""
            SELECT categoria, COUNT(*) as qtd
            FROM fundos.todosfundos WHERE ativo = true
            GROUP BY categoria ORDER BY qtd DESC LIMIT 5
        """)
        contexto['categorias_fundos'] = query_to_dict(cursor)

        # Debêntures
        cursor.execute("""
            SELECT emissor, codigoativo, grupo, duration, percentualtaxa, pu
            FROM titulos.debentures ORDER BY duration DESC LIMIT 10
        """)
        contexto['debentures'] = query_to_dict(cursor)

        cursor.execute("SELECT COUNT(*) FROM titulos.debentures")
        contexto['total_debentures'] = cursor.fetchone()[0]

        # Títulos Públicos
        cursor.execute("""
            SELECT tipo as tipotitulo, COUNT(*) as qtd, AVG(taxaindicativa) as taxamedia
            FROM titulos.titulospublicos GROUP BY tipo
        """)
        contexto['titulos_publicos'] = query_to_dict(cursor)

        # TSB
        cursor.execute("""
            SELECT emissor, setortsb, classificacao, score
            FROM tsb.empresastsb ORDER BY score DESC
        """)
        contexto['empresas_tsb'] = query_to_dict(cursor)

        # CRI/CRA
        cursor.execute("SELECT COUNT(*) as total, tipocontrato FROM titulos.cricra GROUP BY tipocontrato")
        contexto['cricra'] = {row[1]: row[0] for row in cursor.fetchall()}

        # Emissores - Empresas CVM
        cursor.execute("SELECT COUNT(*) FROM emissores.empresas")
        contexto['total_emissores'] = cursor.fetchone()[0]

        # Top empresas por receita (DRE)
        cursor.execute("""
            SELECT
                e.razaosocial,
                d.valor / 1000 as receitamilhoes,
                d.anoexercicio
            FROM emissores.demonstracoesfinanceiras d
            JOIN emissores.empresas e ON d.cnpj = e.cnpj
            WHERE d.codigoconta = '3.01' AND d.anoexercicio >= 2023
            ORDER BY d.valor DESC
            LIMIT 15
        """)
        contexto['top_receitas'] = query_to_dict(cursor)

        # Governanca - resumo por capitulo
        cursor.execute("""
            SELECT capitulo, COUNT(*) as qtd,
                SUM(CASE WHEN praticaadotada = 'Sim' THEN 1 ELSE 0 END) as adotadas
            FROM emissores.governanca
            WHERE anoreferencia >= 2023
            GROUP BY capitulo
            ORDER BY qtd DESC
            LIMIT 10
        """)
        contexto['governanca_resumo'] = query_to_dict(cursor)

        conn.close()
    except Exception as e:
        contexto['erro'] = str(e)

    return contexto

def processar_consulta_ia(mensagem, tipo_resposta, contexto_filtro):
    """Processa a consulta usando a API Groq com dados reais do banco"""
    resultado = {'texto': '', 'tipo': tipo_resposta, 'dados': None}

    try:
        # Obter dados do banco para contexto
        dados = obter_contexto_dados()

        # Construir contexto para a IA
        contexto_sistema = f"""Você é um assistente de consulta de dados financeiros. Você APENAS responde com base nos dados fornecidos abaixo.

⚠️ REGRAS CRÍTICAS - NUNCA QUEBRE ESTAS REGRAS:
1. NUNCA invente dados, nomes, números ou informações
2. NUNCA use conhecimento externo - USE APENAS os dados abaixo
3. Se a informação NÃO está nos dados abaixo, responda: "Não tenho essa informação no banco de dados."
4. Cite APENAS os nomes, números e valores EXATOS que estão listados abaixo
5. Se o usuário perguntar sobre algo específico que não está nos dados, diga claramente que não está disponível

===== DADOS DO BANCO DE DADOS =====

📊 FUNDOS DE INVESTIMENTO:
- Total de fundos ativos: {dados.get('total_fundos', 0):,}
- Fundos IS (Investimento Sustentável): {dados.get('fundos_esg', {}).get('IS - Investimento Sustentavel', 0)}
- Fundos ESG Integrado: {dados.get('fundos_esg', {}).get('ESG Integrado', 0)}
- Categorias disponíveis: {', '.join([f"{c['Categoria']} ({c['Qtd']})" for c in dados.get('categorias_fundos', [])[:5]]) if dados.get('categorias_fundos') else 'Nenhuma'}

📜 DEBÊNTURES (Total: {dados.get('total_debentures', 0)}):
{chr(10).join([f"- {d['CodigoAtivo']}: {d['Emissor']}, Duration {int(d['Duration'] or 0)} dias, Taxa: {d['PercentualTaxa']}" for d in dados.get('debentures', [])[:10]]) if dados.get('debentures') else 'Nenhuma debênture disponível'}

🏛️ TÍTULOS PÚBLICOS:
{chr(10).join([f"- {t['TipoTitulo']}: {t['Qtd']} títulos, taxa média {t['TaxaMedia']:.2f}%" for t in dados.get('titulos_publicos', [])]) if dados.get('titulos_publicos') else 'Nenhum título disponível'}

🌿 EMPRESAS TSB (Taxonomia Sustentável Brasileira):
{chr(10).join([f"- {e['Emissor']}: Setor {e['SetorTSB']}, Classificação {e['Classificacao']}, Score {e['Score']}" for e in dados.get('empresas_tsb', [])]) if dados.get('empresas_tsb') else 'Nenhuma empresa disponível'}

🏠 CRI/CRA:
- Total CRI: {dados.get('cricra', {}).get('CRI', 0)}
- Total CRA: {dados.get('cricra', {}).get('CRA', 0)}

🏢 EMISSORES CVM (Total: {dados.get('total_emissores', 0)}):
TOP EMPRESAS POR RECEITA (DRE):
{chr(10).join([f"- {r['RazaoSocial'][:50]}: R$ {r['ReceitaMilhoes']:,.0f} milhoes ({r['AnoExercicio']})" for r in dados.get('top_receitas', [])[:10]]) if dados.get('top_receitas') else 'Nenhuma empresa disponivel'}

📋 GOVERNANCA CORPORATIVA:
{chr(10).join([f"- {g['Capitulo']}: {g['Qtd']} praticas, {g['Adotadas']} adotadas" for g in dados.get('governanca_resumo', [])[:5]]) if dados.get('governanca_resumo') else 'Nenhum dado disponivel'}

===== FIM DOS DADOS =====

INSTRUÇÕES DE RESPOSTA:
1. Responda em português brasileiro
2. Use APENAS os dados listados acima - NADA MAIS
3. Formate em HTML simples (<p>, <strong>, <table>, <tr>, <th>, <td>, <ul>, <li>)
4. Se não tiver a informação, diga: "Essa informação não está disponível no banco de dados atual."
5. Nunca invente fundos, empresas, taxas ou valores que não estejam listados acima"""

        # Chamar API Groq
        chat_completion = groq_client.chat.completions.create(
            messages=[
                {"role": "system", "content": contexto_sistema},
                {"role": "user", "content": mensagem}
            ],
            model="llama-3.3-70b-versatile",
            temperature=0.7,
            max_tokens=2000
        )

        resposta_ia = chat_completion.choices[0].message.content

        # Garantir que a resposta está em HTML
        if not resposta_ia.strip().startswith('<'):
            resposta_ia = f"<p>{resposta_ia}</p>"

        # Substituir quebras de linha por tags HTML
        resposta_ia = resposta_ia.replace('\n\n', '</p><p>').replace('\n', '<br>')

        resultado['texto'] = resposta_ia

    except Exception as e:
        # Fallback para resposta local se Groq falhar
        resultado['texto'] = f"""<p>⚠️ Erro ao processar com IA: {str(e)}</p>
        <p>Usando análise local dos dados...</p>
        {gerar_resposta_fallback(mensagem)}"""

    return resultado

def gerar_resposta_fallback(mensagem):
    """Gera resposta de fallback quando a API Groq não está disponível"""
    msg_lower = mensagem.lower()

    try:
        conn = get_connection()
        cursor = conn.cursor()

        if 'esg' in msg_lower or 'sustent' in msg_lower:
            cursor.execute("SELECT COUNT(*) FROM fundos.todosfundos WHERE categoriaesg IS NOT NULL AND ativo = true")
            total = cursor.fetchone()[0]
            conn.close()
            return f"<p>📊 Temos {total} fundos ESG/IS cadastrados no sistema.</p>"

        elif 'debenture' in msg_lower or 'risco' in msg_lower:
            cursor.execute("SELECT COUNT(*) FROM titulos.debentures")
            total = cursor.fetchone()[0]
            conn.close()
            return f"<p>📈 Temos {total} debêntures no sistema para análise.</p>"

        elif 'tsb' in msg_lower or 'verde' in msg_lower:
            cursor.execute("SELECT COUNT(*) FROM tsb.empresastsb WHERE classificacao = 'VERDE'")
            total = cursor.fetchone()[0]
            conn.close()
            return f"<p>🌿 Temos {total} empresas com classificação Verde na TSB.</p>"

        conn.close()
    except:
        pass

    return "<p>Por favor, tente novamente ou use uma das sugestões rápidas.</p>"

# ============================================================
# RISK SCORING - ANÁLISE DE RISCO DO PORTFÓLIO
# ============================================================

@app.route('/api/risk-scoring')
def get_risk_scoring():
    """Análise completa de risco do portfólio"""
    try:
        conn = get_connection()
        cursor = conn.cursor()

        # ========== 1. RISCO ESG (baseado em TSB) ==========
        cursor.execute("""
            SELECT
                COUNT(*) as total,
                SUM(CASE WHEN classificacao = 'VERDE' THEN 1 ELSE 0 END) as verde,
                SUM(CASE WHEN classificacao = 'TRANSICAO' THEN 1 ELSE 0 END) as transicao,
                AVG(score) as score_medio,
                MIN(score) as score_min,
                MAX(score) as score_max
            FROM tsb.empresastsb
        """)
        row = cursor.fetchone()
        esg_stats = {
            'total_empresas': row[0] or 0,
            'empresas_verde': row[1] or 0,
            'empresas_transicao': row[2] or 0,
            'score_medio': round(float(row[3]), 1) if row[3] else 0,
            'score_min': round(float(row[4]), 1) if row[4] else 0,
            'score_max': round(float(row[5]), 1) if row[5] else 0
        }
        # Score ESG do portfólio (0-100, quanto maior melhor)
        esg_score = esg_stats['score_medio']
        pct_verde = (esg_stats['empresas_verde'] / esg_stats['total_empresas'] * 100) if esg_stats['total_empresas'] > 0 else 0

        # ========== 2. RISCO DE CONCENTRAÇÃO POR SETOR ==========
        cursor.execute("""
            SELECT setortsb, COUNT(*) as qtd, AVG(score) as score_medio
            FROM tsb.empresastsb
            GROUP BY setortsb
            ORDER BY qtd DESC
        """)
        setores = query_to_dict(cursor)
        total_emp = sum(s['qtd'] for s in setores)

        # Calcular HHI (Herfindahl-Hirschman Index) para concentração
        hhi = sum((s['qtd'] / total_emp * 100) ** 2 for s in setores) if total_emp > 0 else 0
        concentracao_setor = 'Alta' if hhi > 2500 else 'Moderada' if hhi > 1500 else 'Baixa'

        # ========== 3. RISCO DE CRÉDITO (Debêntures) ==========
        cursor.execute("""
            SELECT
                COUNT(*) as total,
                AVG(CAST(taxaindicativa AS FLOAT)) as taxa_media,
                AVG(CAST(duration AS FLOAT)) as duration_media,
                COUNT(CASE WHEN CAST(taxaindicativa AS FLOAT) > 6 THEN 1 END) as alto_spread,
                COUNT(CASE WHEN CAST(taxaindicativa AS FLOAT) BETWEEN 4 AND 6 THEN 1 END) as medio_spread,
                COUNT(CASE WHEN CAST(taxaindicativa AS FLOAT) < 4 THEN 1 END) as baixo_spread
            FROM titulos.debentures
            WHERE taxaindicativa IS NOT NULL AND taxaindicativa != ''
        """)
        row = cursor.fetchone()
        credito_stats = {
            'total_debentures': row[0] or 0,
            'taxa_media': round(float(row[1]), 2) if row[1] else 0,
            'duration_media': round(float(row[2]), 0) if row[2] else 0,
            'alto_spread': row[3] or 0,
            'medio_spread': row[4] or 0,
            'baixo_spread': row[5] or 0
        }

        # Por grupo de indexador
        cursor.execute("""
            SELECT grupo, COUNT(*) as qtd, AVG(CAST(taxaindicativa AS FLOAT)) as taxa_media
            FROM titulos.debentures
            WHERE grupo IS NOT NULL
            GROUP BY grupo
            ORDER BY qtd DESC
        """)
        por_indexador = query_to_dict(cursor)

        # ========== 4. RISCO DE CRI/CRA ==========
        cursor.execute("""
            SELECT
                COUNT(*) as total,
                SUM(CASE WHEN tipocontrato = 'CRI' THEN 1 ELSE 0 END) as cri,
                SUM(CASE WHEN tipocontrato = 'CRA' THEN 1 ELSE 0 END) as cra,
                AVG(CAST(taxaindicativa AS FLOAT)) as taxa_media,
                AVG(CAST(duration AS FLOAT)) as duration_media
            FROM titulos.cricra
            WHERE taxaindicativa IS NOT NULL
        """)
        row = cursor.fetchone()
        cricra_stats = {
            'total': row[0] or 0,
            'cri': row[1] or 0,
            'cra': row[2] or 0,
            'taxa_media': round(float(row[3]), 2) if row[3] else 0,
            'duration_media': round(float(row[4]), 0) if row[4] else 0
        }

        # ========== 5. RISCO DE LIQUIDEZ (Duration) ==========
        cursor.execute("""
            SELECT
                COUNT(CASE WHEN CAST(duration AS FLOAT) <= 365 THEN 1 END) as curto_prazo,
                COUNT(CASE WHEN CAST(duration AS FLOAT) > 365 AND CAST(duration AS FLOAT) <= 1095 THEN 1 END) as medio_prazo,
                COUNT(CASE WHEN CAST(duration AS FLOAT) > 1095 THEN 1 END) as longo_prazo
            FROM titulos.debentures
            WHERE duration IS NOT NULL AND duration != ''
        """)
        row = cursor.fetchone()
        liquidez_stats = {
            'curto_prazo': row[0] or 0,
            'medio_prazo': row[1] or 0,
            'longo_prazo': row[2] or 0
        }

        # ========== 6. FUNDOS ESG ==========
        cursor.execute("""
            SELECT COUNT(*) FROM fundos.gestorassimilares
            WHERE LOWER(nomecompleto) LIKE '%%sustent%%'
               OR LOWER(nomecompleto) LIKE '%%esg%%'
               OR LOWER(nomecompleto) LIKE '%%verde%%'
               OR LOWER(nomecompleto) LIKE '%%clima%%'
        """)
        total_fundos_esg = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM fundos.gestorassimilares")
        total_fundos = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM fundos.todosfundos")
        total_fundos_anbima = cursor.fetchone()[0]

        # ========== 7. CÁLCULO DOS SCORES DE RISCO ==========
        # Score ESG (0-100, quanto maior melhor)
        risk_esg = 100 - esg_score  # Inverso: alto ESG = baixo risco

        # Score de Crédito (baseado em spread médio)
        # Taxa > 6% = alto risco, Taxa < 3% = baixo risco
        taxa_media = credito_stats['taxa_media']
        risk_credito = min(100, max(0, (taxa_media - 2) * 20))

        # Score de Concentração (baseado em HHI)
        # HHI > 2500 = alto risco, HHI < 1000 = baixo risco
        risk_concentracao = min(100, max(0, (hhi - 500) / 30))

        # Score de Liquidez (baseado em duration)
        pct_longo = (liquidez_stats['longo_prazo'] / max(1, sum(liquidez_stats.values()))) * 100
        risk_liquidez = min(100, max(0, pct_longo * 1.5))

        # Score de Clima (baseado em % transição)
        pct_transicao = (esg_stats['empresas_transicao'] / max(1, esg_stats['total_empresas'])) * 100
        risk_clima = min(100, max(0, pct_transicao * 2))

        # Score Global (média ponderada)
        risk_global = round(
            risk_esg * 0.25 +
            risk_credito * 0.25 +
            risk_concentracao * 0.15 +
            risk_liquidez * 0.15 +
            risk_clima * 0.20
        , 1)

        # Classificação de risco
        def get_rating(score):
            if score <= 20: return {'grade': 'A', 'label': 'Muito Baixo', 'color': '#4CAF50'}
            elif score <= 40: return {'grade': 'B', 'label': 'Baixo', 'color': '#8BC34A'}
            elif score <= 60: return {'grade': 'C', 'label': 'Moderado', 'color': '#FFC107'}
            elif score <= 80: return {'grade': 'D', 'label': 'Alto', 'color': '#FF9800'}
            else: return {'grade': 'E', 'label': 'Muito Alto', 'color': '#F44336'}

        # ========== 8. EMPRESAS COM MAIOR RISCO ==========
        cursor.execute("""
            SELECT emissor, setortsb, classificacao, score
            FROM tsb.empresastsb
            ORDER BY score ASC
            LIMIT 10
        """)
        empresas_maior_risco = query_to_dict(cursor)

        # ========== 9. TOP EMISSORES DE DEBÊNTURES ==========
        cursor.execute("""
            SELECT emissor, COUNT(*) as qtd, AVG(CAST(taxaindicativa AS FLOAT)) as taxa_media
            FROM titulos.debentures
            WHERE taxaindicativa IS NOT NULL AND taxaindicativa != ''
            GROUP BY emissor
            ORDER BY qtd DESC
            LIMIT 10
        """)
        top_emissores = query_to_dict(cursor)

        conn.close()

        return jsonify({
            "success": True,
            "resumo": {
                "risk_score_global": risk_global,
                "rating": get_rating(risk_global),
                "total_ativos": credito_stats['total_debentures'] + cricra_stats['total'] + total_fundos + total_fundos_anbima,
                "total_empresas_tsb": esg_stats['total_empresas'],
                "patrimonio_estimado": "R$ 3.35B"  # Valor ilustrativo
            },
            "scores": {
                "esg": {"valor": round(risk_esg, 1), "rating": get_rating(risk_esg)},
                "credito": {"valor": round(risk_credito, 1), "rating": get_rating(risk_credito)},
                "concentracao": {"valor": round(risk_concentracao, 1), "rating": get_rating(risk_concentracao)},
                "liquidez": {"valor": round(risk_liquidez, 1), "rating": get_rating(risk_liquidez)},
                "clima": {"valor": round(risk_clima, 1), "rating": get_rating(risk_clima)}
            },
            "esg": esg_stats,
            "credito": credito_stats,
            "cricra": cricra_stats,
            "liquidez": liquidez_stats,
            "concentracao": {
                "hhi": round(hhi, 0),
                "classificacao": concentracao_setor,
                "por_setor": setores[:10]
            },
            "indexadores": por_indexador,
            "fundos": {
                "total": total_fundos + total_fundos_anbima,
                "esg": total_fundos_esg,
                "pct_esg": round(total_fundos_esg / max(1, total_fundos) * 100, 1)
            },
            "alertas": {
                "empresas_maior_risco": empresas_maior_risco,
                "top_emissores": top_emissores
            }
        })
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({"success": False, "error": str(e)}), 500

# ============================================================
# EARLY WARNING - SISTEMA DE ALERTAS
# ============================================================

@app.route('/api/early-warning')
def get_early_warning():
    """Sistema de alertas antecipados baseado em dados reais"""
    try:
        conn = get_connection()
        cursor = conn.cursor()
        alertas = []

        # ========== 1. ALERTA: Debêntures com taxa muito alta (>8%) ==========
        cursor.execute("""
            SELECT emissor, codigoativo, taxaindicativa, duration
            FROM titulos.debentures
            WHERE CAST(NULLIF(REGEXP_REPLACE(percentualtaxa, '[^0-9.]', '', 'g'), '') AS NUMERIC) > 8
            ORDER BY CAST(NULLIF(REGEXP_REPLACE(percentualtaxa, '[^0-9.]', '', 'g'), '') AS NUMERIC) DESC NULLS LAST
            LIMIT 5
        """)
        debentures_alto_risco = query_to_dict(cursor)
        if debentures_alto_risco:
            alertas.append({
                'tipo': 'credito',
                'severidade': 'alta',
                'icone': '💳',
                'titulo': f'{len(debentures_alto_risco)} Debêntures com Taxa > 8%',
                'descricao': 'Títulos com spread elevado podem indicar maior risco de crédito do emissor',
                'detalhes': [f"{d['codigoativo']} - {d['emissor'][:30]}... ({d['taxaindicativa']})" for d in debentures_alto_risco[:3]]
            })

        # ========== 2. ALERTA: Empresas TSB com score baixo (<70) ==========
        cursor.execute("""
            SELECT emissor, setortsb, classificacao, score
            FROM tsb.empresastsb
            WHERE score < 70
            ORDER BY score ASC
            LIMIT 5
        """)
        empresas_baixo_score = query_to_dict(cursor)
        if empresas_baixo_score:
            alertas.append({
                'tipo': 'esg',
                'severidade': 'media',
                'icone': '🌿',
                'titulo': f'{len(empresas_baixo_score)} Empresas com Score TSB < 70',
                'descricao': 'Empresas com score de sustentabilidade abaixo da média requerem monitoramento',
                'detalhes': [f"{e['emissor'][:25]}... - Score: {e['score']}" for e in empresas_baixo_score[:3]]
            })

        # ========== 3. ALERTA: Debêntures com duration muito longa (>1500 dias) ==========
        cursor.execute("""
            SELECT emissor, codigoativo, duration, taxaindicativa
            FROM titulos.debentures
            WHERE CAST(NULLIF(duration::text, '') AS NUMERIC) > 1500
            ORDER BY CAST(NULLIF(duration::text, '') AS NUMERIC) DESC NULLS LAST
            LIMIT 5
        """)
        debentures_longo_prazo = query_to_dict(cursor)
        if debentures_longo_prazo:
            alertas.append({
                'tipo': 'liquidez',
                'severidade': 'media',
                'icone': '⏱️',
                'titulo': f'{len(debentures_longo_prazo)} Títulos com Duration > 4 anos',
                'descricao': 'Títulos de longo prazo têm maior exposição a variações de taxa de juros',
                'detalhes': [f"{d['codigoativo']} - {int(d['duration'])} dias" for d in debentures_longo_prazo[:3]]
            })

        # ========== 4. ALERTA: Concentração setorial alta ==========
        cursor.execute("""
            SELECT setortsb, COUNT(*) as qtd
            FROM tsb.empresastsb
            GROUP BY setortsb
            ORDER BY qtd DESC
        """)
        setores = cursor.fetchall()
        total = sum(s[1] for s in setores)
        if setores and total > 0:
            maior_setor = setores[0]
            pct_concentracao = (maior_setor[1] / total) * 100
            if pct_concentracao > 30:
                alertas.append({
                    'tipo': 'concentracao',
                    'severidade': 'baixa',
                    'icone': '📊',
                    'titulo': f'Concentração em {maior_setor[0]}: {pct_concentracao:.0f}%',
                    'descricao': 'Alta concentração setorial aumenta risco de eventos adversos específicos',
                    'detalhes': [f"{s[0]}: {s[1]} empresas ({s[1]/total*100:.0f}%)" for s in setores[:3]]
                })

        # ========== 5. ALERTA: Empresas em transição ==========
        cursor.execute("""
            SELECT COUNT(*) as total,
                   SUM(CASE WHEN classificacao = 'TRANSICAO' THEN 1 ELSE 0 END) as transicao
            FROM tsb.empresastsb
        """)
        row = cursor.fetchone()
        if row[0] > 0:
            pct_transicao = (row[1] / row[0]) * 100
            if pct_transicao > 20:
                alertas.append({
                    'tipo': 'climatico',
                    'severidade': 'media',
                    'icone': '🔄',
                    'titulo': f'{row[1]} Empresas em Transição ({pct_transicao:.0f}%)',
                    'descricao': 'Empresas ainda não classificadas como Verde requerem acompanhamento de progresso ESG',
                    'detalhes': ['Monitorar relatórios de sustentabilidade', 'Verificar metas de descarbonização', 'Acompanhar certificações']
                })

        # ========== 6. ALERTA: Vencimentos próximos (simulado) ==========
        cursor.execute("""
            SELECT COUNT(*) FROM titulos.debentures WHERE CAST(NULLIF(duration::text, '') AS NUMERIC) < 365
        """)
        venc_proximo = cursor.fetchone()[0]
        if venc_proximo > 10:
            alertas.append({
                'tipo': 'vencimento',
                'severidade': 'baixa',
                'icone': '📅',
                'titulo': f'{venc_proximo} Títulos vencem em menos de 1 ano',
                'descricao': 'Planejar reinvestimento ou rolagem dos títulos com vencimento próximo',
                'detalhes': ['Avaliar condições de mercado', 'Verificar necessidade de liquidez', 'Analisar opções de reinvestimento']
            })

        conn.close()

        # Ordenar por severidade
        ordem_severidade = {'alta': 0, 'media': 1, 'baixa': 2}
        alertas.sort(key=lambda x: ordem_severidade.get(x['severidade'], 3))

        # Resumo
        resumo = {
            'total_alertas': len(alertas),
            'criticos': len([a for a in alertas if a['severidade'] == 'alta']),
            'medios': len([a for a in alertas if a['severidade'] == 'media']),
            'baixos': len([a for a in alertas if a['severidade'] == 'baixa'])
        }

        return jsonify({
            "success": True,
            "alertas": alertas,
            "resumo": resumo
        })
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({"success": False, "error": str(e)}), 500

# ============================================================
# DEBT ANALYSIS - ANÁLISE DE DÍVIDA
# ============================================================

@app.route('/api/debt-analysis')
def get_debt_analysis():
    """Análise completa da estrutura de dívida"""
    try:
        conn = get_connection()
        cursor = conn.cursor()

        # Total de debêntures e valor
        cursor.execute("""
            SELECT COUNT(*),
                   SUM(CAST(NULLIF(pu::text, '') AS NUMERIC)),
                   AVG(CAST(NULLIF(pu::text, '') AS NUMERIC))
            FROM titulos.debentures
        """)
        row = cursor.fetchone()
        total_debentures = row[0] or 0
        valor_total = row[1] or 0
        pu_medio = row[2] or 0

        # Por indexador (grupo)
        cursor.execute("""
            SELECT grupo,
                   COUNT(*) as qtd,
                   AVG(CAST(NULLIF(REGEXP_REPLACE(percentualtaxa, '[^0-9.]', '', 'g'), '') AS NUMERIC)) as taxa_media,
                   SUM(CAST(NULLIF(pu::text, '') AS NUMERIC)) as valor_total
            FROM titulos.debentures
            GROUP BY grupo
            ORDER BY qtd DESC
        """)
        por_indexador = query_to_dict(cursor)

        # Duration média por indexador
        cursor.execute("""
            SELECT grupo,
                   AVG(CAST(NULLIF(duration::text, '') AS NUMERIC)) as duration_media
            FROM titulos.debentures
            GROUP BY grupo
        """)
        duration_por_indexador = {r[0]: round(float(r[1]), 0) if r[1] else 0 for r in cursor.fetchall()}

        # Top 10 maiores emissores por valor
        cursor.execute("""
            SELECT emissor,
                   COUNT(*) as qtd_titulos,
                   SUM(CAST(NULLIF(pu::text, '') AS NUMERIC)) as valor_total,
                   AVG(CAST(NULLIF(REGEXP_REPLACE(percentualtaxa, '[^0-9.]', '', 'g'), '') AS NUMERIC)) as taxa_media
            FROM titulos.debentures
            GROUP BY emissor
            ORDER BY valor_total DESC NULLS LAST
            LIMIT 10
        """)
        top_emissores = query_to_dict(cursor)

        # Distribuição por faixa de taxa
        cursor.execute("""
            SELECT
                CASE
                    WHEN CAST(NULLIF(REGEXP_REPLACE(percentualtaxa, '[^0-9.]', '', 'g'), '') AS NUMERIC) < 5 THEN '< 5%'
                    WHEN CAST(NULLIF(REGEXP_REPLACE(percentualtaxa, '[^0-9.]', '', 'g'), '') AS NUMERIC) < 7 THEN '5-7%'
                    WHEN CAST(NULLIF(REGEXP_REPLACE(percentualtaxa, '[^0-9.]', '', 'g'), '') AS NUMERIC) < 9 THEN '7-9%'
                    ELSE '> 9%'
                END as faixa,
                COUNT(*) as qtd
            FROM titulos.debentures
            WHERE percentualtaxa IS NOT NULL AND percentualtaxa != ''
            GROUP BY faixa
            ORDER BY faixa
        """)
        por_faixa_taxa = query_to_dict(cursor)

        conn.close()

        return jsonify({
            "success": True,
            "resumo": {
                "total_debentures": total_debentures,
                "valor_total": round(valor_total, 2),
                "pu_medio": round(pu_medio, 2)
            },
            "por_indexador": por_indexador,
            "duration_por_indexador": duration_por_indexador,
            "top_emissores": top_emissores,
            "por_faixa_taxa": por_faixa_taxa
        })
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({"success": False, "error": str(e)}), 500

# ============================================================
# VENCIMENTOS - CALENDÁRIO DE VENCIMENTOS
# ============================================================

@app.route('/api/vencimentos')
def get_vencimentos():
    """Calendário de vencimentos dos títulos"""
    try:
        conn = get_connection()
        cursor = conn.cursor()

        # Debêntures por faixa de duration
        cursor.execute("""
            WITH faixas AS (
                SELECT
                    CASE
                        WHEN CAST(NULLIF(duration::text, '') AS NUMERIC) < 365 THEN '< 1 ano'
                        WHEN CAST(NULLIF(duration::text, '') AS NUMERIC) < 730 THEN '1-2 anos'
                        WHEN CAST(NULLIF(duration::text, '') AS NUMERIC) < 1095 THEN '2-3 anos'
                        WHEN CAST(NULLIF(duration::text, '') AS NUMERIC) < 1460 THEN '3-4 anos'
                        ELSE '> 4 anos'
                    END as faixa,
                    CASE
                        WHEN CAST(NULLIF(duration::text, '') AS NUMERIC) < 365 THEN 1
                        WHEN CAST(NULLIF(duration::text, '') AS NUMERIC) < 730 THEN 2
                        WHEN CAST(NULLIF(duration::text, '') AS NUMERIC) < 1095 THEN 3
                        WHEN CAST(NULLIF(duration::text, '') AS NUMERIC) < 1460 THEN 4
                        ELSE 5
                    END as ordem,
                    CAST(NULLIF(pu::text, '') AS NUMERIC) as pu_num
                FROM titulos.debentures
                WHERE duration IS NOT NULL
            )
            SELECT faixa, COUNT(*) as qtd, SUM(pu_num) as valor
            FROM faixas
            GROUP BY faixa, ordem
            ORDER BY ordem
        """)
        por_faixa = query_to_dict(cursor)

        # Títulos com vencimento mais próximo (menor duration)
        cursor.execute("""
            SELECT emissor, codigoativo, grupo, taxaindicativa, duration, pu
            FROM titulos.debentures
            WHERE duration IS NOT NULL AND duration != ''
            ORDER BY CAST(NULLIF(duration::text, '') AS NUMERIC) ASC NULLS LAST
            LIMIT 15
        """)
        proximos_vencimentos = query_to_dict(cursor)

        # Títulos públicos por tipo
        cursor.execute("""
            SELECT tipo, COUNT(*) as qtd
            FROM titulos.titulospublicos
            GROUP BY tipo
            ORDER BY tipo
        """)
        titulos_publicos = query_to_dict(cursor)

        # Duration média geral
        cursor.execute("""
            SELECT AVG(CAST(NULLIF(duration::text, '') AS NUMERIC)),
                   MIN(CAST(NULLIF(duration::text, '') AS NUMERIC)),
                   MAX(CAST(NULLIF(duration::text, '') AS NUMERIC))
            FROM titulos.debentures
            WHERE duration IS NOT NULL AND duration != ''
        """)
        row = cursor.fetchone()
        duration_stats = {
            'media': round(float(row[0]), 0) if row[0] else 0,
            'minima': round(float(row[1]), 0) if row[1] else 0,
            'maxima': round(float(row[2]), 0) if row[2] else 0
        }

        conn.close()

        return jsonify({
            "success": True,
            "por_faixa": por_faixa,
            "proximos_vencimentos": proximos_vencimentos,
            "titulos_publicos": titulos_publicos,
            "duration_stats": duration_stats
        })
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({"success": False, "error": str(e)}), 500

if __name__ == '__main__':
    print("=" * 60)
    print("SERVIDOR DASHBOARD + API (PostgreSQL)")
    print("=" * 60)
    print("Acesse: http://localhost:5000")
    print("=" * 60)
    app.run(debug=True, host='0.0.0.0', port=5000)
