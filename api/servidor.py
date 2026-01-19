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
    """Lista fundos consolidados ANBIMA + CVM (123.732 fundos)"""
    try:
        page = int(request.args.get('page', 1))
        per_page = min(int(request.args.get('per_page', 50)), 100)
        offset = (page - 1) * per_page
        search = request.args.get('search', '').strip()
        categoria = request.args.get('categoria', '').strip()
        tipo = request.args.get('tipo', '').strip()
        fonte = request.args.get('fonte', '').strip()  # 'ANBIMA', 'CVM' ou vazio para todos
        esg_only = request.args.get('esg_only', '').lower() == 'true'

        conn = get_connection()
        cursor = conn.cursor()

        # Usar view consolidada fundos.fundos_consolidados
        where_clauses = ["1=1"]
        params = []

        if search:
            where_clauses.append("(nome ILIKE %s OR cnpj ILIKE %s)")
            params.extend([f"%{search}%", f"%{search}%"])

        if categoria:
            where_clauses.append("(categoria_anbima ILIKE %s OR classe_cvm ILIKE %s)")
            params.extend([f"%{categoria}%", f"%{categoria}%"])

        if fonte:
            where_clauses.append("fonte_principal = %s")
            params.append(fonte)

        if esg_only:
            where_clauses.append("fundo_esg = TRUE")

        where_sql = " AND ".join(where_clauses)

        # Count total
        cursor.execute(f"SELECT COUNT(*) FROM fundos.fundos_consolidados WHERE {where_sql}", params)
        total = cursor.fetchone()[0]

        # Query principal
        cursor.execute(f"""
            SELECT
                cnpj,
                nome as razaosocial,
                nome as nomecomercial,
                tipo_anbima as tipofundo,
                COALESCE(categoria_anbima, classe_cvm) as categoria,
                CASE WHEN fundo_esg THEN 'ESG' ELSE 'Convencional' END as categoriaesg,
                COALESCE(foco_atuacao, 'N/A') as focoesg,
                gestor_anbima as gestora,
                fonte_principal as fonte,
                tem_dados_cvm,
                patrimonio_liquido,
                numero_cotistas
            FROM fundos.fundos_consolidados
            WHERE {where_sql}
            ORDER BY nome
            LIMIT %s OFFSET %s
        """, params + [per_page, offset])

        fundos = query_to_dict(cursor)

        # Estatisticas por fonte
        cursor.execute("""
            SELECT fonte_principal, COUNT(*) FROM fundos.fundos_consolidados GROUP BY fonte_principal
        """)
        stats_fonte = {r[0]: r[1] for r in cursor.fetchall()}

        conn.close()

        return jsonify({
            "success": True,
            "data": fundos,
            "pagination": {
                "page": page,
                "per_page": per_page,
                "total": total,
                "total_pages": (total + per_page - 1) // per_page
            },
            "stats": {
                "total": total,
                "por_fonte": stats_fonte
            }
        })
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/api/fundos/<cnpj>/detalhes')
def get_fundo_detalhes(cnpj):
    """Retorna detalhes completos de um fundo pelo CNPJ"""
    try:
        conn = get_connection()
        cursor = conn.cursor()

        # Limpar CNPJ
        cnpj_limpo = ''.join(filter(str.isdigit, cnpj))

        # Buscar dados do fundo consolidado
        cursor.execute("""
            SELECT
                fonte_principal, cnpj, nome, categoria_anbima, tipo_anbima,
                fundo_esg, gestor_anbima, administrador_anbima,
                patrimonio_liquido, numero_cotistas, valor_cota,
                foco_atuacao, nivel1_categoria, nivel2_categoria,
                classe_cvm, situacao_cvm, data_registro_cvm,
                captacao_dia, resgate_dia, tem_dados_cvm
            FROM fundos.fundos_consolidados
            WHERE cnpj = %s
        """, (cnpj_limpo,))

        row = cursor.fetchone()
        if not row:
            return jsonify({"success": False, "error": "Fundo não encontrado"}), 404

        fundo = {
            'fonte_principal': row[0],
            'cnpj': row[1],
            'nome': row[2],
            'categoria_anbima': row[3],
            'tipo_anbima': row[4],
            'fundo_esg': row[5],
            'gestor': row[6],
            'administrador': row[7],
            'patrimonio_liquido': float(row[8]) if row[8] else None,
            'numero_cotistas': row[9],
            'valor_cota': float(row[10]) if row[10] else None,
            'foco_atuacao': row[11],
            'nivel1_categoria': row[12],
            'nivel2_categoria': row[13],
            'classe_cvm': row[14],
            'situacao_cvm': row[15],
            'data_registro_cvm': row[16].isoformat() if row[16] else None,
            'captacao_dia': float(row[17]) if row[17] else None,
            'resgate_dia': float(row[18]) if row[18] else None,
            'tem_dados_cvm': row[19]
        }

        # Buscar dados adicionais do ANBIMA (características)
        cursor.execute("""
            SELECT
                codigo_anbima, estrutura, data_inicio, qtd_subclasses,
                composicao, aberto_estatutariamente, tributacao_alvo,
                primeiro_aporte, tipo_investidor, caracteristica_investidor,
                cota_abertura, aplicacao_minima, prazo_resgate_dias,
                adaptado_175, codigo_cvm_subclasse, nivel3_subcategoria
            FROM fundos.fundos_anbima
            WHERE cnpj_classe = %s
        """, (cnpj_limpo,))

        row_anbima = cursor.fetchone()
        if row_anbima:
            fundo['detalhes_anbima'] = {
                'codigo_anbima': row_anbima[0],
                'estrutura': row_anbima[1],
                'data_inicio': row_anbima[2].isoformat() if row_anbima[2] else None,
                'qtd_subclasses': row_anbima[3],
                'composicao': row_anbima[4],
                'aberto_estatutariamente': row_anbima[5],
                'tributacao_alvo': row_anbima[6],
                'primeiro_aporte': row_anbima[7].isoformat() if row_anbima[7] else None,
                'tipo_investidor': row_anbima[8],
                'caracteristica_investidor': row_anbima[9],
                'cota_abertura': row_anbima[10],
                'aplicacao_minima': float(row_anbima[11]) if row_anbima[11] else None,
                'prazo_resgate_dias': row_anbima[12],
                'adaptado_175': row_anbima[13],
                'codigo_cvm': row_anbima[14],
                'subcategoria': row_anbima[15]
            }

        # Buscar dados do cadastro CVM
        cursor.execute("""
            SELECT
                data_constituicao, rentabilidade_fundo, condominio,
                fundo_cotas, fundo_exclusivo, tributacao_longo_prazo,
                investidor_qualificado, taxa_performance, info_taxa_performance,
                taxa_administracao, info_taxa_administracao, diretor, auditor
            FROM cvm.cadastro_fundos
            WHERE cnpj = %s
        """, (cnpj_limpo,))

        row_cvm = cursor.fetchone()
        if row_cvm:
            fundo['detalhes_cvm'] = {
                'data_constituicao': row_cvm[0].isoformat() if row_cvm[0] else None,
                'rentabilidade_fundo': row_cvm[1],
                'condominio': row_cvm[2],
                'fundo_cotas': row_cvm[3],
                'fundo_exclusivo': row_cvm[4],
                'tributacao_longo_prazo': row_cvm[5],
                'investidor_qualificado': row_cvm[6],
                'taxa_performance': float(row_cvm[7]) if row_cvm[7] else None,
                'info_taxa_performance': row_cvm[8],
                'taxa_administracao': float(row_cvm[9]) if row_cvm[9] else None,
                'info_taxa_administracao': row_cvm[10],
                'diretor': row_cvm[11],
                'auditor': row_cvm[12]
            }

        # Buscar últimos informes diários CVM
        cursor.execute("""
            SELECT data_competencia, valor_total, valor_cota, patrimonio_liquido,
                   captacao_dia, resgate_dia, numero_cotistas
            FROM cvm.informes_diarios
            WHERE cnpj = %s
            ORDER BY data_competencia DESC
            LIMIT 5
        """, (cnpj_limpo,))

        informes = []
        for r in cursor.fetchall():
            informes.append({
                'data': r[0].isoformat() if r[0] else None,
                'valor_total': float(r[1]) if r[1] else None,
                'valor_cota': float(r[2]) if r[2] else None,
                'patrimonio_liquido': float(r[3]) if r[3] else None,
                'captacao': float(r[4]) if r[4] else None,
                'resgate': float(r[5]) if r[5] else None,
                'cotistas': r[6]
            })
        fundo['informes_recentes'] = informes

        # Buscar títulos relacionados (CRI/CRA do mesmo emissor/gestor)
        gestor = fundo.get('gestor', '')
        if gestor:
            cursor.execute("""
                SELECT tipo, codigo, emissor, devedor, tipo_remuneracao,
                       data_vencimento, taxa_indicativa, pu_indicativo, duration_dias
                FROM titulos.cricra_anbima
                WHERE emissor ILIKE %s OR devedor ILIKE %s
                LIMIT 10
            """, (f'%{gestor[:20]}%', f'%{gestor[:20]}%'))

            titulos_cricra = []
            for r in cursor.fetchall():
                titulos_cricra.append({
                    'tipo': r[0],
                    'codigo': r[1],
                    'emissor': r[2],
                    'devedor': r[3],
                    'tipo_remuneracao': r[4],
                    'data_vencimento': r[5].isoformat() if r[5] else None,
                    'taxa_indicativa': float(r[6]) if r[6] else None,
                    'pu_indicativo': float(r[7]) if r[7] else None,
                    'duration': r[8]
                })
            fundo['titulos_relacionados'] = titulos_cricra

        cursor.close()
        conn.close()

        return jsonify({"success": True, "data": fundo})
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

# ========== ENDPOINTS ANBIMA DATA (NOVOS DADOS) ==========

@app.route('/api/anbima/fundos')
def get_anbima_fundos():
    """Lista fundos da tabela fundos.fundos_anbima com filtros"""
    try:
        conn = get_connection()
        cursor = conn.cursor()

        # Parametros
        page = int(request.args.get('page', 1))
        per_page = int(request.args.get('per_page', 50))
        search = request.args.get('search', '')
        categoria = request.args.get('categoria', '')
        tipo = request.args.get('tipo', '')
        esg = request.args.get('esg', '')

        offset = (page - 1) * per_page

        # Construir query
        where_clauses = ["1=1"]
        params = []

        if search:
            where_clauses.append("(nome ILIKE %s OR cnpj_classe ILIKE %s OR gestor ILIKE %s)")
            params.extend([f'%{search}%', f'%{search}%', f'%{search}%'])

        if categoria:
            where_clauses.append("categoria_anbima ILIKE %s")
            params.append(f'%{categoria}%')

        if tipo:
            where_clauses.append("tipo_anbima ILIKE %s")
            params.append(f'%{tipo}%')

        if esg == 'true':
            where_clauses.append("fundo_esg = true")

        where_sql = " AND ".join(where_clauses)

        # Contar total
        cursor.execute(f"SELECT COUNT(*) FROM fundos.fundos_anbima WHERE {where_sql}", params)
        total = cursor.fetchone()[0]

        # Buscar dados
        cursor.execute(f"""
            SELECT codigo_anbima, estrutura, nome, cnpj_classe, cnpj_fundo,
                   status, data_inicio, categoria_anbima, tipo_anbima,
                   fundo_esg, administrador, gestor, tipo_investidor,
                   foco_atuacao, nivel1_categoria, nivel2_categoria, nivel3_subcategoria,
                   pl_atual, qtd_cotistas, valor_cota
            FROM fundos.fundos_anbima
            WHERE {where_sql}
            ORDER BY pl_atual DESC NULLS LAST
            LIMIT %s OFFSET %s
        """, params + [per_page, offset])

        fundos = query_to_dict(cursor)

        # Estatisticas basicas (apenas na primeira pagina)
        stats = None
        if page == 1:
            stats = {}
            # Total ESG
            cursor.execute("SELECT COUNT(*) FROM fundos.fundos_anbima WHERE fundo_esg = true")
            stats['total_esg'] = cursor.fetchone()[0]
            # Fundos ativos
            cursor.execute("SELECT COUNT(*) FROM fundos.fundos_anbima WHERE status ILIKE '%ativo%'")
            stats['ativos'] = cursor.fetchone()[0]
            # Categorias distintas
            cursor.execute("SELECT COUNT(DISTINCT categoria_anbima) FROM fundos.fundos_anbima WHERE categoria_anbima IS NOT NULL")
            stats['categorias'] = cursor.fetchone()[0]
            # Lista de categorias para o filtro
            cursor.execute("SELECT DISTINCT categoria_anbima FROM fundos.fundos_anbima WHERE categoria_anbima IS NOT NULL ORDER BY categoria_anbima")
            stats['lista_categorias'] = [r[0] for r in cursor.fetchall()]

        cursor.close()
        conn.close()

        response = {
            "success": True,
            "data": fundos,
            "total": total,
            "page": page,
            "per_page": per_page,
            "pages": (total + per_page - 1) // per_page
        }
        if stats:
            response['stats'] = stats

        return jsonify(response)
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/api/anbima/fundos/stats')
def get_anbima_fundos_stats():
    """Estatisticas dos fundos ANBIMA"""
    try:
        conn = get_connection()
        cursor = conn.cursor()

        stats = {}

        # Total de fundos
        cursor.execute("SELECT COUNT(*) FROM fundos.fundos_anbima")
        stats['total'] = cursor.fetchone()[0]

        # Fundos ESG
        cursor.execute("SELECT COUNT(*) FROM fundos.fundos_anbima WHERE fundo_esg = true")
        stats['total_esg'] = cursor.fetchone()[0]

        # Por categoria
        cursor.execute("""
            SELECT categoria_anbima, COUNT(*) as qtd
            FROM fundos.fundos_anbima
            WHERE categoria_anbima IS NOT NULL
            GROUP BY categoria_anbima
            ORDER BY qtd DESC
            LIMIT 10
        """)
        stats['por_categoria'] = [{'categoria': r[0], 'qtd': r[1]} for r in cursor.fetchall()]

        # Por tipo
        cursor.execute("""
            SELECT tipo_anbima, COUNT(*) as qtd
            FROM fundos.fundos_anbima
            WHERE tipo_anbima IS NOT NULL
            GROUP BY tipo_anbima
            ORDER BY qtd DESC
            LIMIT 10
        """)
        stats['por_tipo'] = [{'tipo': r[0], 'qtd': r[1]} for r in cursor.fetchall()]

        # Patrimonio total
        cursor.execute("SELECT SUM(pl_atual) FROM fundos.fundos_anbima WHERE pl_atual IS NOT NULL")
        pl_total = cursor.fetchone()[0]
        stats['patrimonio_total'] = float(pl_total) if pl_total else 0

        # Total cotistas
        cursor.execute("SELECT SUM(qtd_cotistas) FROM fundos.fundos_anbima WHERE qtd_cotistas IS NOT NULL")
        cotistas = cursor.fetchone()[0]
        stats['total_cotistas'] = int(cotistas) if cotistas else 0

        cursor.close()
        conn.close()

        return jsonify({"success": True, "data": stats})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/api/anbima/fundos/periodicos')
def get_anbima_fundos_periodicos():
    """Lista fundos com dados periodicos (PL, cotistas, valor da cota)"""
    try:
        conn = get_connection()
        cursor = conn.cursor()

        # Parametros
        page = int(request.args.get('page', 1))
        per_page = int(request.args.get('per_page', 50))
        search = request.args.get('search', '')
        ordem = request.args.get('ordem', 'pl')  # pl, cotistas, cota, nome
        foco = request.args.get('foco', '')
        categoria = request.args.get('categoria', '')

        offset = (page - 1) * per_page

        where_sql = "1=1"
        params = []

        if search:
            where_sql += " AND (nome ILIKE %s OR cnpj_classe LIKE %s)"
            params.extend([f'%{search}%', f'%{search}%'])

        if foco:
            where_sql += " AND foco_atuacao = %s"
            params.append(foco)

        if categoria:
            where_sql += " AND nivel1_categoria = %s"
            params.append(categoria)

        # Definir ordenacao
        ordem_sql = "pl_atual DESC NULLS LAST"
        if ordem == 'cotistas':
            ordem_sql = "qtd_cotistas DESC NULLS LAST"
        elif ordem == 'cota':
            ordem_sql = "valor_cota DESC NULLS LAST"
        elif ordem == 'nome':
            ordem_sql = "nome ASC"

        # Buscar dados
        cursor.execute(f"""
            SELECT codigo_anbima, nome, cnpj_classe, status, data_referencia,
                   pl_atual, qtd_cotistas, valor_cota, foco_atuacao,
                   nivel1_categoria, nivel2_categoria, nivel3_subcategoria
            FROM fundos.fundos_anbima_periodicos
            WHERE {where_sql}
            ORDER BY {ordem_sql}
            LIMIT %s OFFSET %s
        """, params + [per_page, offset])

        data = query_to_dict(cursor)

        # Contagem total
        cursor.execute(f"SELECT COUNT(*) FROM fundos.fundos_anbima_periodicos WHERE {where_sql}", params)
        total = cursor.fetchone()[0]

        # Estatisticas gerais
        cursor.execute("""
            SELECT
                COUNT(*) as total_fundos,
                COUNT(CASE WHEN pl_atual > 0 THEN 1 END) as fundos_com_pl,
                SUM(pl_atual) as pl_total,
                SUM(qtd_cotistas) as cotistas_total,
                AVG(valor_cota) as cota_media,
                MAX(data_referencia) as data_referencia
            FROM fundos.fundos_anbima_periodicos
        """)
        stats_row = cursor.fetchone()
        stats = {
            'total_fundos': stats_row[0],
            'fundos_com_pl': stats_row[1],
            'pl_total': float(stats_row[2]) if stats_row[2] else 0,
            'cotistas_total': int(stats_row[3]) if stats_row[3] else 0,
            'cota_media': float(stats_row[4]) if stats_row[4] else 0,
            'data_referencia': stats_row[5].strftime('%d/%m/%Y') if stats_row[5] else '-'
        }

        # Top 10 por PL
        cursor.execute("""
            SELECT nome, pl_atual, qtd_cotistas, valor_cota
            FROM fundos.fundos_anbima_periodicos
            WHERE pl_atual IS NOT NULL AND pl_atual > 0
            ORDER BY pl_atual DESC
            LIMIT 10
        """)
        stats['top_pl'] = [
            {'nome': r[0], 'pl': float(r[1]), 'cotistas': int(r[2]) if r[2] else 0, 'cota': float(r[3]) if r[3] else 0}
            for r in cursor.fetchall()
        ]

        # Por foco de atuacao
        cursor.execute("""
            SELECT foco_atuacao, COUNT(*) as qtd, SUM(pl_atual) as pl
            FROM fundos.fundos_anbima_periodicos
            WHERE foco_atuacao IS NOT NULL
            GROUP BY foco_atuacao
            ORDER BY pl DESC NULLS LAST
        """)
        stats['por_foco'] = [
            {'foco': r[0], 'qtd': r[1], 'pl': float(r[2]) if r[2] else 0}
            for r in cursor.fetchall()
        ]

        # Por categoria nivel 1
        cursor.execute("""
            SELECT nivel1_categoria, COUNT(*) as qtd, SUM(pl_atual) as pl
            FROM fundos.fundos_anbima_periodicos
            WHERE nivel1_categoria IS NOT NULL
            GROUP BY nivel1_categoria
            ORDER BY pl DESC NULLS LAST
            LIMIT 10
        """)
        stats['por_categoria'] = [
            {'categoria': r[0], 'qtd': r[1], 'pl': float(r[2]) if r[2] else 0}
            for r in cursor.fetchall()
        ]

        # Lista de focos para filtro
        cursor.execute("SELECT DISTINCT foco_atuacao FROM fundos.fundos_anbima_periodicos WHERE foco_atuacao IS NOT NULL ORDER BY 1")
        stats['lista_focos'] = [r[0] for r in cursor.fetchall()]

        # Lista de categorias para filtro
        cursor.execute("SELECT DISTINCT nivel1_categoria FROM fundos.fundos_anbima_periodicos WHERE nivel1_categoria IS NOT NULL ORDER BY 1")
        stats['lista_categorias'] = [r[0] for r in cursor.fetchall()]

        cursor.close()
        conn.close()

        return jsonify({
            "success": True,
            "data": data,
            "total": total,
            "page": page,
            "per_page": per_page,
            "pages": (total + per_page - 1) // per_page,
            "stats": stats
        })
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/api/anbima/cricra')
def get_anbima_cricra():
    """Lista CRI/CRA da tabela titulos.cricra_anbima"""
    try:
        conn = get_connection()
        cursor = conn.cursor()

        # Parametros
        tipo = request.args.get('tipo', '')  # CRI ou CRA

        where_sql = "1=1"
        params = []

        if tipo:
            where_sql += " AND tipo = %s"
            params.append(tipo)

        cursor.execute(f"""
            SELECT data_referencia, tipo, codigo, emissor, devedor,
                   tipo_remuneracao, taxa_correcao, serie, emissao,
                   data_vencimento, taxa_compra, taxa_venda, taxa_indicativa,
                   pu_indicativo, desvio_padrao, duration_dias
            FROM titulos.cricra_anbima
            WHERE {where_sql}
            ORDER BY duration_dias DESC NULLS LAST
        """, params)

        data = query_to_dict(cursor)

        # Estatisticas
        cursor.execute("SELECT tipo, COUNT(*) FROM titulos.cricra_anbima GROUP BY tipo")
        stats_rows = cursor.fetchall()
        stats = {'total': sum(r[1] for r in stats_rows)}
        for row in stats_rows:
            stats[row[0].lower()] = row[1]

        # Data de referencia
        cursor.execute("SELECT MAX(data_referencia) FROM titulos.cricra_anbima")
        data_ref = cursor.fetchone()[0]
        stats['data_referencia'] = data_ref.strftime('%d/%m/%Y') if data_ref else '-'

        cursor.close()
        conn.close()

        return jsonify({
            "success": True,
            "data": data,
            "total": len(data),
            "stats": stats
        })
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/api/anbima/titulos-publicos')
def get_anbima_titulos_publicos():
    """Lista titulos publicos da tabela titulos.titulos_publicos_anbima"""
    try:
        conn = get_connection()
        cursor = conn.cursor()

        tipo = request.args.get('tipo', '')

        where_sql = "1=1"
        params = []

        if tipo:
            where_sql += " AND tipo_titulo = %s"
            params.append(tipo)

        cursor.execute(f"""
            SELECT data_referencia, tipo_titulo, codigo_selic, data_vencimento,
                   codigo_isin, data_emissao, taxa_compra, taxa_venda,
                   taxa_indicativa, pu_indicativo, desvio_padrao
            FROM titulos.titulos_publicos_anbima
            WHERE {where_sql}
            ORDER BY data_vencimento
        """, params)

        data = query_to_dict(cursor)

        # Estatisticas
        cursor.execute("SELECT tipo_titulo, COUNT(*) FROM titulos.titulos_publicos_anbima GROUP BY tipo_titulo")
        stats_rows = cursor.fetchall()
        stats = {'total': sum(r[1] for r in stats_rows)}
        for row in stats_rows:
            stats[row[0]] = row[1]

        # Data de referencia
        cursor.execute("SELECT MAX(data_referencia) FROM titulos.titulos_publicos_anbima")
        data_ref = cursor.fetchone()[0]
        stats['data_referencia'] = data_ref.strftime('%d/%m/%Y') if data_ref else '-'

        cursor.close()
        conn.close()

        return jsonify({
            "success": True,
            "data": data,
            "total": len(data),
            "stats": stats
        })
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/api/anbima/resumo')
def get_anbima_resumo():
    """Resumo geral dos dados ANBIMA"""
    try:
        conn = get_connection()
        cursor = conn.cursor()

        resumo = {}

        # Fundos
        cursor.execute("SELECT COUNT(*) FROM fundos.fundos_anbima")
        resumo['fundos'] = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM fundos.fundos_anbima WHERE fundo_esg = true")
        resumo['fundos_esg'] = cursor.fetchone()[0]

        cursor.execute("SELECT SUM(pl_atual) FROM fundos.fundos_anbima")
        pl = cursor.fetchone()[0]
        resumo['patrimonio_total'] = float(pl) if pl else 0

        # CRI/CRA
        cursor.execute("SELECT COUNT(*) FROM titulos.cricra_anbima WHERE tipo = 'CRI'")
        resumo['cri'] = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM titulos.cricra_anbima WHERE tipo = 'CRA'")
        resumo['cra'] = cursor.fetchone()[0]

        # Titulos Publicos
        cursor.execute("SELECT COUNT(*) FROM titulos.titulos_publicos_anbima")
        resumo['titulos_publicos'] = cursor.fetchone()[0]

        cursor.close()
        conn.close()

        return jsonify({"success": True, "data": resumo})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

# ============================================
# ENDPOINTS CVM - Dados da Comissao de Valores Mobiliarios
# ============================================

@app.route('/api/cvm/cadastro')
def get_cvm_cadastro():
    """Lista fundos do cadastro CVM com filtros"""
    try:
        conn = get_connection()
        cursor = conn.cursor()

        page = int(request.args.get('page', 1))
        per_page = int(request.args.get('per_page', 50))
        search = request.args.get('search', '')
        situacao = request.args.get('situacao', '')
        classe = request.args.get('classe', '')

        offset = (page - 1) * per_page
        where_sql = "1=1"
        params = []

        if search:
            where_sql += " AND (nome ILIKE %s OR cnpj ILIKE %s)"
            params.extend([f'%{search}%', f'%{search}%'])

        if situacao:
            where_sql += " AND situacao = %s"
            params.append(situacao)

        if classe:
            where_sql += " AND classe = %s"
            params.append(classe)

        # Contar total
        cursor.execute(f"SELECT COUNT(*) FROM cvm.cadastro_fundos WHERE {where_sql}", params)
        total = cursor.fetchone()[0]

        # Buscar dados
        cursor.execute(f"""
            SELECT cnpj, nome, situacao, classe, data_registro,
                   administrador, gestor, patrimonio_liquido,
                   fundo_cotas, fundo_exclusivo, investidor_qualificado
            FROM cvm.cadastro_fundos
            WHERE {where_sql}
            ORDER BY nome
            LIMIT %s OFFSET %s
        """, params + [per_page, offset])

        data = query_to_dict(cursor)

        # Filtros disponiveis
        cursor.execute("SELECT DISTINCT situacao FROM cvm.cadastro_fundos WHERE situacao IS NOT NULL ORDER BY situacao")
        situacoes = [r[0] for r in cursor.fetchall()]

        cursor.execute("SELECT DISTINCT classe FROM cvm.cadastro_fundos WHERE classe IS NOT NULL ORDER BY classe")
        classes = [r[0] for r in cursor.fetchall()]

        cursor.close()
        conn.close()

        return jsonify({
            "success": True,
            "data": data,
            "total": total,
            "page": page,
            "per_page": per_page,
            "pages": (total + per_page - 1) // per_page,
            "filtros": {
                "situacoes": situacoes,
                "classes": classes
            }
        })
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/api/cvm/informes')
def get_cvm_informes():
    """Lista informes diarios da CVM"""
    try:
        conn = get_connection()
        cursor = conn.cursor()

        page = int(request.args.get('page', 1))
        per_page = int(request.args.get('per_page', 50))
        search = request.args.get('search', '')
        ano_mes = request.args.get('ano_mes', '')

        offset = (page - 1) * per_page
        where_sql = "1=1"
        params = []

        if search:
            where_sql += " AND cnpj ILIKE %s"
            params.append(f'%{search}%')

        if ano_mes:
            where_sql += " AND ano_mes = %s"
            params.append(ano_mes)

        # Contar total
        cursor.execute(f"SELECT COUNT(*) FROM cvm.informes_diarios WHERE {where_sql}", params)
        total = cursor.fetchone()[0]

        # Buscar dados
        cursor.execute(f"""
            SELECT cnpj, data_competencia, valor_total, valor_cota,
                   patrimonio_liquido, captacao_dia, resgate_dia, numero_cotistas, ano_mes
            FROM cvm.informes_diarios
            WHERE {where_sql}
            ORDER BY patrimonio_liquido DESC NULLS LAST
            LIMIT %s OFFSET %s
        """, params + [per_page, offset])

        data = query_to_dict(cursor)

        # Estatisticas
        cursor.execute("""
            SELECT
                COUNT(*) as total_registros,
                COUNT(DISTINCT cnpj) as fundos_unicos,
                SUM(patrimonio_liquido) as pl_total,
                SUM(numero_cotistas) as cotistas_total,
                SUM(captacao_dia) as captacao_total,
                SUM(resgate_dia) as resgate_total
            FROM cvm.informes_diarios
        """)
        row = cursor.fetchone()
        stats = {
            'total_registros': row[0],
            'fundos_unicos': row[1],
            'pl_total': float(row[2]) if row[2] else 0,
            'cotistas_total': int(row[3]) if row[3] else 0,
            'captacao_total': float(row[4]) if row[4] else 0,
            'resgate_total': float(row[5]) if row[5] else 0
        }

        # Periodos disponiveis
        cursor.execute("SELECT DISTINCT ano_mes FROM cvm.informes_diarios ORDER BY ano_mes DESC")
        periodos = [r[0] for r in cursor.fetchall()]

        cursor.close()
        conn.close()

        return jsonify({
            "success": True,
            "data": data,
            "total": total,
            "page": page,
            "per_page": per_page,
            "pages": (total + per_page - 1) // per_page,
            "stats": stats,
            "periodos": periodos
        })
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/api/cvm/stats')
def get_cvm_stats():
    """Estatisticas gerais dos dados CVM"""
    try:
        conn = get_connection()
        cursor = conn.cursor()

        stats = {}

        # Cadastro
        cursor.execute("SELECT COUNT(*) FROM cvm.cadastro_fundos")
        stats['total_cadastro'] = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM cvm.cadastro_fundos WHERE situacao = 'EM FUNCIONAMENTO NORMAL'")
        stats['fundos_ativos'] = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(DISTINCT classe) FROM cvm.cadastro_fundos")
        stats['classes_distintas'] = cursor.fetchone()[0]

        # Informes
        cursor.execute("SELECT COUNT(*) FROM cvm.informes_diarios")
        stats['total_informes'] = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(DISTINCT cnpj) FROM cvm.informes_diarios")
        stats['fundos_com_informes'] = cursor.fetchone()[0]

        cursor.execute("SELECT SUM(patrimonio_liquido) FROM cvm.informes_diarios")
        pl = cursor.fetchone()[0]
        stats['pl_total_informes'] = float(pl) if pl else 0

        # Top classes
        cursor.execute("""
            SELECT classe, COUNT(*) as total
            FROM cvm.cadastro_fundos
            WHERE classe IS NOT NULL
            GROUP BY classe
            ORDER BY total DESC
            LIMIT 10
        """)
        stats['top_classes'] = [{'classe': r[0], 'total': r[1]} for r in cursor.fetchall()]

        # Por situacao
        cursor.execute("""
            SELECT situacao, COUNT(*) as total
            FROM cvm.cadastro_fundos
            WHERE situacao IS NOT NULL
            GROUP BY situacao
            ORDER BY total DESC
        """)
        stats['por_situacao'] = [{'situacao': r[0], 'total': r[1]} for r in cursor.fetchall()]

        cursor.close()
        conn.close()

        return jsonify({"success": True, "data": stats})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/api/cvm/carteira')
def get_cvm_carteira():
    """Lista carteira de fundos da CVM (CDA)"""
    try:
        conn = get_connection()
        cursor = conn.cursor()

        page = int(request.args.get('page', 1))
        per_page = int(request.args.get('per_page', 50))
        cnpj_fundo = request.args.get('cnpj_fundo', '')
        tipo_ativo = request.args.get('tipo_ativo', '')
        emissor = request.args.get('emissor', '')

        offset = (page - 1) * per_page
        where_sql = "1=1"
        params = []

        if cnpj_fundo:
            where_sql += " AND cnpj_fundo = %s"
            params.append(cnpj_fundo.replace('.', '').replace('/', '').replace('-', ''))

        if tipo_ativo:
            where_sql += " AND tipo_ativo ILIKE %s"
            params.append(f'%{tipo_ativo}%')

        if emissor:
            where_sql += " AND (emissor ILIKE %s OR cnpj_emissor ILIKE %s)"
            params.extend([f'%{emissor}%', f'%{emissor}%'])

        # Total count
        cursor.execute(f"""
            SELECT COUNT(*) FROM cvm.carteira_fundos WHERE {where_sql}
        """, params)
        total = cursor.fetchone()[0]

        # Data
        cursor.execute(f"""
            SELECT cnpj_fundo, nome_fundo, tipo_fundo, data_competencia,
                   tipo_aplicacao, tipo_ativo, emissor, cnpj_emissor,
                   quantidade_posicao, valor_mercado, valor_custo,
                   data_vencimento, indexador, titulo_cetip, bloco
            FROM cvm.carteira_fundos
            WHERE {where_sql}
            ORDER BY valor_mercado DESC NULLS LAST
            LIMIT %s OFFSET %s
        """, params + [per_page, offset])

        columns = ['cnpj_fundo', 'nome_fundo', 'tipo_fundo', 'data_competencia',
                   'tipo_aplicacao', 'tipo_ativo', 'emissor', 'cnpj_emissor',
                   'quantidade_posicao', 'valor_mercado', 'valor_custo',
                   'data_vencimento', 'indexador', 'titulo_cetip', 'bloco']

        data = []
        for row in cursor.fetchall():
            item = {}
            for i, col in enumerate(columns):
                val = row[i]
                if val is not None:
                    if hasattr(val, 'isoformat'):
                        val = val.isoformat()
                    elif isinstance(val, Decimal):
                        val = float(val)
                item[col] = val
            data.append(item)

        cursor.close()
        conn.close()

        return jsonify({
            "success": True,
            "data": data,
            "pagination": {
                "page": page,
                "per_page": per_page,
                "total": total,
                "total_pages": (total + per_page - 1) // per_page
            }
        })
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/api/fundos/<cnpj>/carteira')
def get_fundo_carteira(cnpj):
    """Retorna carteira de um fundo especifico"""
    try:
        conn = get_connection()
        cursor = conn.cursor()

        # Limpar CNPJ
        cnpj_limpo = cnpj.replace('.', '').replace('/', '').replace('-', '')

        # Buscar posicoes do fundo
        cursor.execute("""
            SELECT tipo_ativo, tipo_aplicacao, emissor, cnpj_emissor,
                   quantidade_posicao, valor_mercado, valor_custo,
                   data_vencimento, indexador, titulo_cetip, bloco
            FROM cvm.carteira_fundos
            WHERE cnpj_fundo = %s
            ORDER BY valor_mercado DESC NULLS LAST
            LIMIT 100
        """, (cnpj_limpo,))

        columns = ['tipo_ativo', 'tipo_aplicacao', 'emissor', 'cnpj_emissor',
                   'quantidade_posicao', 'valor_mercado', 'valor_custo',
                   'data_vencimento', 'indexador', 'titulo_cetip', 'bloco']

        posicoes = []
        total_mercado = 0
        for row in cursor.fetchall():
            item = {}
            for i, col in enumerate(columns):
                val = row[i]
                if val is not None:
                    if hasattr(val, 'isoformat'):
                        val = val.isoformat()
                    elif isinstance(val, Decimal):
                        val = float(val)
                item[col] = val
            posicoes.append(item)
            if item.get('valor_mercado'):
                total_mercado += item['valor_mercado']

        # Resumo por tipo de ativo
        cursor.execute("""
            SELECT tipo_ativo, COUNT(*), SUM(valor_mercado)
            FROM cvm.carteira_fundos
            WHERE cnpj_fundo = %s
            GROUP BY tipo_ativo
            ORDER BY SUM(valor_mercado) DESC NULLS LAST
        """, (cnpj_limpo,))

        resumo_ativos = []
        for row in cursor.fetchall():
            resumo_ativos.append({
                'tipo_ativo': row[0],
                'quantidade': row[1],
                'valor_total': float(row[2]) if row[2] else 0
            })

        cursor.close()
        conn.close()

        return jsonify({
            "success": True,
            "cnpj": cnpj,
            "total_posicoes": len(posicoes),
            "valor_total_mercado": total_mercado,
            "resumo_por_tipo": resumo_ativos,
            "posicoes": posicoes
        })
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/api/cvm/carteira/stats')
def get_cvm_carteira_stats():
    """Estatisticas da carteira de fundos CVM"""
    try:
        conn = get_connection()
        cursor = conn.cursor()

        stats = {}

        # Total de posicoes
        cursor.execute("SELECT COUNT(*) FROM cvm.carteira_fundos")
        stats['total_posicoes'] = cursor.fetchone()[0]

        # Total de fundos com carteira
        cursor.execute("SELECT COUNT(DISTINCT cnpj_fundo) FROM cvm.carteira_fundos")
        stats['total_fundos'] = cursor.fetchone()[0]

        # Valor total de mercado
        cursor.execute("SELECT SUM(valor_mercado) FROM cvm.carteira_fundos")
        result = cursor.fetchone()[0]
        stats['valor_total_mercado'] = float(result) if result else 0

        # Por tipo de ativo (top 15)
        cursor.execute("""
            SELECT tipo_ativo, COUNT(*) as qtd, SUM(valor_mercado) as valor
            FROM cvm.carteira_fundos
            WHERE tipo_ativo IS NOT NULL
            GROUP BY tipo_ativo
            ORDER BY SUM(valor_mercado) DESC NULLS LAST
            LIMIT 15
        """)
        stats['por_tipo_ativo'] = [
            {'tipo': r[0], 'quantidade': r[1], 'valor': float(r[2]) if r[2] else 0}
            for r in cursor.fetchall()
        ]

        # Por bloco
        cursor.execute("""
            SELECT bloco, COUNT(*) as qtd, SUM(valor_mercado) as valor
            FROM cvm.carteira_fundos
            GROUP BY bloco
            ORDER BY SUM(valor_mercado) DESC NULLS LAST
        """)
        stats['por_bloco'] = [
            {'bloco': r[0], 'quantidade': r[1], 'valor': float(r[2]) if r[2] else 0}
            for r in cursor.fetchall()
        ]

        cursor.close()
        conn.close()

        return jsonify({"success": True, "data": stats})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/api/fundos/<cnpj>/titulos-anbima')
def get_fundo_titulos_anbima(cnpj):
    """Retorna titulos ANBIMA encontrados na carteira do fundo"""
    try:
        conn = get_connection()
        cursor = conn.cursor()

        # Limpar CNPJ
        cnpj_limpo = cnpj.replace('.', '').replace('/', '').replace('-', '')

        # Buscar titulos ANBIMA na carteira do fundo
        cursor.execute("""
            SELECT tipo_ativo, emissor_carteira, cnpj_emissor, valor_mercado,
                   tipo_titulo_anbima, codigo_anbima, emissor_anbima, originador,
                   taxaindicativa, pu_indicativo, duration, tiporemuneracao
            FROM fundos.carteira_titulos_anbima
            WHERE cnpj_fundo = %s
            ORDER BY valor_mercado DESC NULLS LAST
        """, (cnpj_limpo,))

        columns = ['tipo_ativo', 'emissor_carteira', 'cnpj_emissor', 'valor_mercado',
                   'tipo_titulo_anbima', 'codigo_anbima', 'emissor_anbima', 'originador',
                   'taxa_indicativa', 'pu_indicativo', 'duration', 'tipo_remuneracao']

        titulos = []
        valor_total = 0
        for row in cursor.fetchall():
            item = {}
            for i, col in enumerate(columns):
                val = row[i]
                if val is not None:
                    if hasattr(val, 'isoformat'):
                        val = val.isoformat()
                    elif isinstance(val, Decimal):
                        val = float(val)
                item[col] = val
            titulos.append(item)
            if item.get('valor_mercado'):
                valor_total += item['valor_mercado']

        # Resumo por tipo
        cursor.execute("""
            SELECT tipo_titulo_anbima, COUNT(*), SUM(valor_mercado)
            FROM fundos.carteira_titulos_anbima
            WHERE cnpj_fundo = %s
            GROUP BY tipo_titulo_anbima
        """, (cnpj_limpo,))

        resumo = [{'tipo': r[0], 'quantidade': r[1], 'valor': float(r[2]) if r[2] else 0}
                  for r in cursor.fetchall()]

        cursor.close()
        conn.close()

        return jsonify({
            "success": True,
            "cnpj": cnpj,
            "total_titulos": len(titulos),
            "valor_total": valor_total,
            "resumo_por_tipo": resumo,
            "titulos": titulos
        })
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/api/titulos-anbima/fundos')
def get_titulos_anbima_fundos():
    """Lista fundos que possuem titulos ANBIMA em suas carteiras"""
    try:
        conn = get_connection()
        cursor = conn.cursor()

        page = int(request.args.get('page', 1))
        per_page = int(request.args.get('per_page', 50))
        tipo_titulo = request.args.get('tipo', '')  # CRI/CRA ou Debenture
        codigo = request.args.get('codigo', '')

        offset = (page - 1) * per_page
        where_sql = "1=1"
        params = []

        if tipo_titulo:
            where_sql += " AND tipo_titulo_anbima = %s"
            params.append(tipo_titulo)

        if codigo:
            where_sql += " AND codigo_anbima ILIKE %s"
            params.append(f'%{codigo}%')

        # Total
        cursor.execute(f"""
            SELECT COUNT(DISTINCT cnpj_fundo) FROM fundos.carteira_titulos_anbima WHERE {where_sql}
        """, params)
        total_fundos = cursor.fetchone()[0]

        # Dados agregados por fundo
        cursor.execute(f"""
            SELECT cnpj_fundo, nome_fundo,
                   COUNT(*) as total_titulos,
                   SUM(valor_mercado) as valor_total,
                   array_agg(DISTINCT tipo_titulo_anbima) as tipos
            FROM fundos.carteira_titulos_anbima
            WHERE {where_sql}
            GROUP BY cnpj_fundo, nome_fundo
            ORDER BY SUM(valor_mercado) DESC NULLS LAST
            LIMIT %s OFFSET %s
        """, params + [per_page, offset])

        fundos = []
        for row in cursor.fetchall():
            fundos.append({
                'cnpj': row[0],
                'nome': row[1],
                'total_titulos': row[2],
                'valor_total': float(row[3]) if row[3] else 0,
                'tipos': row[4]
            })

        # Stats
        cursor.execute(f"""
            SELECT tipo_titulo_anbima, COUNT(*), COUNT(DISTINCT cnpj_fundo), SUM(valor_mercado)
            FROM fundos.carteira_titulos_anbima
            WHERE {where_sql}
            GROUP BY tipo_titulo_anbima
        """, params)

        stats = {
            'total_fundos': total_fundos,
            'por_tipo': [{'tipo': r[0], 'posicoes': r[1], 'fundos': r[2], 'valor': float(r[3]) if r[3] else 0}
                         for r in cursor.fetchall()]
        }

        cursor.close()
        conn.close()

        return jsonify({
            "success": True,
            "data": fundos,
            "stats": stats,
            "pagination": {
                "page": page,
                "per_page": per_page,
                "total": total_fundos,
                "total_pages": (total_fundos + per_page - 1) // per_page
            }
        })
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/api/fundos/consolidados')
def get_fundos_consolidados():
    """Lista fundos consolidados (ANBIMA + CVM)"""
    try:
        conn = get_connection()
        cursor = conn.cursor()

        page = int(request.args.get('page', 1))
        per_page = int(request.args.get('per_page', 50))
        search = request.args.get('search', '')
        fonte = request.args.get('fonte', '')
        categoria = request.args.get('categoria', '')
        classe_cvm = request.args.get('classe_cvm', '')
        esg_only = request.args.get('esg_only', '').lower() == 'true'
        com_cvm = request.args.get('com_cvm', '').lower() == 'true'

        offset = (page - 1) * per_page
        where_sql = "1=1"
        params = []

        if search:
            where_sql += " AND (nome ILIKE %s OR cnpj ILIKE %s)"
            params.extend([f'%{search}%', f'%{search}%'])

        if fonte:
            where_sql += " AND fonte_principal = %s"
            params.append(fonte)

        if categoria:
            where_sql += " AND categoria_anbima ILIKE %s"
            params.append(f'%{categoria}%')

        if classe_cvm:
            where_sql += " AND classe_cvm = %s"
            params.append(classe_cvm)

        if esg_only:
            where_sql += " AND fundo_esg = TRUE"

        if com_cvm:
            where_sql += " AND tem_dados_cvm = TRUE"

        # Contar total
        cursor.execute(f"SELECT COUNT(*) FROM fundos.fundos_consolidados WHERE {where_sql}", params)
        total = cursor.fetchone()[0]

        # Buscar dados
        cursor.execute(f"""
            SELECT fonte_principal, cnpj, nome, categoria_anbima, tipo_anbima,
                   fundo_esg, gestor_anbima, administrador_anbima,
                   patrimonio_liquido, numero_cotistas, valor_cota,
                   foco_atuacao, nivel1_categoria, nivel2_categoria,
                   classe_cvm, situacao_cvm, tem_dados_cvm,
                   captacao_dia, resgate_dia
            FROM fundos.fundos_consolidados
            WHERE {where_sql}
            ORDER BY patrimonio_liquido DESC NULLS LAST
            LIMIT %s OFFSET %s
        """, params + [per_page, offset])

        data = query_to_dict(cursor)

        # Estatisticas
        cursor.execute("""
            SELECT
                COUNT(*) as total,
                COUNT(CASE WHEN fonte_principal = 'ANBIMA' THEN 1 END) as anbima,
                COUNT(CASE WHEN fonte_principal = 'CVM' THEN 1 END) as cvm,
                COUNT(CASE WHEN tem_dados_cvm = TRUE THEN 1 END) as com_dados_cvm,
                COUNT(CASE WHEN fundo_esg = TRUE THEN 1 END) as esg,
                SUM(patrimonio_liquido) as pl_total,
                SUM(numero_cotistas) as cotistas_total
            FROM fundos.fundos_consolidados
        """)
        row = cursor.fetchone()
        stats = {
            'total': row[0],
            'anbima': row[1],
            'cvm': row[2],
            'com_dados_cvm': row[3],
            'esg': row[4],
            'pl_total': float(row[5]) if row[5] else 0,
            'cotistas_total': int(row[6]) if row[6] else 0
        }

        # Filtros disponiveis
        cursor.execute("SELECT DISTINCT fonte_principal FROM fundos.fundos_consolidados ORDER BY fonte_principal")
        fontes = [r[0] for r in cursor.fetchall()]

        cursor.execute("""
            SELECT DISTINCT categoria_anbima FROM fundos.fundos_consolidados
            WHERE categoria_anbima IS NOT NULL ORDER BY categoria_anbima LIMIT 50
        """)
        categorias = [r[0] for r in cursor.fetchall()]

        cursor.execute("""
            SELECT DISTINCT classe_cvm FROM fundos.fundos_consolidados
            WHERE classe_cvm IS NOT NULL ORDER BY classe_cvm
        """)
        classes = [r[0] for r in cursor.fetchall()]

        cursor.close()
        conn.close()

        return jsonify({
            "success": True,
            "data": data,
            "total": total,
            "page": page,
            "per_page": per_page,
            "pages": (total + per_page - 1) // per_page,
            "stats": stats,
            "filtros": {
                "fontes": fontes,
                "categorias": categorias,
                "classes_cvm": classes
            }
        })
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/api/fundos/consolidados/stats')
def get_fundos_consolidados_stats():
    """Estatisticas dos fundos consolidados"""
    try:
        conn = get_connection()
        cursor = conn.cursor()

        stats = {}

        # Totais por fonte
        cursor.execute("""
            SELECT fonte_principal, COUNT(*), SUM(patrimonio_liquido), SUM(numero_cotistas)
            FROM fundos.fundos_consolidados
            GROUP BY fonte_principal
        """)
        stats['por_fonte'] = []
        for row in cursor.fetchall():
            stats['por_fonte'].append({
                'fonte': row[0],
                'total': row[1],
                'pl_total': float(row[2]) if row[2] else 0,
                'cotistas': int(row[3]) if row[3] else 0
            })

        # Top 10 por PL
        cursor.execute("""
            SELECT fonte_principal, cnpj, nome, patrimonio_liquido, numero_cotistas,
                   categoria_anbima, classe_cvm
            FROM fundos.fundos_consolidados
            WHERE patrimonio_liquido > 0
            ORDER BY patrimonio_liquido DESC
            LIMIT 10
        """)
        stats['top_pl'] = query_to_dict(cursor)

        # Cruzamento ANBIMA x CVM
        cursor.execute("""
            SELECT
                COUNT(*) FILTER (WHERE fonte_principal = 'ANBIMA' AND tem_dados_cvm = TRUE) as anbima_com_cvm,
                COUNT(*) FILTER (WHERE fonte_principal = 'ANBIMA' AND tem_dados_cvm = FALSE) as anbima_sem_cvm,
                COUNT(*) FILTER (WHERE fonte_principal = 'CVM') as apenas_cvm
            FROM fundos.fundos_consolidados
        """)
        row = cursor.fetchone()
        stats['cruzamento'] = {
            'anbima_com_cvm': row[0],
            'anbima_sem_cvm': row[1],
            'apenas_cvm': row[2]
        }

        # ESG
        cursor.execute("""
            SELECT fonte_principal, COUNT(*)
            FROM fundos.fundos_consolidados
            WHERE fundo_esg = TRUE
            GROUP BY fonte_principal
        """)
        stats['esg_por_fonte'] = [{'fonte': r[0], 'total': r[1]} for r in cursor.fetchall()]

        # Top classes CVM
        cursor.execute("""
            SELECT classe_cvm, COUNT(*) as total
            FROM fundos.fundos_consolidados
            WHERE classe_cvm IS NOT NULL
            GROUP BY classe_cvm
            ORDER BY total DESC
            LIMIT 10
        """)
        stats['top_classes_cvm'] = [{'classe': r[0], 'total': r[1]} for r in cursor.fetchall()]

        cursor.close()
        conn.close()

        return jsonify({"success": True, "data": stats})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

if __name__ == '__main__':
    print("=" * 60)
    print("SERVIDOR DASHBOARD + API (PostgreSQL)")
    print("=" * 60)
    print("Acesse: http://localhost:5000")
    print("=" * 60)
    app.run(debug=True, host='0.0.0.0', port=5000)
