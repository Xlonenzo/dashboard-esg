"""
Servidor simples para o Dashboard
Serve arquivos estáticos e redireciona API
"""
import os
from pathlib import Path
from flask import Flask, send_from_directory, jsonify, request
from flask_cors import CORS
import psycopg2

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

        conn = get_connection()
        cursor = conn.cursor()

        where_clauses = ["ativo = true"]
        params = []

        if search:
            where_clauses.append("(nomecomercial ILIKE %s OR cnpj ILIKE %s OR razaosocial ILIKE %s)")
            params.extend([f"%{search}%", f"%{search}%", f"%{search}%"])
        if categoria:
            where_clauses.append("categoria = %s")
            params.append(categoria)
        if tipo:
            where_clauses.append("categoriaesg = %s")
            params.append(tipo)

        where_sql = " AND ".join(where_clauses)

        cursor.execute(f"SELECT COUNT(*) FROM fundos.todosfundos WHERE {where_sql}", params)
        total = cursor.fetchone()[0]

        cursor.execute(f"""
            SELECT codigofundo, cnpj, razaosocial, nomecomercial, tipofundo, categoria,
                   COALESCE(categoriaesg, 'Convencional') as categoriaesg,
                   COALESCE(focoesg, 'Não aplicável') as focoesg
            FROM fundos.todosfundos WHERE {where_sql}
            ORDER BY nomecomercial
            LIMIT %s OFFSET %s
        """, params + [per_page, offset])

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
        cursor.execute("""
            SELECT DISTINCT categoria, COUNT(*) as qtd FROM fundos.todosfundos
            WHERE ativo = true AND categoria IS NOT NULL GROUP BY categoria ORDER BY qtd DESC
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
        cursor.execute("SELECT COUNT(*) FROM fundos.todosfundos WHERE ativo = true")
        total = cursor.fetchone()[0]
        cursor.execute("""
            SELECT COALESCE(categoriaesg, 'Convencional') as tipo, COUNT(*) as qtd
            FROM fundos.todosfundos WHERE ativo = true GROUP BY categoriaesg
        """)
        por_tipo = {row[0]: row[1] for row in cursor.fetchall()}
        cursor.execute("""
            SELECT categoria, COUNT(*) as qtd FROM fundos.todosfundos
            WHERE ativo = true AND categoria IS NOT NULL GROUP BY categoria ORDER BY qtd DESC LIMIT 10
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

        # KPIs preenchidos pela empresa
        cursor.execute("""
            SELECT codigokpi, valor, status
            FROM tsb.kpisempresa WHERE empresaid = %s
        """, (empresa_id,))
        valores = {row[0]: {'valor': row[1], 'status': row[2]} for row in cursor.fetchall()}

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

if __name__ == '__main__':
    print("=" * 60)
    print("SERVIDOR DASHBOARD + API (PostgreSQL)")
    print("=" * 60)
    print("Acesse: http://localhost:5000")
    print("=" * 60)
    app.run(debug=True, host='0.0.0.0', port=5000)
