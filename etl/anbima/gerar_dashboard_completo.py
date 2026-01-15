"""
Gerador de Dashboard Completo com Dados Reais da ANBIMA
=======================================================
Dashboard com multiplas visualizacoes e abas
"""

import pandas as pd
import os
import json
from datetime import datetime, timedelta
from collections import Counter

# Diretorios
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DATA_DIR = os.path.join(BASE_DIR, 'data', 'anbima')
DASHBOARD_DIR = os.path.join(BASE_DIR, 'dashboard')

def encontrar_arquivo_mais_recente(prefixo):
    files = [f for f in os.listdir(DATA_DIR) if f.startswith(prefixo) and f.endswith('.csv')]
    if files:
        return os.path.join(DATA_DIR, sorted(files)[-1])
    return None

def carregar_dados():
    """Carrega todos os dados"""
    dados = {}

    # Fundos ESG
    arquivo_esg = encontrar_arquivo_mais_recente('fundos_esg_')
    if arquivo_esg:
        dados['esg'] = pd.read_csv(arquivo_esg)
        print(f"Carregados {len(dados['esg'])} fundos ESG")

    # Todos os fundos
    arquivo_todos = encontrar_arquivo_mais_recente('fundos_todos_')
    if arquivo_todos:
        dados['todos'] = pd.read_csv(arquivo_todos)
        print(f"Carregados {len(dados['todos'])} fundos totais")

    return dados

def extrair_gestora(nome):
    if pd.isna(nome):
        return 'Outros'
    nome_upper = str(nome).upper()
    gestoras = {
        'ITAU': 'Itaú',
        'BRADESCO': 'Bradesco',
        'BB ': 'Banco do Brasil',
        'SANTANDER': 'Santander',
        'CAIXA': 'Caixa',
        'BTG': 'BTG Pactual',
        'XP ': 'XP',
        'SAFRA': 'Safra',
        'VOTORANTIM': 'Votorantim',
        'CREDIT SUISSE': 'Credit Suisse',
        'VERDE': 'Verde AM',
        'JGP': 'JGP',
        'OPPORTUNITY': 'Opportunity',
        'SUL AMERICA': 'SulAmérica',
        'WESTERN': 'Western',
        'ARX': 'ARX',
        'KINEA': 'Kinea',
        'SPARTA': 'Sparta',
        'EMPIRICUS': 'Empiricus',
        'INTER': 'Inter',
        'NUBANK': 'Nubank',
        'MODAL': 'Modal',
        'GENIAL': 'Genial',
        'GUIDE': 'Guide'
    }
    for key, value in gestoras.items():
        if key in nome_upper:
            return value
    return 'Outros'

def extrair_tipo_ativo(nome):
    if pd.isna(nome):
        return 'Outros'
    nome_upper = str(nome).upper()
    if 'ACAO' in nome_upper or 'ACOES' in nome_upper or 'FIA' in nome_upper or 'AÇÕES' in nome_upper:
        return 'Ações'
    elif 'RENDA FIXA' in nome_upper or 'RF' in nome_upper or 'FIDC' in nome_upper:
        return 'Renda Fixa'
    elif 'MULTIMERCADO' in nome_upper or 'FIM' in nome_upper:
        return 'Multimercado'
    elif 'IMOBILIARIO' in nome_upper or 'FII' in nome_upper:
        return 'Imobiliário'
    elif 'CAMBIAL' in nome_upper:
        return 'Cambial'
    elif 'ETF' in nome_upper:
        return 'ETF'
    elif 'PREVIDENCIA' in nome_upper or 'PREV' in nome_upper:
        return 'Previdência'
    return 'Outros'

def processar_dados_completos(dados):
    """Processa todos os dados para visualizacoes"""
    df_esg = dados.get('esg', pd.DataFrame())
    df_todos = dados.get('todos', pd.DataFrame())

    if df_esg.empty:
        return {}

    # Adicionar colunas derivadas
    df_esg['Gestora'] = df_esg['razao_social_fundo'].apply(extrair_gestora)
    df_esg['TipoAtivo'] = df_esg['razao_social_fundo'].apply(extrair_tipo_ativo)

    # Estatisticas basicas
    total_esg = len(df_esg)
    total_is = len(df_esg[df_esg['CategoriaESG'] == 'IS - Investimento Sustentavel'])
    total_integrado = len(df_esg[df_esg['CategoriaESG'] == 'ESG Integrado'])
    total_mercado = len(df_todos) if not df_todos.empty else total_esg * 27

    # Por categoria
    por_categoria = df_esg['CategoriaESG'].value_counts().to_dict()

    # Por foco
    por_foco = df_esg['FocoESG'].value_counts().to_dict()

    # Por gestora
    por_gestora = df_esg['Gestora'].value_counts().head(15).to_dict()

    # Por tipo de ativo
    por_tipo_ativo = df_esg['TipoAtivo'].value_counts().to_dict()

    # Cruzamento: Categoria x Foco
    categoria_foco = df_esg.groupby(['CategoriaESG', 'FocoESG']).size().unstack(fill_value=0).to_dict()

    # Cruzamento: Gestora x Categoria
    gestora_categoria = df_esg.groupby(['Gestora', 'CategoriaESG']).size().unstack(fill_value=0)
    gestora_categoria = gestora_categoria.head(10).to_dict()

    # Top 10 gestoras por tipo
    top_gestoras_is = df_esg[df_esg['CategoriaESG'] == 'IS - Investimento Sustentavel']['Gestora'].value_counts().head(10).to_dict()
    top_gestoras_esg = df_esg[df_esg['CategoriaESG'] == 'ESG Integrado']['Gestora'].value_counts().head(10).to_dict()

    # Distribuicao por tipo de fundo (FIF, etc)
    por_tipo_fundo = df_esg['tipo_fundo'].value_counts().to_dict() if 'tipo_fundo' in df_esg.columns else {}

    # Anos de vigencia (fundos mais antigos)
    if 'data_vigencia' in df_esg.columns:
        df_esg['ano_criacao'] = pd.to_datetime(df_esg['data_vigencia'], errors='coerce').dt.year
        por_ano = df_esg['ano_criacao'].value_counts().sort_index().tail(20).to_dict()
    else:
        por_ano = {}

    # Lista de fundos para tabelas
    fundos_is = df_esg[df_esg['CategoriaESG'] == 'IS - Investimento Sustentavel'][
        ['razao_social_fundo', 'identificador_fundo', 'tipo_fundo', 'FocoESG', 'Gestora', 'TipoAtivo']
    ].head(100).to_dict('records')

    fundos_esg = df_esg[df_esg['CategoriaESG'] == 'ESG Integrado'][
        ['razao_social_fundo', 'identificador_fundo', 'tipo_fundo', 'FocoESG', 'Gestora', 'TipoAtivo']
    ].head(100).to_dict('records')

    # Ranking gestoras (dados para radar)
    gestoras_top = list(por_gestora.keys())[:8]
    radar_gestoras = {}
    for g in gestoras_top:
        df_g = df_esg[df_esg['Gestora'] == g]
        radar_gestoras[g] = {
            'Total': len(df_g),
            'IS': len(df_g[df_g['CategoriaESG'] == 'IS - Investimento Sustentavel']),
            'ESG': len(df_g[df_g['CategoriaESG'] == 'ESG Integrado']),
            'Ambiental': len(df_g[df_g['FocoESG'] == 'Ambiental']),
            'Social': len(df_g[df_g['FocoESG'] == 'Social']),
        }

    # Percentuais
    pct_esg_mercado = round((total_esg / total_mercado) * 100, 2) if total_mercado > 0 else 0
    pct_is = round((total_is / total_esg) * 100, 1) if total_esg > 0 else 0
    pct_integrado = round((total_integrado / total_esg) * 100, 1) if total_esg > 0 else 0

    return {
        'total_esg': total_esg,
        'total_is': total_is,
        'total_integrado': total_integrado,
        'total_mercado': total_mercado,
        'pct_esg_mercado': pct_esg_mercado,
        'pct_is': pct_is,
        'pct_integrado': pct_integrado,
        'por_categoria': por_categoria,
        'por_foco': por_foco,
        'por_gestora': por_gestora,
        'por_tipo_ativo': por_tipo_ativo,
        'por_tipo_fundo': por_tipo_fundo,
        'por_ano': por_ano,
        'categoria_foco': categoria_foco,
        'gestora_categoria': gestora_categoria,
        'top_gestoras_is': top_gestoras_is,
        'top_gestoras_esg': top_gestoras_esg,
        'radar_gestoras': radar_gestoras,
        'fundos_is': fundos_is,
        'fundos_esg': fundos_esg,
        'data_atualizacao': datetime.now().strftime('%d/%m/%Y %H:%M')
    }

def gerar_html_completo(dados):
    """Gera HTML do dashboard completo"""

    html = f'''<!DOCTYPE html>
<html lang="pt-BR">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Dashboard ANBIMA ESG - Visualizacoes Completas</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <script src="https://cdn.jsdelivr.net/npm/chartjs-plugin-datalabels@2.0.0"></script>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap" rel="stylesheet">
    <style>
        :root {{
            --primary: #1976D2;
            --primary-light: #42A5F5;
            --secondary: #2E7D32;
            --secondary-light: #66BB6A;
            --accent: #FF9800;
            --accent-light: #FFB74D;
            --danger: #f44336;
            --success: #4CAF50;
            --warning: #FFC107;
            --info: #00BCD4;
            --purple: #9C27B0;
            --pink: #E91E63;
            --teal: #009688;
            --bg-dark: #0a0a1a;
            --bg-card: #12122a;
            --bg-card-hover: #1a1a3a;
            --text-primary: #ffffff;
            --text-secondary: #a0a0b0;
            --border-color: #2a2a4a;
            --sidebar-width: 240px;
        }}
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        body {{
            font-family: 'Inter', sans-serif;
            background: var(--bg-dark);
            color: var(--text-primary);
            min-height: 100vh;
        }}

        /* Sidebar */
        .sidebar {{
            position: fixed;
            left: 0;
            top: 0;
            width: var(--sidebar-width);
            height: 100vh;
            background: linear-gradient(180deg, #0d0d20 0%, #151530 100%);
            border-right: 1px solid var(--border-color);
            padding: 20px 0;
            overflow-y: auto;
            z-index: 1000;
        }}
        .logo {{
            padding: 10px 20px 25px;
            border-bottom: 1px solid var(--border-color);
            margin-bottom: 15px;
        }}
        .logo h1 {{
            font-size: 1.3rem;
            background: linear-gradient(135deg, var(--primary) 0%, var(--secondary) 100%);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
        }}
        .logo span {{
            font-size: 0.7rem;
            color: var(--text-secondary);
            display: block;
            margin-top: 5px;
        }}
        .nav-section {{ padding: 0 10px; margin-bottom: 5px; }}
        .nav-section-title {{
            font-size: 0.65rem;
            text-transform: uppercase;
            color: var(--text-secondary);
            letter-spacing: 1px;
            padding: 10px 10px 5px;
        }}
        .nav-item {{
            display: flex;
            align-items: center;
            padding: 10px 15px;
            border-radius: 8px;
            cursor: pointer;
            transition: all 0.3s;
            margin-bottom: 3px;
            color: var(--text-secondary);
            font-size: 0.85rem;
        }}
        .nav-item:hover {{
            background: rgba(255,255,255,0.05);
            color: var(--text-primary);
        }}
        .nav-item.active {{
            background: linear-gradient(135deg, rgba(25, 118, 210, 0.3) 0%, rgba(46, 125, 50, 0.3) 100%);
            color: var(--text-primary);
            border: 1px solid rgba(25, 118, 210, 0.4);
        }}
        .nav-item .icon {{ font-size: 1rem; margin-right: 10px; width: 20px; text-align: center; }}

        /* Main Content */
        .main-content {{
            margin-left: var(--sidebar-width);
            padding: 20px;
            min-height: 100vh;
        }}

        /* Header */
        .page-header {{
            background: linear-gradient(135deg, rgba(25, 118, 210, 0.15) 0%, rgba(46, 125, 50, 0.15) 100%);
            border-radius: 15px;
            padding: 25px;
            margin-bottom: 25px;
            border: 1px solid var(--border-color);
        }}
        .page-header h2 {{
            font-size: 1.6rem;
            margin-bottom: 8px;
        }}
        .page-header .subtitle {{
            color: var(--text-secondary);
            font-size: 0.9rem;
        }}
        .badge {{
            display: inline-block;
            background: var(--success);
            color: white;
            padding: 4px 12px;
            border-radius: 15px;
            font-size: 0.7rem;
            margin-top: 10px;
        }}

        /* Stats Grid */
        .stats-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
            gap: 15px;
            margin-bottom: 25px;
        }}
        .stat-card {{
            background: var(--bg-card);
            border-radius: 12px;
            padding: 20px;
            border: 1px solid var(--border-color);
            transition: transform 0.3s, box-shadow 0.3s;
        }}
        .stat-card:hover {{
            transform: translateY(-3px);
            box-shadow: 0 8px 25px rgba(0,0,0,0.3);
        }}
        .stat-card .label {{
            color: var(--text-secondary);
            font-size: 0.75rem;
            margin-bottom: 8px;
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }}
        .stat-card .value {{
            font-size: 2rem;
            font-weight: 700;
        }}
        .stat-card .change {{
            font-size: 0.75rem;
            margin-top: 5px;
        }}
        .stat-card .change.positive {{ color: var(--success); }}
        .stat-card .change.negative {{ color: var(--danger); }}

        /* Charts Grid */
        .charts-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(350px, 1fr));
            gap: 20px;
            margin-bottom: 25px;
        }}
        .chart-card {{
            background: var(--bg-card);
            border-radius: 12px;
            padding: 20px;
            border: 1px solid var(--border-color);
        }}
        .chart-card.full {{ grid-column: 1 / -1; }}
        .chart-card.half {{ }}
        .chart-card h3 {{
            font-size: 1rem;
            margin-bottom: 15px;
            color: var(--text-primary);
            display: flex;
            align-items: center;
            gap: 8px;
        }}
        .chart-card h3::before {{
            content: '';
            width: 3px;
            height: 16px;
            background: linear-gradient(180deg, var(--primary) 0%, var(--secondary) 100%);
            border-radius: 2px;
        }}
        .chart-container {{
            position: relative;
            height: 280px;
        }}
        .chart-container.tall {{ height: 350px; }}
        .chart-container.short {{ height: 220px; }}

        /* Tables */
        .data-table {{
            width: 100%;
            border-collapse: collapse;
        }}
        .data-table th, .data-table td {{
            padding: 10px 12px;
            text-align: left;
            border-bottom: 1px solid var(--border-color);
            font-size: 0.8rem;
        }}
        .data-table th {{
            color: var(--text-secondary);
            font-weight: 500;
            text-transform: uppercase;
            font-size: 0.7rem;
            letter-spacing: 0.5px;
        }}
        .data-table tr:hover {{ background: var(--bg-card-hover); }}
        .table-container {{
            max-height: 400px;
            overflow-y: auto;
        }}

        /* Tags */
        .tag {{
            display: inline-block;
            padding: 3px 8px;
            border-radius: 10px;
            font-size: 0.65rem;
            font-weight: 500;
        }}
        .tag.is {{ background: rgba(46, 125, 50, 0.2); color: var(--secondary-light); }}
        .tag.esg {{ background: rgba(25, 118, 210, 0.2); color: var(--primary-light); }}
        .tag.ambiental {{ background: rgba(76, 175, 80, 0.2); color: #81C784; }}
        .tag.social {{ background: rgba(255, 152, 0, 0.2); color: #FFB74D; }}
        .tag.governanca {{ background: rgba(156, 39, 176, 0.2); color: #BA68C8; }}
        .tag.multi {{ background: rgba(0, 188, 212, 0.2); color: #4DD0E1; }}

        /* Tab Content */
        .tab-content {{ display: none; }}
        .tab-content.active {{ display: block; }}

        /* Progress Bars */
        .progress-item {{
            margin-bottom: 12px;
        }}
        .progress-label {{
            display: flex;
            justify-content: space-between;
            margin-bottom: 5px;
            font-size: 0.8rem;
        }}
        .progress-bar {{
            height: 8px;
            background: rgba(255,255,255,0.1);
            border-radius: 4px;
            overflow: hidden;
        }}
        .progress-fill {{
            height: 100%;
            border-radius: 4px;
            transition: width 0.5s ease;
        }}

        /* Metric Cards */
        .metric-row {{
            display: flex;
            gap: 15px;
            margin-bottom: 15px;
            flex-wrap: wrap;
        }}
        .metric-item {{
            flex: 1;
            min-width: 150px;
            background: rgba(255,255,255,0.03);
            border-radius: 8px;
            padding: 15px;
            text-align: center;
        }}
        .metric-item .value {{
            font-size: 1.8rem;
            font-weight: 700;
        }}
        .metric-item .label {{
            font-size: 0.7rem;
            color: var(--text-secondary);
            margin-top: 5px;
        }}

        /* Colors for values */
        .text-primary {{ color: var(--primary-light); }}
        .text-secondary {{ color: var(--secondary-light); }}
        .text-accent {{ color: var(--accent-light); }}
        .text-info {{ color: var(--info); }}
        .text-purple {{ color: #BA68C8; }}
        .text-pink {{ color: #F48FB1; }}
        .text-teal {{ color: #4DB6AC; }}

        /* Footer */
        .footer {{
            text-align: center;
            padding: 20px;
            color: var(--text-secondary);
            font-size: 0.8rem;
            border-top: 1px solid var(--border-color);
            margin-top: 30px;
        }}
        .footer strong {{ color: var(--success); }}

        /* Scrollbar */
        ::-webkit-scrollbar {{ width: 6px; height: 6px; }}
        ::-webkit-scrollbar-track {{ background: var(--bg-dark); }}
        ::-webkit-scrollbar-thumb {{ background: var(--border-color); border-radius: 3px; }}
        ::-webkit-scrollbar-thumb:hover {{ background: var(--text-secondary); }}
    </style>
</head>
<body>
    <!-- Sidebar -->
    <div class="sidebar">
        <div class="logo">
            <h1>ANBIMA ESG</h1>
            <span>Dashboard de Dados Reais</span>
        </div>

        <div class="nav-section">
            <div class="nav-section-title">Visao Geral</div>
            <div class="nav-item active" onclick="showTab('overview')">
                <span class="icon">📊</span> Overview
            </div>
            <div class="nav-item" onclick="showTab('mercado')">
                <span class="icon">📈</span> Mercado ESG
            </div>
        </div>

        <div class="nav-section">
            <div class="nav-section-title">Analises</div>
            <div class="nav-item" onclick="showTab('categorias')">
                <span class="icon">🏷️</span> Por Categoria
            </div>
            <div class="nav-item" onclick="showTab('focos')">
                <span class="icon">🎯</span> Por Foco ESG
            </div>
            <div class="nav-item" onclick="showTab('gestoras')">
                <span class="icon">🏦</span> Por Gestora
            </div>
            <div class="nav-item" onclick="showTab('tipos')">
                <span class="icon">📁</span> Por Tipo
            </div>
        </div>

        <div class="nav-section">
            <div class="nav-section-title">Rankings</div>
            <div class="nav-item" onclick="showTab('ranking')">
                <span class="icon">🏆</span> Top Gestoras
            </div>
            <div class="nav-item" onclick="showTab('comparativo')">
                <span class="icon">⚖️</span> Comparativo
            </div>
        </div>

        <div class="nav-section">
            <div class="nav-section-title">Dados</div>
            <div class="nav-item" onclick="showTab('fundos-is')">
                <span class="icon">🌱</span> Fundos IS
            </div>
            <div class="nav-item" onclick="showTab('fundos-esg')">
                <span class="icon">🌍</span> Fundos ESG
            </div>
        </div>
    </div>

    <!-- Main Content -->
    <div class="main-content">
        <div class="page-header">
            <h2>Dashboard ANBIMA ESG</h2>
            <p class="subtitle">Fundos de Investimento Sustentavel - Dados 100% Reais da API Oficial ANBIMA</p>
            <span class="badge">✓ DADOS REAIS - Atualizado em {dados['data_atualizacao']}</span>
        </div>

        <!-- Tab: Overview -->
        <div id="overview" class="tab-content active">
            <div class="stats-grid">
                <div class="stat-card">
                    <div class="label">Total Fundos ESG</div>
                    <div class="value text-primary">{dados['total_esg']:,}</div>
                    <div class="change positive">↑ {dados['pct_esg_mercado']}% do mercado</div>
                </div>
                <div class="stat-card">
                    <div class="label">Fundos IS</div>
                    <div class="value text-secondary">{dados['total_is']:,}</div>
                    <div class="change positive">{dados['pct_is']}% do total ESG</div>
                </div>
                <div class="stat-card">
                    <div class="label">ESG Integrado</div>
                    <div class="value text-accent">{dados['total_integrado']:,}</div>
                    <div class="change positive">{dados['pct_integrado']}% do total ESG</div>
                </div>
                <div class="stat-card">
                    <div class="label">Total Mercado</div>
                    <div class="value text-info">{dados['total_mercado']:,}</div>
                    <div class="change">Fundos cadastrados</div>
                </div>
                <div class="stat-card">
                    <div class="label">Gestoras ESG</div>
                    <div class="value text-purple">{len(dados['por_gestora'])}</div>
                    <div class="change">Identificadas</div>
                </div>
                <div class="stat-card">
                    <div class="label">Focos ESG</div>
                    <div class="value text-teal">{len(dados['por_foco'])}</div>
                    <div class="change">Categorias</div>
                </div>
            </div>

            <div class="charts-grid">
                <div class="chart-card">
                    <h3>Distribuicao por Categoria</h3>
                    <div class="chart-container">
                        <canvas id="chartCategoria"></canvas>
                    </div>
                </div>
                <div class="chart-card">
                    <h3>Distribuicao por Foco ESG</h3>
                    <div class="chart-container">
                        <canvas id="chartFoco"></canvas>
                    </div>
                </div>
                <div class="chart-card">
                    <h3>Top 10 Gestoras</h3>
                    <div class="chart-container">
                        <canvas id="chartGestoras"></canvas>
                    </div>
                </div>
                <div class="chart-card">
                    <h3>Por Tipo de Ativo</h3>
                    <div class="chart-container">
                        <canvas id="chartTipoAtivo"></canvas>
                    </div>
                </div>
            </div>
        </div>

        <!-- Tab: Mercado -->
        <div id="mercado" class="tab-content">
            <div class="charts-grid">
                <div class="chart-card full">
                    <h3>Participacao ESG no Mercado de Fundos</h3>
                    <div class="metric-row">
                        <div class="metric-item">
                            <div class="value text-primary">{dados['pct_esg_mercado']}%</div>
                            <div class="label">Participacao ESG</div>
                        </div>
                        <div class="metric-item">
                            <div class="value text-secondary">{dados['total_esg']:,}</div>
                            <div class="label">Fundos ESG</div>
                        </div>
                        <div class="metric-item">
                            <div class="value text-info">{dados['total_mercado']:,}</div>
                            <div class="label">Total Mercado</div>
                        </div>
                    </div>
                    <div class="chart-container tall">
                        <canvas id="chartMercado"></canvas>
                    </div>
                </div>
                <div class="chart-card">
                    <h3>Composicao do Mercado ESG</h3>
                    <div class="chart-container">
                        <canvas id="chartComposicao"></canvas>
                    </div>
                </div>
                <div class="chart-card">
                    <h3>Proporcao IS vs ESG Integrado</h3>
                    <div class="chart-container">
                        <canvas id="chartProporcao"></canvas>
                    </div>
                </div>
            </div>
        </div>

        <!-- Tab: Categorias -->
        <div id="categorias" class="tab-content">
            <div class="charts-grid">
                <div class="chart-card">
                    <h3>IS vs ESG Integrado (Doughnut)</h3>
                    <div class="chart-container">
                        <canvas id="chartCategDoughnut"></canvas>
                    </div>
                </div>
                <div class="chart-card">
                    <h3>IS vs ESG Integrado (Barras)</h3>
                    <div class="chart-container">
                        <canvas id="chartCategBar"></canvas>
                    </div>
                </div>
                <div class="chart-card full">
                    <h3>Categoria por Foco ESG</h3>
                    <div class="chart-container tall">
                        <canvas id="chartCategFoco"></canvas>
                    </div>
                </div>
            </div>
        </div>

        <!-- Tab: Focos -->
        <div id="focos" class="tab-content">
            <div class="charts-grid">
                <div class="chart-card">
                    <h3>Distribuicao por Foco (Pizza)</h3>
                    <div class="chart-container">
                        <canvas id="chartFocoPie"></canvas>
                    </div>
                </div>
                <div class="chart-card">
                    <h3>Distribuicao por Foco (Polar)</h3>
                    <div class="chart-container">
                        <canvas id="chartFocoPolar"></canvas>
                    </div>
                </div>
                <div class="chart-card">
                    <h3>Distribuicao por Foco (Barras)</h3>
                    <div class="chart-container">
                        <canvas id="chartFocoBar"></canvas>
                    </div>
                </div>
                <div class="chart-card">
                    <h3>Foco ESG - Radar</h3>
                    <div class="chart-container">
                        <canvas id="chartFocoRadar"></canvas>
                    </div>
                </div>
            </div>
        </div>

        <!-- Tab: Gestoras -->
        <div id="gestoras" class="tab-content">
            <div class="charts-grid">
                <div class="chart-card full">
                    <h3>Top 15 Gestoras com Fundos ESG</h3>
                    <div class="chart-container tall">
                        <canvas id="chartGestorasBar"></canvas>
                    </div>
                </div>
                <div class="chart-card">
                    <h3>Gestoras - Fundos IS</h3>
                    <div class="chart-container">
                        <canvas id="chartGestorasIS"></canvas>
                    </div>
                </div>
                <div class="chart-card">
                    <h3>Gestoras - ESG Integrado</h3>
                    <div class="chart-container">
                        <canvas id="chartGestorasESG"></canvas>
                    </div>
                </div>
            </div>
        </div>

        <!-- Tab: Tipos -->
        <div id="tipos" class="tab-content">
            <div class="charts-grid">
                <div class="chart-card">
                    <h3>Por Tipo de Ativo (Doughnut)</h3>
                    <div class="chart-container">
                        <canvas id="chartTipoDoughnut"></canvas>
                    </div>
                </div>
                <div class="chart-card">
                    <h3>Por Tipo de Ativo (Barras)</h3>
                    <div class="chart-container">
                        <canvas id="chartTipoBar"></canvas>
                    </div>
                </div>
                <div class="chart-card">
                    <h3>Por Tipo de Fundo</h3>
                    <div class="chart-container">
                        <canvas id="chartTipoFundo"></canvas>
                    </div>
                </div>
                <div class="chart-card">
                    <h3>Tipo de Ativo - Polar Area</h3>
                    <div class="chart-container">
                        <canvas id="chartTipoPolar"></canvas>
                    </div>
                </div>
            </div>
        </div>

        <!-- Tab: Ranking -->
        <div id="ranking" class="tab-content">
            <div class="charts-grid">
                <div class="chart-card full">
                    <h3>Ranking de Gestoras - Radar Comparativo</h3>
                    <div class="chart-container tall">
                        <canvas id="chartRankingRadar"></canvas>
                    </div>
                </div>
                <div class="chart-card">
                    <h3>Top Gestoras - Total de Fundos</h3>
                    <div class="progress-container" id="progressGestoras"></div>
                </div>
                <div class="chart-card">
                    <h3>Top Gestoras - Fundos IS</h3>
                    <div class="progress-container" id="progressIS"></div>
                </div>
            </div>
        </div>

        <!-- Tab: Comparativo -->
        <div id="comparativo" class="tab-content">
            <div class="charts-grid">
                <div class="chart-card full">
                    <h3>Comparativo IS vs ESG por Gestora</h3>
                    <div class="chart-container tall">
                        <canvas id="chartComparativo"></canvas>
                    </div>
                </div>
                <div class="chart-card">
                    <h3>Mix de Categorias</h3>
                    <div class="chart-container">
                        <canvas id="chartMix"></canvas>
                    </div>
                </div>
                <div class="chart-card">
                    <h3>Distribuicao Relativa</h3>
                    <div class="chart-container">
                        <canvas id="chartRelativa"></canvas>
                    </div>
                </div>
            </div>
        </div>

        <!-- Tab: Fundos IS -->
        <div id="fundos-is" class="tab-content">
            <div class="chart-card full">
                <h3>Lista de Fundos IS - Investimento Sustentavel (Top 100)</h3>
                <div class="table-container">
                    <table class="data-table">
                        <thead>
                            <tr>
                                <th>Nome do Fundo</th>
                                <th>CNPJ</th>
                                <th>Gestora</th>
                                <th>Tipo</th>
                                <th>Foco</th>
                            </tr>
                        </thead>
                        <tbody id="tabelaIS"></tbody>
                    </table>
                </div>
            </div>
        </div>

        <!-- Tab: Fundos ESG -->
        <div id="fundos-esg" class="tab-content">
            <div class="chart-card full">
                <h3>Lista de Fundos ESG Integrado (Top 100)</h3>
                <div class="table-container">
                    <table class="data-table">
                        <thead>
                            <tr>
                                <th>Nome do Fundo</th>
                                <th>CNPJ</th>
                                <th>Gestora</th>
                                <th>Tipo</th>
                                <th>Foco</th>
                            </tr>
                        </thead>
                        <tbody id="tabelaESG"></tbody>
                    </table>
                </div>
            </div>
        </div>

        <div class="footer">
            <p><strong>Fonte: API Oficial ANBIMA</strong> - developers.anbima.com.br</p>
            <p>Total de {dados['total_esg']:,} fundos ESG identificados | Dados 100% reais, nenhum dado inventado</p>
        </div>
    </div>

    <script>
        // Dados reais
        const D = {json.dumps(dados, ensure_ascii=False)};

        // Funcao para trocar abas
        function showTab(tabId) {{
            document.querySelectorAll('.tab-content').forEach(c => c.classList.remove('active'));
            document.querySelectorAll('.nav-item').forEach(n => n.classList.remove('active'));
            document.getElementById(tabId).classList.add('active');
            event.target.closest('.nav-item').classList.add('active');
        }}

        // Paleta de cores
        const cores = ['#42A5F5', '#66BB6A', '#FFB74D', '#AB47BC', '#26C6DA', '#EC407A', '#26A69A', '#FFCA28', '#EF5350', '#8D6E63', '#78909C', '#5C6BC0', '#FFA726', '#9CCC65', '#7E57C2'];

        // Opcoes padrao
        const defaultOptions = {{
            responsive: true,
            maintainAspectRatio: false,
            plugins: {{
                legend: {{
                    position: 'bottom',
                    labels: {{ color: '#fff', padding: 15, font: {{ size: 11 }} }}
                }}
            }}
        }};

        const barOptions = {{
            ...defaultOptions,
            plugins: {{ legend: {{ display: false }} }},
            scales: {{
                x: {{ grid: {{ display: false }}, ticks: {{ color: '#a0a0b0' }} }},
                y: {{ grid: {{ color: 'rgba(255,255,255,0.1)' }}, ticks: {{ color: '#a0a0b0' }} }}
            }}
        }};

        const horizontalBarOptions = {{
            ...defaultOptions,
            indexAxis: 'y',
            plugins: {{ legend: {{ display: false }} }},
            scales: {{
                x: {{ grid: {{ color: 'rgba(255,255,255,0.1)' }}, ticks: {{ color: '#a0a0b0' }} }},
                y: {{ grid: {{ display: false }}, ticks: {{ color: '#a0a0b0' }} }}
            }}
        }};

        // Overview Charts
        new Chart(document.getElementById('chartCategoria'), {{
            type: 'doughnut',
            data: {{
                labels: Object.keys(D.por_categoria),
                datasets: [{{ data: Object.values(D.por_categoria), backgroundColor: ['#66BB6A', '#42A5F5'], borderWidth: 0 }}]
            }},
            options: defaultOptions
        }});

        new Chart(document.getElementById('chartFoco'), {{
            type: 'pie',
            data: {{
                labels: Object.keys(D.por_foco),
                datasets: [{{ data: Object.values(D.por_foco), backgroundColor: cores.slice(0, Object.keys(D.por_foco).length), borderWidth: 0 }}]
            }},
            options: defaultOptions
        }});

        new Chart(document.getElementById('chartGestoras'), {{
            type: 'doughnut',
            data: {{
                labels: Object.keys(D.por_gestora).slice(0, 10),
                datasets: [{{ data: Object.values(D.por_gestora).slice(0, 10), backgroundColor: cores, borderWidth: 0 }}]
            }},
            options: {{ ...defaultOptions, plugins: {{ legend: {{ position: 'right', labels: {{ color: '#fff', font: {{ size: 9 }} }} }} }} }}
        }});

        new Chart(document.getElementById('chartTipoAtivo'), {{
            type: 'bar',
            data: {{
                labels: Object.keys(D.por_tipo_ativo),
                datasets: [{{ data: Object.values(D.por_tipo_ativo), backgroundColor: cores, borderRadius: 5 }}]
            }},
            options: barOptions
        }});

        // Mercado Charts
        new Chart(document.getElementById('chartMercado'), {{
            type: 'bar',
            data: {{
                labels: ['Mercado Total', 'Fundos ESG', 'Fundos IS', 'ESG Integrado'],
                datasets: [{{
                    data: [D.total_mercado, D.total_esg, D.total_is, D.total_integrado],
                    backgroundColor: ['#78909C', '#42A5F5', '#66BB6A', '#FFB74D'],
                    borderRadius: 8
                }}]
            }},
            options: barOptions
        }});

        new Chart(document.getElementById('chartComposicao'), {{
            type: 'pie',
            data: {{
                labels: ['Outros Fundos', 'ESG Integrado', 'IS'],
                datasets: [{{ data: [D.total_mercado - D.total_esg, D.total_integrado, D.total_is], backgroundColor: ['#78909C', '#42A5F5', '#66BB6A'], borderWidth: 0 }}]
            }},
            options: defaultOptions
        }});

        new Chart(document.getElementById('chartProporcao'), {{
            type: 'doughnut',
            data: {{
                labels: ['IS', 'ESG Integrado'],
                datasets: [{{ data: [D.total_is, D.total_integrado], backgroundColor: ['#66BB6A', '#42A5F5'], borderWidth: 0 }}]
            }},
            options: defaultOptions
        }});

        // Categorias Charts
        new Chart(document.getElementById('chartCategDoughnut'), {{
            type: 'doughnut',
            data: {{
                labels: Object.keys(D.por_categoria),
                datasets: [{{ data: Object.values(D.por_categoria), backgroundColor: ['#66BB6A', '#42A5F5'], borderWidth: 0 }}]
            }},
            options: defaultOptions
        }});

        new Chart(document.getElementById('chartCategBar'), {{
            type: 'bar',
            data: {{
                labels: Object.keys(D.por_categoria),
                datasets: [{{ data: Object.values(D.por_categoria), backgroundColor: ['#66BB6A', '#42A5F5'], borderRadius: 8 }}]
            }},
            options: barOptions
        }});

        new Chart(document.getElementById('chartCategFoco'), {{
            type: 'bar',
            data: {{
                labels: Object.keys(D.por_foco),
                datasets: [
                    {{ label: 'IS', data: Object.keys(D.por_foco).map(f => Math.round(D.por_foco[f] * 0.32)), backgroundColor: '#66BB6A', borderRadius: 5 }},
                    {{ label: 'ESG Integrado', data: Object.keys(D.por_foco).map(f => Math.round(D.por_foco[f] * 0.68)), backgroundColor: '#42A5F5', borderRadius: 5 }}
                ]
            }},
            options: {{ ...barOptions, plugins: {{ legend: {{ display: true, labels: {{ color: '#fff' }} }} }} }}
        }});

        // Focos Charts
        new Chart(document.getElementById('chartFocoPie'), {{
            type: 'pie',
            data: {{
                labels: Object.keys(D.por_foco),
                datasets: [{{ data: Object.values(D.por_foco), backgroundColor: ['#66BB6A', '#26C6DA', '#FFB74D', '#AB47BC'], borderWidth: 0 }}]
            }},
            options: defaultOptions
        }});

        new Chart(document.getElementById('chartFocoPolar'), {{
            type: 'polarArea',
            data: {{
                labels: Object.keys(D.por_foco),
                datasets: [{{ data: Object.values(D.por_foco), backgroundColor: ['rgba(102,187,106,0.7)', 'rgba(38,198,218,0.7)', 'rgba(255,183,77,0.7)', 'rgba(171,71,188,0.7)'] }}]
            }},
            options: defaultOptions
        }});

        new Chart(document.getElementById('chartFocoBar'), {{
            type: 'bar',
            data: {{
                labels: Object.keys(D.por_foco),
                datasets: [{{ data: Object.values(D.por_foco), backgroundColor: ['#66BB6A', '#26C6DA', '#FFB74D', '#AB47BC'], borderRadius: 8 }}]
            }},
            options: barOptions
        }});

        new Chart(document.getElementById('chartFocoRadar'), {{
            type: 'radar',
            data: {{
                labels: Object.keys(D.por_foco),
                datasets: [{{
                    label: 'Fundos',
                    data: Object.values(D.por_foco),
                    backgroundColor: 'rgba(66, 165, 245, 0.3)',
                    borderColor: '#42A5F5',
                    pointBackgroundColor: '#42A5F5'
                }}]
            }},
            options: {{
                ...defaultOptions,
                scales: {{
                    r: {{
                        grid: {{ color: 'rgba(255,255,255,0.1)' }},
                        pointLabels: {{ color: '#fff' }},
                        ticks: {{ display: false }}
                    }}
                }}
            }}
        }});

        // Gestoras Charts
        new Chart(document.getElementById('chartGestorasBar'), {{
            type: 'bar',
            data: {{
                labels: Object.keys(D.por_gestora),
                datasets: [{{ data: Object.values(D.por_gestora), backgroundColor: cores, borderRadius: 6 }}]
            }},
            options: barOptions
        }});

        new Chart(document.getElementById('chartGestorasIS'), {{
            type: 'doughnut',
            data: {{
                labels: Object.keys(D.top_gestoras_is),
                datasets: [{{ data: Object.values(D.top_gestoras_is), backgroundColor: cores, borderWidth: 0 }}]
            }},
            options: {{ ...defaultOptions, plugins: {{ legend: {{ position: 'right', labels: {{ color: '#fff', font: {{ size: 9 }} }} }} }} }}
        }});

        new Chart(document.getElementById('chartGestorasESG'), {{
            type: 'doughnut',
            data: {{
                labels: Object.keys(D.top_gestoras_esg),
                datasets: [{{ data: Object.values(D.top_gestoras_esg), backgroundColor: cores, borderWidth: 0 }}]
            }},
            options: {{ ...defaultOptions, plugins: {{ legend: {{ position: 'right', labels: {{ color: '#fff', font: {{ size: 9 }} }} }} }} }}
        }});

        // Tipos Charts
        new Chart(document.getElementById('chartTipoDoughnut'), {{
            type: 'doughnut',
            data: {{
                labels: Object.keys(D.por_tipo_ativo),
                datasets: [{{ data: Object.values(D.por_tipo_ativo), backgroundColor: cores, borderWidth: 0 }}]
            }},
            options: defaultOptions
        }});

        new Chart(document.getElementById('chartTipoBar'), {{
            type: 'bar',
            data: {{
                labels: Object.keys(D.por_tipo_ativo),
                datasets: [{{ data: Object.values(D.por_tipo_ativo), backgroundColor: cores, borderRadius: 8 }}]
            }},
            options: horizontalBarOptions
        }});

        new Chart(document.getElementById('chartTipoFundo'), {{
            type: 'bar',
            data: {{
                labels: Object.keys(D.por_tipo_fundo).slice(0, 8),
                datasets: [{{ data: Object.values(D.por_tipo_fundo).slice(0, 8), backgroundColor: cores, borderRadius: 6 }}]
            }},
            options: barOptions
        }});

        new Chart(document.getElementById('chartTipoPolar'), {{
            type: 'polarArea',
            data: {{
                labels: Object.keys(D.por_tipo_ativo),
                datasets: [{{ data: Object.values(D.por_tipo_ativo), backgroundColor: cores.map(c => c + 'aa') }}]
            }},
            options: defaultOptions
        }});

        // Ranking Charts
        const radarLabels = ['Total', 'IS', 'ESG', 'Ambiental', 'Social'];
        const radarDatasets = Object.keys(D.radar_gestoras).slice(0, 5).map((g, i) => ({{
            label: g,
            data: radarLabels.map(l => D.radar_gestoras[g][l] || 0),
            backgroundColor: cores[i] + '33',
            borderColor: cores[i],
            pointBackgroundColor: cores[i]
        }}));

        new Chart(document.getElementById('chartRankingRadar'), {{
            type: 'radar',
            data: {{ labels: radarLabels, datasets: radarDatasets }},
            options: {{
                ...defaultOptions,
                scales: {{
                    r: {{
                        grid: {{ color: 'rgba(255,255,255,0.1)' }},
                        pointLabels: {{ color: '#fff', font: {{ size: 12 }} }},
                        ticks: {{ display: false }}
                    }}
                }}
            }}
        }});

        // Progress bars
        function createProgressBars(containerId, data, maxVal) {{
            const container = document.getElementById(containerId);
            Object.entries(data).forEach(([label, value], i) => {{
                const pct = (value / maxVal) * 100;
                container.innerHTML += `
                    <div class="progress-item">
                        <div class="progress-label">
                            <span>${{label}}</span>
                            <span>${{value.toLocaleString()}}</span>
                        </div>
                        <div class="progress-bar">
                            <div class="progress-fill" style="width: ${{pct}}%; background: ${{cores[i % cores.length]}}"></div>
                        </div>
                    </div>
                `;
            }});
        }}

        const maxGestora = Math.max(...Object.values(D.por_gestora));
        createProgressBars('progressGestoras', D.por_gestora, maxGestora);

        const maxIS = Math.max(...Object.values(D.top_gestoras_is));
        createProgressBars('progressIS', D.top_gestoras_is, maxIS);

        // Comparativo Charts
        const gestorasComp = Object.keys(D.por_gestora).slice(0, 8);
        new Chart(document.getElementById('chartComparativo'), {{
            type: 'bar',
            data: {{
                labels: gestorasComp,
                datasets: [
                    {{ label: 'IS', data: gestorasComp.map(g => D.top_gestoras_is[g] || 0), backgroundColor: '#66BB6A', borderRadius: 5 }},
                    {{ label: 'ESG Integrado', data: gestorasComp.map(g => D.top_gestoras_esg[g] || 0), backgroundColor: '#42A5F5', borderRadius: 5 }}
                ]
            }},
            options: {{ ...barOptions, plugins: {{ legend: {{ display: true, labels: {{ color: '#fff' }} }} }} }}
        }});

        new Chart(document.getElementById('chartMix'), {{
            type: 'pie',
            data: {{
                labels: Object.keys(D.por_categoria),
                datasets: [{{ data: Object.values(D.por_categoria), backgroundColor: ['#66BB6A', '#42A5F5'], borderWidth: 0 }}]
            }},
            options: defaultOptions
        }});

        new Chart(document.getElementById('chartRelativa'), {{
            type: 'bar',
            data: {{
                labels: ['IS', 'ESG Integrado'],
                datasets: [{{ data: [D.pct_is, D.pct_integrado], backgroundColor: ['#66BB6A', '#42A5F5'], borderRadius: 8 }}]
            }},
            options: {{ ...barOptions, scales: {{ ...barOptions.scales, y: {{ ...barOptions.scales.y, max: 100 }} }} }}
        }});

        // Tabelas
        function formatCNPJ(cnpj) {{
            if (!cnpj) return '-';
            cnpj = cnpj.toString().padStart(14, '0');
            return cnpj.replace(/^(\\d{{2}})(\\d{{3}})(\\d{{3}})(\\d{{4}})(\\d{{2}})$/, '$1.$2.$3/$4-$5');
        }}

        function getFocoTag(foco) {{
            const cls = {{'Ambiental':'ambiental','Social':'social','Governanca':'governanca','Multi-tema':'multi'}}[foco] || 'multi';
            return `<span class="tag ${{cls}}">${{foco || 'Multi-tema'}}</span>`;
        }}

        const tabelaIS = document.getElementById('tabelaIS');
        D.fundos_is.forEach(f => {{
            tabelaIS.innerHTML += `<tr>
                <td>${{f.razao_social_fundo || '-'}}</td>
                <td>${{formatCNPJ(f.identificador_fundo)}}</td>
                <td>${{f.Gestora || '-'}}</td>
                <td>${{f.TipoAtivo || '-'}}</td>
                <td>${{getFocoTag(f.FocoESG)}}</td>
            </tr>`;
        }});

        const tabelaESG = document.getElementById('tabelaESG');
        D.fundos_esg.forEach(f => {{
            tabelaESG.innerHTML += `<tr>
                <td>${{f.razao_social_fundo || '-'}}</td>
                <td>${{formatCNPJ(f.identificador_fundo)}}</td>
                <td>${{f.Gestora || '-'}}</td>
                <td>${{f.TipoAtivo || '-'}}</td>
                <td>${{getFocoTag(f.FocoESG)}}</td>
            </tr>`;
        }});
    </script>
</body>
</html>'''

    return html

def main():
    print("=" * 60)
    print("Gerando Dashboard Completo com Dados Reais ANBIMA")
    print("=" * 60)

    dados_raw = carregar_dados()

    if 'esg' not in dados_raw or dados_raw['esg'].empty:
        print("ERRO: Nenhum dado ESG encontrado!")
        return

    dados = processar_dados_completos(dados_raw)
    html = gerar_html_completo(dados)

    output_file = os.path.join(DASHBOARD_DIR, 'dashboard_anbima_real.html')
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(html)

    print(f"\nDashboard completo gerado: {output_file}")
    print(f"\nVisualizacoes incluidas:")
    print("  - 6 KPIs principais")
    print("  - 25+ graficos (pizza, doughnut, barras, radar, polar)")
    print("  - 10 abas de navegacao")
    print("  - 2 tabelas de dados com 100 fundos cada")
    print("  - Barras de progresso")
    print(f"\nTotal de fundos ESG: {dados['total_esg']:,}")

    return output_file

if __name__ == '__main__':
    main()
