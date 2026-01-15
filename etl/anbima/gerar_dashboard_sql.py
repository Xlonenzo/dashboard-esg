"""
Gerador de Dashboard com Dados do SQL Server + TSB
==================================================
Este script le os dados do banco SQL Server ANBIMA_ESG
e dados TSB do JSON, gerando um dashboard HTML completo.
"""

import pandas as pd
import pyodbc
import os
import json
from datetime import datetime

# Diretorios
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DATA_DIR = os.path.join(BASE_DIR, 'data', 'anbima')
DASHBOARD_DIR = os.path.join(BASE_DIR, 'dashboard')


class DashboardSQL:
    """Classe para gerar dashboard a partir do SQL Server"""

    def __init__(self, server: str = 'localhost', database: str = 'ANBIMA_ESG'):
        self.server = server
        self.database = database
        self.connection_string = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={server};"
            f"DATABASE={database};"
            f"Trusted_Connection=yes;"
        )
        self.conn = None

    def conectar(self) -> bool:
        try:
            self.conn = pyodbc.connect(self.connection_string)
            print(f"Conectado ao SQL Server: {self.server}/{self.database}")
            return True
        except Exception as e:
            print(f"Erro ao conectar: {e}")
            return False

    def fechar(self):
        if self.conn:
            self.conn.close()

    def obter_totais(self) -> dict:
        sql = """
        SELECT
            COUNT(*) as total_fundos,
            SUM(CASE WHEN SufixoIS = 1 THEN 1 ELSE 0 END) as total_is,
            SUM(CASE WHEN ESGIntegrado = 1 AND SufixoIS = 0 THEN 1 ELSE 0 END) as total_integrado
        FROM fundos.FatoFundo WHERE Ativo = 1
        """
        df = pd.read_sql(sql, self.conn)
        return {
            'total_esg': int(df['total_fundos'].iloc[0] or 0),
            'total_is': int(df['total_is'].iloc[0] or 0),
            'total_integrado': int(df['total_integrado'].iloc[0] or 0)
        }

    def obter_por_categoria(self) -> dict:
        sql = """
        SELECT c.CategoriaNome as categoria, COUNT(f.FundoID) as total
        FROM fundos.FatoFundo f
        INNER JOIN esg.DimCategoriaESG c ON f.CategoriaESGID = c.CategoriaESGID
        WHERE f.Ativo = 1 GROUP BY c.CategoriaNome ORDER BY total DESC
        """
        df = pd.read_sql(sql, self.conn)
        return dict(zip(df['categoria'], df['total']))

    def obter_por_foco(self) -> dict:
        sql = """
        SELECT ISNULL(fo.FocoNome, 'Multi-tema') as foco, COUNT(f.FundoID) as total
        FROM fundos.FatoFundo f
        LEFT JOIN esg.DimFocoESG fo ON f.FocoESGID = fo.FocoESGID
        WHERE f.Ativo = 1 GROUP BY fo.FocoNome ORDER BY total DESC
        """
        df = pd.read_sql(sql, self.conn)
        return dict(zip(df['foco'], df['total']))

    def obter_por_gestora(self) -> dict:
        sql = """
        SELECT TOP 10 ISNULL(g.GestoraNome, 'Outros') as gestora, COUNT(f.FundoID) as total
        FROM fundos.FatoFundo f
        LEFT JOIN fundos.DimGestora g ON f.GestoraID = g.GestoraID
        WHERE f.Ativo = 1 GROUP BY g.GestoraNome ORDER BY total DESC
        """
        df = pd.read_sql(sql, self.conn)
        return dict(zip(df['gestora'], df['total']))

    def obter_fundos_is(self, limit: int = 50) -> list:
        sql = f"""
        SELECT TOP {limit} f.FundoNome as razao_social_fundo, f.FundoCNPJ as identificador_fundo,
            'FIF' as tipo_fundo, ISNULL(fo.FocoNome, 'Multi-tema') as FocoESG
        FROM fundos.FatoFundo f
        LEFT JOIN esg.DimFocoESG fo ON f.FocoESGID = fo.FocoESGID
        WHERE f.Ativo = 1 AND f.SufixoIS = 1 ORDER BY f.FundoNome
        """
        df = pd.read_sql(sql, self.conn)
        return df.to_dict('records')

    def obter_fundos_esg(self, limit: int = 50) -> list:
        sql = f"""
        SELECT TOP {limit} f.FundoNome as razao_social_fundo, f.FundoCNPJ as identificador_fundo,
            'FIF' as tipo_fundo, ISNULL(fo.FocoNome, 'Multi-tema') as FocoESG
        FROM fundos.FatoFundo f
        LEFT JOIN esg.DimFocoESG fo ON f.FocoESGID = fo.FocoESGID
        WHERE f.Ativo = 1 AND f.ESGIntegrado = 1 AND f.SufixoIS = 0 ORDER BY f.FundoNome
        """
        df = pd.read_sql(sql, self.conn)
        return df.to_dict('records')

    def obter_dados_completos(self) -> dict:
        totais = self.obter_totais()
        return {
            'total_esg': totais['total_esg'],
            'total_is': totais['total_is'],
            'total_integrado': totais['total_integrado'],
            'por_categoria': self.obter_por_categoria(),
            'por_foco': self.obter_por_foco(),
            'por_gestora': self.obter_por_gestora(),
            'fundos_is': self.obter_fundos_is(),
            'fundos_esg': self.obter_fundos_esg(),
            'data_atualizacao': datetime.now().strftime('%d/%m/%Y %H:%M'),
            'fonte': 'SQL Server'
        }


def carregar_dados_tsb() -> dict:
    """Carrega dados TSB do arquivo JSON"""
    arquivo_tsb = os.path.join(DATA_DIR, 'tsb_kpis_empresas.json')
    if os.path.exists(arquivo_tsb):
        with open(arquivo_tsb, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {}


def gerar_dashboard_html(dados: dict, dados_tsb: dict) -> str:
    """Gera o HTML do dashboard com dados do SQL Server e TSB"""

    # Processar dados TSB
    tsb_empresas = dados_tsb.get('empresas', [])
    tsb_resumo = dados_tsb.get('resumo_dashboard', {})
    tsb_kpis = dados_tsb.get('kpis_obrigatorios_por_setor', {})

    html = f'''<!DOCTYPE html>
<html lang="pt-BR">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Dashboard Sustentabilidade - ANBIMA ESG + TSB</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap" rel="stylesheet">
    <style>
        :root {{
            --primary: #1976D2;
            --primary-light: #42A5F5;
            --secondary: #2E7D32;
            --secondary-light: #66BB6A;
            --accent: #FF9800;
            --verde-tsb: #00C853;
            --transicao-tsb: #FFD600;
            --bg-dark: #0a0a1a;
            --bg-card: #12122a;
            --bg-card-hover: #1a1a3a;
            --text-primary: #ffffff;
            --text-secondary: #a0a0b0;
            --border-color: #2a2a4a;
        }}
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        body {{
            font-family: 'Inter', sans-serif;
            background: var(--bg-dark);
            color: var(--text-primary);
            min-height: 100vh;
            padding: 20px;
        }}
        .header {{
            text-align: center;
            padding: 30px;
            margin-bottom: 30px;
            background: linear-gradient(135deg, rgba(25, 118, 210, 0.2) 0%, rgba(46, 125, 50, 0.2) 100%);
            border-radius: 20px;
            border: 1px solid var(--border-color);
        }}
        .header h1 {{
            font-size: 2.5rem;
            background: linear-gradient(135deg, var(--primary) 0%, var(--secondary) 100%);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            margin-bottom: 10px;
        }}
        .header .subtitle {{ color: var(--text-secondary); font-size: 1rem; }}
        .header .badge {{
            display: inline-block;
            background: #2196F3;
            color: white;
            padding: 5px 15px;
            border-radius: 20px;
            font-size: 0.8rem;
            margin-top: 15px;
            margin-right: 10px;
        }}
        .header .badge.tsb {{ background: var(--verde-tsb); }}
        .stats-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 20px;
            margin-bottom: 30px;
        }}
        .stat-card {{
            background: var(--bg-card);
            border-radius: 15px;
            padding: 20px;
            border: 1px solid var(--border-color);
            transition: transform 0.3s, box-shadow 0.3s;
        }}
        .stat-card:hover {{
            transform: translateY(-5px);
            box-shadow: 0 10px 30px rgba(0,0,0,0.3);
        }}
        .stat-card .label {{ color: var(--text-secondary); font-size: 0.8rem; margin-bottom: 8px; }}
        .stat-card .value {{ font-size: 2rem; font-weight: 700; }}
        .stat-card .value.primary {{ color: var(--primary-light); }}
        .stat-card .value.secondary {{ color: var(--secondary-light); }}
        .stat-card .value.accent {{ color: var(--accent); }}
        .stat-card .value.verde {{ color: var(--verde-tsb); }}
        .charts-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(350px, 1fr));
            gap: 25px;
            margin-bottom: 30px;
        }}
        .chart-card {{
            background: var(--bg-card);
            border-radius: 15px;
            padding: 25px;
            border: 1px solid var(--border-color);
        }}
        .chart-card h3 {{
            font-size: 1rem;
            margin-bottom: 20px;
            color: var(--text-primary);
            display: flex;
            align-items: center;
            gap: 10px;
        }}
        .chart-card h3::before {{
            content: '';
            width: 4px;
            height: 20px;
            background: linear-gradient(180deg, var(--primary) 0%, var(--secondary) 100%);
            border-radius: 2px;
        }}
        .chart-card h3.tsb::before {{ background: linear-gradient(180deg, var(--verde-tsb) 0%, var(--secondary) 100%); }}
        .chart-container {{ position: relative; height: 280px; }}
        .funds-table {{
            width: 100%;
            border-collapse: collapse;
            margin-top: 15px;
        }}
        .funds-table th, .funds-table td {{
            padding: 10px;
            text-align: left;
            border-bottom: 1px solid var(--border-color);
        }}
        .funds-table th {{ color: var(--text-secondary); font-weight: 500; font-size: 0.75rem; text-transform: uppercase; }}
        .funds-table td {{ font-size: 0.85rem; }}
        .funds-table tr:hover {{ background: var(--bg-card-hover); }}
        .tag {{
            display: inline-block;
            padding: 3px 10px;
            border-radius: 12px;
            font-size: 0.7rem;
            font-weight: 500;
        }}
        .tag.ambiental {{ background: rgba(76, 175, 80, 0.2); color: #81C784; }}
        .tag.social {{ background: rgba(255, 152, 0, 0.2); color: #FFB74D; }}
        .tag.governanca {{ background: rgba(156, 39, 176, 0.2); color: #BA68C8; }}
        .tag.multi {{ background: rgba(0, 188, 212, 0.2); color: #4DD0E1; }}
        .tag.verde {{ background: rgba(0, 200, 83, 0.2); color: #69F0AE; }}
        .tag.transicao {{ background: rgba(255, 214, 0, 0.2); color: #FFEA00; }}
        .tag.energia {{ background: rgba(255, 152, 0, 0.2); color: #FFB74D; }}
        .tag.saneamento {{ background: rgba(33, 150, 243, 0.2); color: #64B5F6; }}
        .tag.telecom {{ background: rgba(156, 39, 176, 0.2); color: #CE93D8; }}
        .tag.financeiro {{ background: rgba(0, 150, 136, 0.2); color: #80CBC4; }}
        .fonte-info {{
            text-align: center;
            padding: 20px;
            color: var(--text-secondary);
            font-size: 0.85rem;
            border-top: 1px solid var(--border-color);
            margin-top: 30px;
        }}
        .fonte-info strong {{ color: #2196F3; }}
        .tabs {{
            display: flex;
            gap: 10px;
            margin-bottom: 25px;
            flex-wrap: wrap;
        }}
        .tab {{
            padding: 12px 20px;
            background: var(--bg-card);
            border: 1px solid var(--border-color);
            border-radius: 10px;
            cursor: pointer;
            transition: all 0.3s;
            color: var(--text-secondary);
            font-size: 0.9rem;
        }}
        .tab:hover {{ background: var(--bg-card-hover); }}
        .tab.active {{
            background: linear-gradient(135deg, rgba(25, 118, 210, 0.3) 0%, rgba(46, 125, 50, 0.3) 100%);
            border-color: var(--primary);
            color: var(--text-primary);
        }}
        .tab.tsb.active {{
            background: linear-gradient(135deg, rgba(0, 200, 83, 0.3) 0%, rgba(46, 125, 50, 0.3) 100%);
            border-color: var(--verde-tsb);
        }}
        .tab-content {{ display: none; }}
        .tab-content.active {{ display: block; }}
        .full-width {{ grid-column: 1 / -1; }}
        .kpi-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
            gap: 10px;
            margin-top: 15px;
        }}
        .kpi-item {{
            background: var(--bg-card-hover);
            border-radius: 8px;
            padding: 12px;
            text-align: center;
        }}
        .kpi-item .kpi-codigo {{ font-size: 0.7rem; color: var(--text-secondary); }}
        .kpi-item .kpi-nome {{ font-size: 0.75rem; margin: 5px 0; }}
        .kpi-item .kpi-valor {{ font-size: 1.2rem; font-weight: 600; color: var(--accent); }}
        .kpi-item .kpi-unidade {{ font-size: 0.65rem; color: var(--text-secondary); }}
        .progress-bar {{
            height: 8px;
            background: var(--border-color);
            border-radius: 4px;
            overflow: hidden;
            margin-top: 10px;
        }}
        .progress-bar .fill {{
            height: 100%;
            border-radius: 4px;
            transition: width 0.5s;
        }}
        .progress-bar .fill.verde {{ background: var(--verde-tsb); }}
        .progress-bar .fill.amarelo {{ background: var(--transicao-tsb); }}
        .section-title {{
            font-size: 1.2rem;
            margin: 30px 0 20px;
            color: var(--text-primary);
            border-left: 4px solid var(--verde-tsb);
            padding-left: 15px;
        }}
    </style>
</head>
<body>
    <div class="header">
        <h1>Dashboard Sustentabilidade</h1>
        <p class="subtitle">Fundos ESG ANBIMA + Taxonomia Sustentavel Brasileira (TSB)</p>
        <span class="badge">SQL SERVER</span>
        <span class="badge tsb">TSB - Fase Voluntaria</span>
    </div>

    <div class="stats-grid">
        <div class="stat-card">
            <div class="label">Fundos ESG</div>
            <div class="value primary">{dados['total_esg']:,}</div>
        </div>
        <div class="stat-card">
            <div class="label">Fundos IS</div>
            <div class="value secondary">{dados['total_is']:,}</div>
        </div>
        <div class="stat-card">
            <div class="label">ESG Integrado</div>
            <div class="value accent">{dados['total_integrado']:,}</div>
        </div>
        <div class="stat-card">
            <div class="label">Empresas TSB</div>
            <div class="value verde">{tsb_resumo.get('total_empresas', len(tsb_empresas))}</div>
        </div>
        <div class="stat-card">
            <div class="label">KPIs Pendentes</div>
            <div class="value accent">{tsb_resumo.get('kpis_pendentes', 0)}</div>
        </div>
    </div>

    <div class="tabs">
        <div class="tab active" onclick="showTab('overview')">Visao Geral</div>
        <div class="tab" onclick="showTab('fundos-is')">Fundos IS</div>
        <div class="tab" onclick="showTab('fundos-esg')">ESG Integrado</div>
        <div class="tab tsb" onclick="showTab('tsb-empresas')">TSB - Empresas</div>
        <div class="tab tsb" onclick="showTab('tsb-kpis')">TSB - KPIs</div>
    </div>

    <div id="overview" class="tab-content active">
        <div class="charts-grid">
            <div class="chart-card">
                <h3>Categoria ESG</h3>
                <div class="chart-container">
                    <canvas id="chartCategoria"></canvas>
                </div>
            </div>
            <div class="chart-card">
                <h3>Foco ESG</h3>
                <div class="chart-container">
                    <canvas id="chartFoco"></canvas>
                </div>
            </div>
            <div class="chart-card">
                <h3 class="tsb">Empresas TSB por Setor</h3>
                <div class="chart-container">
                    <canvas id="chartTSBSetor"></canvas>
                </div>
            </div>
            <div class="chart-card">
                <h3 class="tsb">Classificacao TSB</h3>
                <div class="chart-container">
                    <canvas id="chartTSBClass"></canvas>
                </div>
            </div>
        </div>
    </div>

    <div id="fundos-is" class="tab-content">
        <div class="chart-card full-width">
            <h3>Fundos IS - Investimento Sustentavel</h3>
            <table class="funds-table">
                <thead>
                    <tr><th>Nome do Fundo</th><th>CNPJ</th><th>Tipo</th><th>Foco</th></tr>
                </thead>
                <tbody id="tabelaFundosIS"></tbody>
            </table>
        </div>
    </div>

    <div id="fundos-esg" class="tab-content">
        <div class="chart-card full-width">
            <h3>Fundos ESG Integrado</h3>
            <table class="funds-table">
                <thead>
                    <tr><th>Nome do Fundo</th><th>CNPJ</th><th>Tipo</th><th>Foco</th></tr>
                </thead>
                <tbody id="tabelaFundosESG"></tbody>
            </table>
        </div>
    </div>

    <div id="tsb-empresas" class="tab-content">
        <div class="chart-card full-width">
            <h3 class="tsb">Empresas Classificadas pela TSB</h3>
            <p style="color: var(--text-secondary); margin-bottom: 15px; font-size: 0.85rem;">
                Taxonomia Sustentavel Brasileira - Ministerio da Fazenda | Fase Voluntaria (2024-2027)
            </p>
            <table class="funds-table">
                <thead>
                    <tr>
                        <th>Empresa</th>
                        <th>CNPJ</th>
                        <th>Setor TSB</th>
                        <th>Classificacao</th>
                        <th>Score</th>
                        <th>Titulos</th>
                    </tr>
                </thead>
                <tbody id="tabelaTSBEmpresas"></tbody>
            </table>
        </div>
    </div>

    <div id="tsb-kpis" class="tab-content">
        <div class="charts-grid">
            <div class="chart-card full-width">
                <h3 class="tsb">KPIs Obrigatorios por Setor - Energia</h3>
                <div class="kpi-grid" id="kpisEnergia"></div>
            </div>
            <div class="chart-card full-width">
                <h3 class="tsb">KPIs Obrigatorios por Setor - Saneamento</h3>
                <div class="kpi-grid" id="kpisSaneamento"></div>
            </div>
            <div class="chart-card full-width">
                <h3 class="tsb">KPIs Obrigatorios por Setor - Telecomunicacoes</h3>
                <div class="kpi-grid" id="kpisTelecom"></div>
            </div>
            <div class="chart-card full-width">
                <h3 class="tsb">KPIs Obrigatorios por Setor - Servicos Financeiros</h3>
                <div class="kpi-grid" id="kpisFinanceiro"></div>
            </div>
        </div>
    </div>

    <div class="fonte-info">
        <p><strong>Fontes:</strong> SQL Server (ANBIMA_ESG) | TSB - Ministerio da Fazenda</p>
        <p>Atualizado em {dados['data_atualizacao']}</p>
    </div>

    <script>
        const dadosSQL = {json.dumps(dados, ensure_ascii=False)};
        const dadosTSB = {json.dumps(dados_tsb, ensure_ascii=False)};

        function showTab(tabId) {{
            document.querySelectorAll('.tab-content').forEach(c => c.classList.remove('active'));
            document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
            document.getElementById(tabId).classList.add('active');
            event.target.classList.add('active');
        }}

        const cores = {{
            primary: '#42A5F5', secondary: '#66BB6A', accent: '#FFB74D',
            purple: '#AB47BC', info: '#26C6DA', pink: '#EC407A',
            teal: '#26A69A', lime: '#9CCC65', amber: '#FFCA28', red: '#ef5350',
            verde: '#69F0AE', transicao: '#FFEA00'
        }};

        // Graficos ESG
        new Chart(document.getElementById('chartCategoria'), {{
            type: 'doughnut',
            data: {{
                labels: Object.keys(dadosSQL.por_categoria),
                datasets: [{{ data: Object.values(dadosSQL.por_categoria), backgroundColor: [cores.secondary, cores.primary], borderWidth: 0 }}]
            }},
            options: {{ responsive: true, maintainAspectRatio: false, plugins: {{ legend: {{ position: 'bottom', labels: {{ color: '#fff' }} }} }} }}
        }});

        new Chart(document.getElementById('chartFoco'), {{
            type: 'pie',
            data: {{
                labels: Object.keys(dadosSQL.por_foco),
                datasets: [{{ data: Object.values(dadosSQL.por_foco), backgroundColor: [cores.secondary, cores.info, cores.accent, cores.purple], borderWidth: 0 }}]
            }},
            options: {{ responsive: true, maintainAspectRatio: false, plugins: {{ legend: {{ position: 'bottom', labels: {{ color: '#fff' }} }} }} }}
        }});

        // Graficos TSB
        const tsbResumo = dadosTSB.resumo_dashboard || {{}};
        const porSetor = tsbResumo.por_setor || {{}};
        const porClass = tsbResumo.por_classificacao || {{}};

        new Chart(document.getElementById('chartTSBSetor'), {{
            type: 'bar',
            data: {{
                labels: Object.keys(porSetor),
                datasets: [{{ label: 'Empresas', data: Object.values(porSetor), backgroundColor: [cores.accent, cores.info, cores.purple, cores.teal], borderRadius: 8 }}]
            }},
            options: {{
                responsive: true, maintainAspectRatio: false,
                plugins: {{ legend: {{ display: false }} }},
                scales: {{ x: {{ ticks: {{ color: '#fff' }} }}, y: {{ ticks: {{ color: '#fff' }}, grid: {{ color: 'rgba(255,255,255,0.1)' }} }} }}
            }}
        }});

        new Chart(document.getElementById('chartTSBClass'), {{
            type: 'doughnut',
            data: {{
                labels: Object.keys(porClass),
                datasets: [{{ data: Object.values(porClass), backgroundColor: [cores.verde, cores.transicao, cores.info, cores.red], borderWidth: 0 }}]
            }},
            options: {{ responsive: true, maintainAspectRatio: false, plugins: {{ legend: {{ position: 'bottom', labels: {{ color: '#fff' }} }} }} }}
        }});

        // Funcoes auxiliares
        function getTagFoco(foco) {{
            const classes = {{ 'Ambiental': 'ambiental', 'Social': 'social', 'Governanca': 'governanca', 'Multi-tema': 'multi' }};
            return `<span class="tag ${{classes[foco] || 'multi'}}">${{foco || 'Multi-tema'}}</span>`;
        }}

        function getTagSetor(setor) {{
            const classes = {{ 'Energia': 'energia', 'Saneamento e Residuos': 'saneamento', 'Telecomunicacoes': 'telecom', 'Servicos Financeiros': 'financeiro' }};
            return `<span class="tag ${{classes[setor] || 'multi'}}">${{setor}}</span>`;
        }}

        function getTagClass(classificacao) {{
            const c = classificacao.toLowerCase();
            if (c === 'verde') return '<span class="tag verde">VERDE</span>';
            if (c === 'transicao') return '<span class="tag transicao">TRANSICAO</span>';
            return `<span class="tag">${{classificacao}}</span>`;
        }}

        // Preencher tabela Fundos IS
        const tabelaIS = document.getElementById('tabelaFundosIS');
        dadosSQL.fundos_is.forEach(f => {{
            const tr = document.createElement('tr');
            tr.innerHTML = `<td>${{f.razao_social_fundo || '-'}}</td><td>${{f.identificador_fundo || '-'}}</td><td>${{f.tipo_fundo || '-'}}</td><td>${{getTagFoco(f.FocoESG)}}</td>`;
            tabelaIS.appendChild(tr);
        }});

        // Preencher tabela Fundos ESG
        const tabelaESG = document.getElementById('tabelaFundosESG');
        dadosSQL.fundos_esg.forEach(f => {{
            const tr = document.createElement('tr');
            tr.innerHTML = `<td>${{f.razao_social_fundo || '-'}}</td><td>${{f.identificador_fundo || '-'}}</td><td>${{f.tipo_fundo || '-'}}</td><td>${{getTagFoco(f.FocoESG)}}</td>`;
            tabelaESG.appendChild(tr);
        }});

        // Preencher tabela TSB Empresas
        const tabelaTSB = document.getElementById('tabelaTSBEmpresas');
        const empresasTSB = dadosTSB.empresas || [];
        empresasTSB.forEach(e => {{
            const tr = document.createElement('tr');
            tr.innerHTML = `
                <td>${{e.emissor}}</td>
                <td>${{e.cnpj}}</td>
                <td>${{getTagSetor(e.setor_tsb)}}</td>
                <td>${{getTagClass(e.classificacao)}}</td>
                <td>${{e.score}}</td>
                <td>${{e.titulos}}</td>
            `;
            tabelaTSB.appendChild(tr);
        }});

        // Preencher KPIs por setor
        function renderKPIs(containerId, kpis) {{
            const container = document.getElementById(containerId);
            if (!kpis || !kpis.kpis) return;
            kpis.kpis.forEach(kpi => {{
                const div = document.createElement('div');
                div.className = 'kpi-item';
                div.innerHTML = `
                    <div class="kpi-codigo">${{kpi.id}}</div>
                    <div class="kpi-nome">${{kpi.nome}}</div>
                    <div class="kpi-valor">${{kpi.obrigatorio ? 'Obrigatorio' : 'Opcional'}}</div>
                    <div class="kpi-unidade">${{kpi.unidade}} | ${{kpi.frequencia}}</div>
                `;
                container.appendChild(div);
            }});
        }}

        const kpisSetor = dadosTSB.kpis_obrigatorios_por_setor || {{}};
        renderKPIs('kpisEnergia', kpisSetor.energia);
        renderKPIs('kpisSaneamento', kpisSetor.saneamento);
        renderKPIs('kpisTelecom', kpisSetor.telecomunicacoes);
        renderKPIs('kpisFinanceiro', kpisSetor.servicos_financeiros);
    </script>
</body>
</html>'''

    return html


def main():
    print("=" * 60)
    print("Gerando Dashboard Sustentabilidade (ESG + TSB)")
    print("=" * 60)

    server = 'localhost'

    dashboard = DashboardSQL(server=server)

    if not dashboard.conectar():
        print("\nERRO: Nao foi possivel conectar ao SQL Server.")
        return

    try:
        print("\nCarregando dados do SQL Server...")
        dados = dashboard.obter_dados_completos()

        print("Carregando dados TSB do JSON...")
        dados_tsb = carregar_dados_tsb()

        if not dados_tsb:
            print("AVISO: Dados TSB nao encontrados, continuando sem TSB...")
            dados_tsb = {'empresas': [], 'resumo_dashboard': {}, 'kpis_obrigatorios_por_setor': {}}

        # Gerar HTML
        html = gerar_dashboard_html(dados, dados_tsb)

        # Salvar
        os.makedirs(DASHBOARD_DIR, exist_ok=True)
        output_file = os.path.join(DASHBOARD_DIR, 'dashboard_sql_server.html')
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(html)

        print(f"\nDashboard gerado: {output_file}")
        print(f"\nResumo:")
        print(f"  - Fundos ESG: {dados['total_esg']:,}")
        print(f"  - Fundos IS: {dados['total_is']:,}")
        print(f"  - Empresas TSB: {len(dados_tsb.get('empresas', []))}")

    finally:
        dashboard.fechar()


if __name__ == '__main__':
    main()
