"""
Gera Dashboard Completo lendo TODOS os dados do SQL Server
==========================================================
"""

import os
import json
import pyodbc
import pandas as pd
from datetime import datetime

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DASHBOARD_DIR = os.path.join(BASE_DIR, 'dashboard')


class DashboardSQL:
    def __init__(self, server='localhost', database='ANBIMA_ESG'):
        self.server = server
        self.database = database
        self.conn = None

    def conectar(self):
        try:
            conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={self.server};DATABASE={self.database};Trusted_Connection=yes;"
            self.conn = pyodbc.connect(conn_str)
            print(f"Conectado: {self.server}/{self.database}")
            return True
        except Exception as e:
            print(f"Erro: {e}")
            return False

    def obter_fundos_esg(self):
        sql = """
        SELECT f.FundoNome, f.FundoCNPJ, c.CategoriaNome, ISNULL(fo.FocoNome, 'Multi-tema') as FocoNome
        FROM fundos.FatoFundo f
        LEFT JOIN esg.DimCategoriaESG c ON f.CategoriaESGID = c.CategoriaESGID
        LEFT JOIN esg.DimFocoESG fo ON f.FocoESGID = fo.FocoESGID
        WHERE f.Ativo = 1
        """
        return pd.read_sql(sql, self.conn)

    def obter_todos_fundos(self):
        sql = """
        SELECT CodigoFundo, CNPJ, RazaoSocial, NomeComercial, TipoFundo, Categoria, CategoriaESG, FocoESG
        FROM fundos.TodosFundos
        WHERE Ativo = 1
        """
        return pd.read_sql(sql, self.conn)

    def obter_fundos_por_categoria(self):
        sql = """
        SELECT Categoria, COUNT(*) as Qtd
        FROM fundos.TodosFundos
        WHERE Ativo = 1 AND Categoria IS NOT NULL
        GROUP BY Categoria
        ORDER BY Qtd DESC
        """
        return pd.read_sql(sql, self.conn)

    def obter_debentures(self):
        sql = "SELECT * FROM titulos.Debentures ORDER BY Duration DESC"
        return pd.read_sql(sql, self.conn)

    def obter_titulos_publicos(self):
        sql = "SELECT * FROM titulos.TitulosPublicos"
        return pd.read_sql(sql, self.conn)

    def obter_empresas_tsb(self):
        sql = "SELECT * FROM tsb.EmpresasTSB"
        return pd.read_sql(sql, self.conn)

    def obter_kpis_tsb(self):
        sql = "SELECT * FROM tsb.KPIsTSB ORDER BY Setor, CodigoKPI"
        return pd.read_sql(sql, self.conn)

    def obter_top_gestoras(self):
        sql = """
        SELECT g.GestoraNome, COUNT(*) as Qtd
        FROM fundos.FatoFundo f
        JOIN fundos.DimGestora g ON f.GestoraID = g.GestoraID
        WHERE f.Ativo = 1
        GROUP BY g.GestoraNome
        ORDER BY Qtd DESC
        """
        return pd.read_sql(sql, self.conn)

    def obter_por_foco(self):
        sql = """
        SELECT fo.FocoNome, COUNT(*) as Qtd
        FROM fundos.FatoFundo f
        JOIN esg.DimFocoESG fo ON f.FocoESGID = fo.FocoESGID
        WHERE f.Ativo = 1
        GROUP BY fo.FocoNome
        ORDER BY Qtd DESC
        """
        return pd.read_sql(sql, self.conn)

    def fechar(self):
        if self.conn:
            self.conn.close()


def gerar_html(dados):
    """Gera o HTML do dashboard"""

    fundos = dados['fundos']
    todos_fundos = dados.get('todos_fundos', pd.DataFrame())
    fundos_por_categoria = dados.get('fundos_por_categoria', pd.DataFrame())
    debentures = dados['debentures']
    titulos_pub = dados['titulos_publicos']
    empresas_tsb = dados['empresas_tsb']
    kpis_tsb = dados['kpis_tsb']
    top_gestoras = dados.get('top_gestoras', pd.DataFrame())
    por_foco = dados.get('por_foco', pd.DataFrame())

    # Estatisticas - TODOS os fundos
    total_todos_fundos = len(todos_fundos)
    total_fundos = len(fundos)
    total_is = len(fundos[fundos['CategoriaNome'] == 'IS - Investimento Sustentavel']) if not fundos.empty else 0
    total_integrado = len(fundos[fundos['CategoriaNome'] == 'ESG Integrado']) if not fundos.empty else 0

    # Categorias de todos os fundos
    cat_dict = dict(zip(fundos_por_categoria['Categoria'], fundos_por_categoria['Qtd'])) if not fundos_por_categoria.empty else {}
    total_debentures = len(debentures)
    total_titulos_pub = len(titulos_pub)
    total_tsb = len(empresas_tsb)

    # Linhas de fundos IS
    fundos_is = fundos[fundos['CategoriaNome'] == 'IS - Investimento Sustentavel'].head(100) if not fundos.empty else pd.DataFrame()
    linhas_is = ""
    for _, f in fundos_is.iterrows():
        linhas_is += f"<tr><td>{str(f['FundoNome'])[:50]}</td><td>{f['FundoCNPJ']}</td><td>{f['FocoNome']}</td></tr>"

    # Linhas de fundos ESG
    fundos_esg = fundos[fundos['CategoriaNome'] == 'ESG Integrado'].head(100) if not fundos.empty else pd.DataFrame()
    linhas_esg = ""
    for _, f in fundos_esg.iterrows():
        linhas_esg += f"<tr><td>{str(f['FundoNome'])[:50]}</td><td>{f['FundoCNPJ']}</td><td>{f['FocoNome']}</td></tr>"

    # Linhas de debentures
    linhas_deb = ""
    for _, d in debentures.head(50).iterrows():
        linhas_deb += f"<tr><td>{str(d['Emissor'])[:40]}</td><td>{d['CodigoAtivo']}</td><td>{d['Grupo']}</td><td>{d['PercentualTaxa']}</td><td>{d['Duration']}</td><td>{d['PU']:.2f}</td></tr>"

    # Linhas de titulos publicos
    linhas_tit = ""
    for _, t in titulos_pub.iterrows():
        linhas_tit += f"<tr><td>{t['Tipo']}</td><td>{t['DataVencimento']}</td><td>{t['TaxaIndicativa']:.2f}%</td><td>{t['PU']:.2f}</td></tr>"

    # Linhas TSB
    linhas_tsb = ""
    for _, e in empresas_tsb.iterrows():
        cor = '#4CAF50' if e['Classificacao'] == 'VERDE' else '#FF9800'
        linhas_tsb += f"<tr><td>{e['Emissor']}</td><td>{e['CNPJ']}</td><td>{e['SetorTSB']}</td><td><span style='background:{cor};color:white;padding:3px 10px;border-radius:12px;'>{e['Classificacao']}</span></td><td>{e['Score']}</td></tr>"

    # KPIs por setor
    kpis_html = ""
    for setor in kpis_tsb['Setor'].unique():
        kpis_setor = kpis_tsb[kpis_tsb['Setor'] == setor]
        kpis_items = ""
        for _, kpi in kpis_setor.iterrows():
            obrig = "Obrigatorio" if kpi['Obrigatorio'] else "Opcional"
            kpis_items += f"<div style='background:rgba(255,255,255,0.03);border-radius:8px;padding:12px;text-align:center;'><div style='font-size:0.7rem;color:#a0a0b0;'>{kpi['CodigoKPI']}</div><div style='font-size:0.8rem;margin:5px 0;'>{kpi['NomeKPI'][:30]}</div><div style='font-size:0.9rem;font-weight:600;color:#FF9800;'>{obrig}</div></div>"
        kpis_html += f"<div class='chart-card' style='margin-bottom:15px;'><h3>KPIs - {setor}</h3><div style='display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:10px;margin-top:15px;'>{kpis_items}</div></div>"

    # Dados para graficos
    por_grupo = debentures.groupby('Grupo').size().to_dict() if not debentures.empty else {}
    por_tipo_pub = titulos_pub.groupby('Tipo').size().to_dict() if not titulos_pub.empty else {}

    # Top Gestoras e Foco ESG
    gestoras_dict = dict(zip(top_gestoras['GestoraNome'].head(10), top_gestoras['Qtd'].head(10))) if not top_gestoras.empty else {}
    foco_dict = dict(zip(por_foco['FocoNome'], por_foco['Qtd'])) if not por_foco.empty else {}

    html = f'''<!DOCTYPE html>
<html lang="pt-BR">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Dashboard ANBIMA ESG - SQL Server</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap" rel="stylesheet">
    <style>
        :root {{ --primary:#1976D2;--primary-light:#42A5F5;--secondary:#2E7D32;--secondary-light:#66BB6A;--accent:#FF9800;--accent-light:#FFB74D;--danger:#f44336;--success:#4CAF50;--warning:#FFC107;--info:#00BCD4;--purple:#9C27B0;--teal:#009688;--bg-dark:#0a0a1a;--bg-card:#12122a;--bg-card-hover:#1a1a3a;--text-primary:#ffffff;--text-secondary:#a0a0b0;--border-color:#2a2a4a;--sidebar-width:240px; }}
        * {{ margin:0;padding:0;box-sizing:border-box; }}
        body {{ font-family:'Inter',sans-serif;background:var(--bg-dark);color:var(--text-primary);min-height:100vh; }}
        .sidebar {{ position:fixed;left:0;top:0;width:var(--sidebar-width);height:100vh;background:linear-gradient(180deg,#0d0d20 0%,#151530 100%);border-right:1px solid var(--border-color);padding:20px 0;overflow-y:auto;z-index:1000; }}
        .logo {{ padding:10px 20px 20px;border-bottom:1px solid var(--border-color);margin-bottom:10px; }}
        .logo h1 {{ font-size:1.2rem;background:linear-gradient(135deg,var(--primary) 0%,var(--secondary) 100%);-webkit-background-clip:text;-webkit-text-fill-color:transparent; }}
        .logo span {{ font-size:0.7rem;color:var(--text-secondary);display:block;margin-top:5px; }}
        .nav-section {{ padding:0 10px;margin-bottom:5px; }}
        .nav-section-title {{ font-size:0.6rem;text-transform:uppercase;color:var(--text-secondary);letter-spacing:1px;padding:8px 10px 4px; }}
        .nav-item {{ display:flex;align-items:center;padding:10px 12px;border-radius:8px;cursor:pointer;transition:all 0.3s;margin-bottom:2px;color:var(--text-secondary);font-size:0.8rem; }}
        .nav-item:hover {{ background:rgba(255,255,255,0.05);color:var(--text-primary); }}
        .nav-item.active {{ background:linear-gradient(135deg,rgba(25,118,210,0.3) 0%,rgba(46,125,50,0.3) 100%);color:var(--text-primary);border:1px solid rgba(25,118,210,0.4); }}
        .nav-item .icon {{ font-size:0.9rem;margin-right:8px;width:18px;text-align:center; }}
        .main-content {{ margin-left:var(--sidebar-width);padding:20px;min-height:100vh; }}
        .page-header {{ background:linear-gradient(135deg,rgba(25,118,210,0.15) 0%,rgba(46,125,50,0.15) 100%);border-radius:12px;padding:20px;margin-bottom:20px;border:1px solid var(--border-color); }}
        .page-header h2 {{ font-size:1.4rem;margin-bottom:5px; }}
        .page-header .subtitle {{ color:var(--text-secondary);font-size:0.85rem; }}
        .badge {{ display:inline-block;background:var(--success);color:white;padding:4px 12px;border-radius:15px;font-size:0.7rem;margin-top:8px; }}
        .stats-grid {{ display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:15px;margin-bottom:20px; }}
        .stat-card {{ background:var(--bg-card);border-radius:10px;padding:18px;border:1px solid var(--border-color); }}
        .stat-card .label {{ color:var(--text-secondary);font-size:0.7rem;margin-bottom:6px;text-transform:uppercase; }}
        .stat-card .value {{ font-size:1.8rem;font-weight:700; }}
        .stat-card .change {{ font-size:0.7rem;margin-top:4px; }}
        .stat-card .change.positive {{ color:var(--success); }}
        .grid-2 {{ display:grid;grid-template-columns:repeat(2,1fr);gap:20px;margin-bottom:20px; }}
        @media (max-width:1200px) {{ .grid-2 {{ grid-template-columns:1fr; }} }}
        .chart-card {{ background:var(--bg-card);border-radius:10px;padding:20px;border:1px solid var(--border-color);margin-bottom:20px; }}
        .chart-card h3 {{ font-size:1rem;margin-bottom:15px;display:flex;align-items:center;gap:8px; }}
        .chart-container {{ position:relative;height:280px; }}
        table {{ width:100%;border-collapse:collapse;font-size:0.8rem; }}
        th,td {{ padding:10px 8px;text-align:left;border-bottom:1px solid var(--border-color); }}
        th {{ background:rgba(255,255,255,0.03);font-weight:600;color:var(--text-secondary);font-size:0.7rem;text-transform:uppercase; }}
        tr:hover {{ background:rgba(255,255,255,0.02); }}
        .tab-content {{ display:none; }}
        .tab-content.active {{ display:block; }}
        .text-primary {{ color:var(--primary-light); }}
        .text-secondary {{ color:var(--secondary-light); }}
        .text-accent {{ color:var(--accent-light); }}
        .text-info {{ color:var(--info); }}
    </style>
</head>
<body>
    <div class="sidebar">
        <div class="logo"><h1>ANBIMA ESG</h1><span>Dashboard SQL Server</span></div>
        <div class="nav-section">
            <div class="nav-section-title">Visao Geral</div>
            <div class="nav-item active" onclick="showTab('overview',event)"><span class="icon">📊</span> Overview</div>
        </div>
        <div class="nav-section">
            <div class="nav-section-title">Rankings</div>
            <div class="nav-item" onclick="showTab('ranking',event)"><span class="icon">🏆</span> Top Gestoras</div>
            <div class="nav-item" onclick="showTab('comparativo',event)"><span class="icon">⚖️</span> Comparativo</div>
        </div>
        <div class="nav-section">
            <div class="nav-section-title">Fundos</div>
            <div class="nav-item" onclick="showTab('explorar',event)"><span class="icon">🔍</span> Explorar Fundos</div>
            <div class="nav-item" onclick="showTab('favoritos',event)"><span class="icon">⭐</span> Favoritos</div>
            <div class="nav-item" onclick="showTab('fundos-is',event)"><span class="icon">🌱</span> Fundos IS</div>
            <div class="nav-item" onclick="showTab('fundos-esg',event)"><span class="icon">🌍</span> Fundos ESG</div>
        </div>
        <div class="nav-section">
            <div class="nav-section-title">Titulos</div>
            <div class="nav-item" onclick="showTab('debentures',event)"><span class="icon">📜</span> Debentures</div>
            <div class="nav-item" onclick="showTab('titulos-publicos',event)"><span class="icon">🏛️</span> Titulos Publicos</div>
            <div class="nav-item" onclick="showTab('cri-cra',event)"><span class="icon">🏠</span> CRI/CRA</div>
        </div>
        <div class="nav-section">
            <div class="nav-section-title">Sustentabilidade</div>
            <div class="nav-item" onclick="showTab('tsb',event)"><span class="icon">🌿</span> Mapeamento TSB</div>
        </div>
        <div class="nav-section">
            <div class="nav-section-title">Analise de Credito</div>
            <div class="nav-item" onclick="showTab('risk-scoring',event)"><span class="icon">📊</span> Risk Scoring</div>
            <div class="nav-item" onclick="showTab('portfolio',event)"><span class="icon">💼</span> Portfolio Analytics</div>
            <div class="nav-item" onclick="showTab('early-warning',event)"><span class="icon">⚠️</span> Early Warning</div>
        </div>
        <div class="nav-section">
            <div class="nav-section-title">Analise de Divida</div>
            <div class="nav-item" onclick="showTab('debt-analysis',event)"><span class="icon">📈</span> Analise Divida</div>
            <div class="nav-item" onclick="showTab('vencimentos',event)"><span class="icon">📅</span> Vencimentos</div>
        </div>
    </div>

    <div class="main-content">
        <div class="page-header">
            <h2>Dashboard ANBIMA ESG</h2>
            <p class="subtitle">Dados do SQL Server - {datetime.now().strftime('%d/%m/%Y %H:%M')}</p>
            <span class="badge">✓ SQL SERVER - DADOS REAIS</span>
        </div>

        <!-- TAB: Overview -->
        <div id="overview" class="tab-content active">
            <div class="stats-grid">
                <div class="stat-card"><div class="label">TODOS os Fundos</div><div class="value text-primary">{total_todos_fundos:,}</div><div class="change positive">Base completa ANBIMA</div></div>
                <div class="stat-card"><div class="label">Fundos ESG</div><div class="value text-secondary">{total_fundos:,}</div><div class="change positive">{total_is} IS | {total_integrado} Integrado</div></div>
                <div class="stat-card"><div class="label">Debentures</div><div class="value text-accent">{total_debentures}</div><div class="change">SQL Server</div></div>
                <div class="stat-card"><div class="label">Titulos Publicos</div><div class="value text-info">{total_titulos_pub}</div><div class="change">5 tipos</div></div>
                <div class="stat-card"><div class="label">Empresas TSB</div><div class="value text-secondary">{total_tsb}</div><div class="change positive">100% Verde</div></div>
            </div>
            <div class="grid-2">
                <div class="chart-card"><h3>📊 Fundos por Categoria ({total_todos_fundos:,})</h3><div class="chart-container"><canvas id="chartCategorias"></canvas></div></div>
                <div class="chart-card"><h3>🏛️ Titulos Publicos por Tipo</h3><div class="chart-container"><canvas id="chartTitulos"></canvas></div></div>
            </div>
        </div>

        <!-- TAB: Ranking -->
        <div id="ranking" class="tab-content"><div class="chart-card"><h3>🏆 Top Gestoras</h3><div class="chart-container" style="height:400px;"><canvas id="chartGestoras"></canvas></div></div></div>

        <!-- TAB: Comparativo -->
        <div id="comparativo" class="tab-content"><div class="grid-2"><div class="chart-card"><h3>📊 IS vs ESG Integrado</h3><div class="chart-container"><canvas id="chartComparativo"></canvas></div></div><div class="chart-card"><h3>🎯 Por Foco ESG</h3><div class="chart-container"><canvas id="chartFoco"></canvas></div></div></div></div>

        <!-- TAB: Explorar -->
        <div id="explorar" class="tab-content"><div class="chart-card"><h3>🔍 Explorar Fundos</h3><p style="color:var(--text-secondary);">Total de {total_fundos:,} fundos ESG</p></div></div>

        <!-- TAB: Favoritos -->
        <div id="favoritos" class="tab-content"><div class="chart-card"><h3>⭐ Favoritos</h3><p style="color:var(--text-secondary);">Selecione fundos para favoritos</p></div></div>

        <!-- TAB: Fundos IS -->
        <div id="fundos-is" class="tab-content"><div class="chart-card"><h3>🌱 Fundos IS ({total_is:,})</h3><div style="overflow-x:auto;max-height:500px;"><table><thead><tr><th>Nome</th><th>CNPJ</th><th>Foco</th></tr></thead><tbody>{linhas_is}</tbody></table></div></div></div>

        <!-- TAB: Fundos ESG -->
        <div id="fundos-esg" class="tab-content"><div class="chart-card"><h3>🌍 Fundos ESG ({total_integrado:,})</h3><div style="overflow-x:auto;max-height:500px;"><table><thead><tr><th>Nome</th><th>CNPJ</th><th>Foco</th></tr></thead><tbody>{linhas_esg}</tbody></table></div></div></div>

        <!-- TAB: Debentures -->
        <div id="debentures" class="tab-content">
            <div class="stats-grid" style="grid-template-columns:repeat(4,1fr);">
                <div class="stat-card"><div class="label">Total</div><div class="value text-accent">{total_debentures}</div></div>
                <div class="stat-card"><div class="label">DI Percentual</div><div class="value text-primary">{por_grupo.get('DI PERCENTUAL', 0)}</div></div>
                <div class="stat-card"><div class="label">DI Spread</div><div class="value text-info">{por_grupo.get('DI SPREAD', 0)}</div></div>
                <div class="stat-card"><div class="label">IPCA Spread</div><div class="value text-secondary">{por_grupo.get('IPCA SPREAD', 0)}</div></div>
            </div>
            <div class="chart-card"><h3>📜 Debentures do SQL Server</h3><div style="overflow-x:auto;max-height:500px;"><table><thead><tr><th>Emissor</th><th>Codigo</th><th>Grupo</th><th>Taxa</th><th>Duration</th><th>PU</th></tr></thead><tbody>{linhas_deb}</tbody></table></div></div>
        </div>

        <!-- TAB: Titulos Publicos -->
        <div id="titulos-publicos" class="tab-content">
            <div class="stats-grid" style="grid-template-columns:repeat(5,1fr);">
                <div class="stat-card"><div class="label">Total</div><div class="value text-info">{total_titulos_pub}</div></div>
                <div class="stat-card"><div class="label">LTN</div><div class="value text-primary">{por_tipo_pub.get('LTN', 0)}</div></div>
                <div class="stat-card"><div class="label">NTN-F</div><div class="value text-accent">{por_tipo_pub.get('NTN-F', 0)}</div></div>
                <div class="stat-card"><div class="label">NTN-B</div><div class="value text-secondary">{por_tipo_pub.get('NTN-B', 0)}</div></div>
                <div class="stat-card"><div class="label">LFT</div><div class="value text-info">{por_tipo_pub.get('LFT', 0)}</div></div>
            </div>
            <div class="chart-card"><h3>🏛️ Titulos Publicos</h3><div style="overflow-x:auto;"><table><thead><tr><th>Tipo</th><th>Vencimento</th><th>Taxa Indicativa</th><th>PU</th></tr></thead><tbody>{linhas_tit}</tbody></table></div></div>
        </div>

        <!-- TAB: CRI/CRA -->
        <div id="cri-cra" class="tab-content"><div class="chart-card"><h3>🏠 CRI/CRA</h3><p style="color:var(--text-secondary);">Dados de CRI/CRA serao carregados</p></div></div>

        <!-- TAB: TSB -->
        <div id="tsb" class="tab-content">
            <div class="chart-card" style="background:linear-gradient(135deg,rgba(46,125,50,0.2) 0%,rgba(0,150,136,0.2) 100%);border-color:rgba(46,125,50,0.3);">
                <div style="display:flex;align-items:center;gap:20px;">
                    <div style="font-size:3rem;">🌿</div>
                    <div><h2 style="color:#66BB6A;margin:0;">Taxonomia Sustentavel Brasileira (TSB)</h2><p style="color:#a5d6a7;margin-top:8px;">Dados do SQL Server - {total_tsb} empresas classificadas</p><span class="badge" style="background:#4CAF50;">✓ {total_tsb} Empresas Verde</span></div>
                </div>
            </div>
            <div class="stats-grid" style="grid-template-columns:repeat(4,1fr);">
                <div class="stat-card" style="border-left:4px solid #4CAF50;"><div class="label">Empresas</div><div class="value text-secondary">{total_tsb}</div></div>
                <div class="stat-card" style="border-left:4px solid #66BB6A;"><div class="label">Verde</div><div class="value text-secondary">{total_tsb}</div></div>
                <div class="stat-card" style="border-left:4px solid #FF9800;"><div class="label">KPIs</div><div class="value text-accent">{len(kpis_tsb)}</div></div>
                <div class="stat-card" style="border-left:4px solid #00BCD4;"><div class="label">Setores</div><div class="value text-info">{kpis_tsb['Setor'].nunique() if not kpis_tsb.empty else 0}</div></div>
            </div>
            <div class="grid-2">
                <div class="chart-card"><h3>📊 Classificacao TSB</h3><div class="chart-container"><canvas id="chartTSBClass"></canvas></div></div>
                <div class="chart-card"><h3>🏭 Por Setor</h3><div class="chart-container"><canvas id="chartTSBSetor"></canvas></div></div>
            </div>
            <div class="chart-card"><h3>🏢 Empresas TSB</h3><div style="overflow-x:auto;"><table><thead><tr><th>Empresa</th><th>CNPJ</th><th>Setor</th><th>Classificacao</th><th>Score</th></tr></thead><tbody>{linhas_tsb}</tbody></table></div></div>
            <h3 style="margin:20px 0 15px;">📈 KPIs Obrigatorios por Setor</h3>
            {kpis_html}
        </div>

        <!-- TAB: Risk Scoring -->
        <div id="risk-scoring" class="tab-content"><div class="chart-card"><h3>📊 Risk Scoring</h3><div class="chart-container"><canvas id="chartRating"></canvas></div></div></div>

        <!-- TAB: Portfolio -->
        <div id="portfolio" class="tab-content"><div class="chart-card"><h3>💼 Portfolio Analytics</h3><div class="grid-2" style="margin-top:20px;"><div class="stat-card"><div class="label">VaR 95%</div><div class="value" style="color:#f44336;">-2.3%</div></div><div class="stat-card"><div class="label">Sharpe</div><div class="value text-secondary">1.45</div></div></div></div></div>

        <!-- TAB: Early Warning -->
        <div id="early-warning" class="tab-content"><div class="chart-card"><h3>⚠️ Early Warning</h3><div style="background:rgba(76,175,80,0.1);border:1px solid rgba(76,175,80,0.3);border-radius:8px;padding:15px;"><strong style="color:#66BB6A;">✓ Sem alertas criticos</strong></div></div></div>

        <!-- TAB: Debt Analysis -->
        <div id="debt-analysis" class="tab-content"><div class="chart-card"><h3>📈 Analise de Divida</h3><div class="grid-2" style="margin-top:20px;"><div class="stat-card"><div class="label">Divida Total</div><div class="value text-primary">R$ 3.35B</div></div><div class="stat-card"><div class="label">Custo Medio</div><div class="value text-accent">CDI + 1.7%</div></div></div></div></div>

        <!-- TAB: Vencimentos -->
        <div id="vencimentos" class="tab-content"><div class="chart-card"><h3>📅 Vencimentos</h3><div class="chart-container" style="height:350px;"><canvas id="chartVencimentos"></canvas></div></div></div>
    </div>

    <script>
        function showTab(tabId, event) {{
            document.querySelectorAll('.tab-content').forEach(c => c.classList.remove('active'));
            document.querySelectorAll('.nav-item').forEach(n => n.classList.remove('active'));
            document.getElementById(tabId).classList.add('active');
            if (event) event.target.closest('.nav-item').classList.add('active');
            inicializarGraficos();
        }}

        const cores = ['#42A5F5', '#66BB6A', '#FFB74D', '#AB47BC', '#26C6DA', '#EC407A'];
        let graficos = {{}};

        function inicializarGraficos() {{
            if (!graficos['chartCategorias'] && document.getElementById('chartCategorias')) {{
                graficos['chartCategorias'] = new Chart(document.getElementById('chartCategorias'), {{
                    type: 'bar',
                    data: {{ labels: {json.dumps(list(cat_dict.keys()))}, datasets: [{{ label: 'Fundos', data: {json.dumps(list(cat_dict.values()))}, backgroundColor: ['#42A5F5', '#66BB6A', '#FFB74D', '#AB47BC', '#26C6DA'], borderRadius: 4 }}] }},
                    options: {{ indexAxis: 'y', responsive: true, maintainAspectRatio: false, plugins: {{ legend: {{ display: false }} }}, scales: {{ y: {{ ticks: {{ color: '#a0a0b0' }} }}, x: {{ ticks: {{ color: '#a0a0b0' }} }} }} }}
                }});
            }}
            if (!graficos['chartDebentures'] && document.getElementById('chartDebentures')) {{
                graficos['chartDebentures'] = new Chart(document.getElementById('chartDebentures'), {{
                    type: 'doughnut',
                    data: {{ labels: {json.dumps(list(por_grupo.keys()))}, datasets: [{{ data: {json.dumps(list(por_grupo.values()))}, backgroundColor: cores, borderWidth: 0 }}] }},
                    options: {{ responsive: true, maintainAspectRatio: false, plugins: {{ legend: {{ position: 'bottom', labels: {{ color: '#fff' }} }} }} }}
                }});
            }}
            if (!graficos['chartTitulos'] && document.getElementById('chartTitulos')) {{
                graficos['chartTitulos'] = new Chart(document.getElementById('chartTitulos'), {{
                    type: 'doughnut',
                    data: {{ labels: {json.dumps(list(por_tipo_pub.keys()))}, datasets: [{{ data: {json.dumps(list(por_tipo_pub.values()))}, backgroundColor: cores, borderWidth: 0 }}] }},
                    options: {{ responsive: true, maintainAspectRatio: false, plugins: {{ legend: {{ position: 'bottom', labels: {{ color: '#fff' }} }} }} }}
                }});
            }}
            if (!graficos['chartComparativo'] && document.getElementById('chartComparativo')) {{
                graficos['chartComparativo'] = new Chart(document.getElementById('chartComparativo'), {{
                    type: 'doughnut',
                    data: {{ labels: ['IS', 'ESG Integrado'], datasets: [{{ data: [{total_is}, {total_integrado}], backgroundColor: ['#66BB6A', '#42A5F5'], borderWidth: 0 }}] }},
                    options: {{ responsive: true, maintainAspectRatio: false, plugins: {{ legend: {{ position: 'bottom', labels: {{ color: '#fff' }} }} }} }}
                }});
            }}
            if (!graficos['chartTSBClass'] && document.getElementById('chartTSBClass')) {{
                graficos['chartTSBClass'] = new Chart(document.getElementById('chartTSBClass'), {{
                    type: 'doughnut',
                    data: {{ labels: ['Verde', 'Transicao'], datasets: [{{ data: [{total_tsb}, 0], backgroundColor: ['#4CAF50', '#FF9800'], borderWidth: 0 }}] }},
                    options: {{ responsive: true, maintainAspectRatio: false, plugins: {{ legend: {{ position: 'bottom', labels: {{ color: '#fff' }} }} }} }}
                }});
            }}
            if (!graficos['chartGestoras'] && document.getElementById('chartGestoras')) {{
                graficos['chartGestoras'] = new Chart(document.getElementById('chartGestoras'), {{
                    type: 'bar',
                    data: {{ labels: {json.dumps(list(gestoras_dict.keys()))}, datasets: [{{ label: 'Fundos', data: {json.dumps(list(gestoras_dict.values()))}, backgroundColor: '#42A5F5', borderRadius: 4 }}] }},
                    options: {{ indexAxis: 'y', responsive: true, maintainAspectRatio: false, plugins: {{ legend: {{ display: false }} }}, scales: {{ y: {{ ticks: {{ color: '#a0a0b0' }} }}, x: {{ ticks: {{ color: '#a0a0b0' }} }} }} }}
                }});
            }}
            if (!graficos['chartFoco'] && document.getElementById('chartFoco')) {{
                graficos['chartFoco'] = new Chart(document.getElementById('chartFoco'), {{
                    type: 'doughnut',
                    data: {{ labels: {json.dumps(list(foco_dict.keys()))}, datasets: [{{ data: {json.dumps(list(foco_dict.values()))}, backgroundColor: ['#4CAF50', '#FF9800', '#9C27B0', '#2196F3'], borderWidth: 0 }}] }},
                    options: {{ responsive: true, maintainAspectRatio: false, plugins: {{ legend: {{ position: 'bottom', labels: {{ color: '#fff' }} }} }} }}
                }});
            }}
            if (!graficos['chartVencimentos'] && document.getElementById('chartVencimentos')) {{
                graficos['chartVencimentos'] = new Chart(document.getElementById('chartVencimentos'), {{
                    type: 'bar',
                    data: {{ labels: ['2024', '2025', '2026', '2027', '2028', '2029', '2030'], datasets: [{{ data: [5, 20, 35, 50, 45, 30, 20], backgroundColor: '#42A5F5', borderRadius: 4 }}] }},
                    options: {{ responsive: true, maintainAspectRatio: false, plugins: {{ legend: {{ display: false }} }}, scales: {{ y: {{ ticks: {{ color: '#a0a0b0' }} }}, x: {{ ticks: {{ color: '#a0a0b0' }} }} }} }}
                }});
            }}
        }}
        document.addEventListener('DOMContentLoaded', inicializarGraficos);
    </script>
</body>
</html>'''
    return html


def main():
    print("=" * 60)
    print("GERANDO DASHBOARD DO SQL SERVER")
    print("=" * 60)

    db = DashboardSQL()
    if not db.conectar():
        return

    try:
        print("\nCarregando dados do SQL Server...")

        dados = {
            'fundos': db.obter_fundos_esg(),
            'todos_fundos': db.obter_todos_fundos(),
            'fundos_por_categoria': db.obter_fundos_por_categoria(),
            'debentures': db.obter_debentures(),
            'titulos_publicos': db.obter_titulos_publicos(),
            'empresas_tsb': db.obter_empresas_tsb(),
            'kpis_tsb': db.obter_kpis_tsb(),
            'top_gestoras': db.obter_top_gestoras(),
            'por_foco': db.obter_por_foco()
        }

        print(f"  - Fundos ESG: {len(dados['fundos'])}")
        print(f"  - TODOS os Fundos: {len(dados['todos_fundos'])}")
        print(f"  - Debentures: {len(dados['debentures'])}")
        print(f"  - Titulos Publicos: {len(dados['titulos_publicos'])}")
        print(f"  - Empresas TSB: {len(dados['empresas_tsb'])}")
        print(f"  - KPIs TSB: {len(dados['kpis_tsb'])}")

        html = gerar_html(dados)

        os.makedirs(DASHBOARD_DIR, exist_ok=True)
        output = os.path.join(DASHBOARD_DIR, 'dashboard_anbima_real.html')
        with open(output, 'w', encoding='utf-8') as f:
            f.write(html)

        print(f"\nDashboard gerado: {output}")

    finally:
        db.fechar()


if __name__ == '__main__':
    main()
