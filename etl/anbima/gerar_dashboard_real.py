"""
Gerador de Dashboard com Dados Reais da ANBIMA
==============================================
Este script le os dados reais coletados da API ANBIMA
e gera um dashboard HTML com os dados embutidos.
"""

import pandas as pd
import os
import json
from datetime import datetime

# Diretorios
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DATA_DIR = os.path.join(BASE_DIR, 'data', 'anbima')
DASHBOARD_DIR = os.path.join(BASE_DIR, 'dashboard')

def encontrar_arquivo_mais_recente(prefixo):
    """Encontra o arquivo mais recente com o prefixo dado"""
    files = [f for f in os.listdir(DATA_DIR) if f.startswith(prefixo) and f.endswith('.csv')]
    if files:
        return os.path.join(DATA_DIR, sorted(files)[-1])
    return None

def carregar_dados_esg():
    """Carrega dados de fundos ESG"""
    arquivo = encontrar_arquivo_mais_recente('fundos_esg_')
    if arquivo:
        df = pd.read_csv(arquivo)
        print(f"Carregados {len(df)} fundos ESG de {arquivo}")
        return df
    return pd.DataFrame()

def processar_dados_para_dashboard(df):
    """Processa dados para o formato do dashboard"""
    if df.empty:
        return {}

    # Estatisticas gerais
    total_fundos = len(df)
    total_is = len(df[df['CategoriaESG'] == 'IS - Investimento Sustentavel'])
    total_esg = len(df[df['CategoriaESG'] == 'ESG Integrado'])

    # Por categoria
    por_categoria = df['CategoriaESG'].value_counts().to_dict()

    # Por foco
    por_foco = df['FocoESG'].value_counts().to_dict()

    # Lista de fundos IS (primeiros 50)
    fundos_is = df[df['CategoriaESG'] == 'IS - Investimento Sustentavel'][
        ['razao_social_fundo', 'identificador_fundo', 'tipo_fundo', 'FocoESG']
    ].head(50).to_dict('records')

    # Lista de fundos ESG Integrado (primeiros 50)
    fundos_esg = df[df['CategoriaESG'] == 'ESG Integrado'][
        ['razao_social_fundo', 'identificador_fundo', 'tipo_fundo', 'FocoESG']
    ].head(50).to_dict('records')

    # Top gestoras (extrair do nome do fundo)
    def extrair_gestora(nome):
        if pd.isna(nome):
            return 'Outros'
        nome_upper = str(nome).upper()
        gestoras = ['ITAU', 'BRADESCO', 'BB ', 'SANTANDER', 'CAIXA', 'BTG', 'XP', 'SAFRA',
                   'VOTORANTIM', 'CREDIT SUISSE', 'VERDE', 'JGP', 'OPPORTUNITY', 'SUL AMERICA']
        for g in gestoras:
            if g in nome_upper:
                return g.strip()
        return 'Outros'

    df['Gestora'] = df['razao_social_fundo'].apply(extrair_gestora)
    por_gestora = df['Gestora'].value_counts().head(10).to_dict()

    # Tipo de fundo
    por_tipo = df['tipo_fundo'].value_counts().to_dict() if 'tipo_fundo' in df.columns else {}

    return {
        'total_fundos': total_fundos,
        'total_is': total_is,
        'total_esg_integrado': total_esg,
        'por_categoria': por_categoria,
        'por_foco': por_foco,
        'por_gestora': por_gestora,
        'por_tipo': por_tipo,
        'fundos_is': fundos_is,
        'fundos_esg': fundos_esg,
        'data_atualizacao': datetime.now().strftime('%d/%m/%Y %H:%M')
    }

def gerar_dashboard_html(dados):
    """Gera o HTML do dashboard com dados reais"""

    html = f'''<!DOCTYPE html>
<html lang="pt-BR">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Dashboard ANBIMA ESG - Dados Reais</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap" rel="stylesheet">
    <style>
        :root {{
            --primary: #1976D2;
            --primary-light: #42A5F5;
            --secondary: #2E7D32;
            --secondary-light: #66BB6A;
            --accent: #FF9800;
            --danger: #f44336;
            --success: #4CAF50;
            --warning: #FFC107;
            --info: #00BCD4;
            --purple: #9C27B0;
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
        .header .subtitle {{
            color: var(--text-secondary);
            font-size: 1rem;
        }}
        .header .badge {{
            display: inline-block;
            background: var(--success);
            color: white;
            padding: 5px 15px;
            border-radius: 20px;
            font-size: 0.8rem;
            margin-top: 15px;
        }}
        .stats-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
            gap: 20px;
            margin-bottom: 30px;
        }}
        .stat-card {{
            background: var(--bg-card);
            border-radius: 15px;
            padding: 25px;
            border: 1px solid var(--border-color);
            transition: transform 0.3s, box-shadow 0.3s;
        }}
        .stat-card:hover {{
            transform: translateY(-5px);
            box-shadow: 0 10px 30px rgba(0,0,0,0.3);
        }}
        .stat-card .label {{
            color: var(--text-secondary);
            font-size: 0.85rem;
            margin-bottom: 10px;
        }}
        .stat-card .value {{
            font-size: 2.5rem;
            font-weight: 700;
        }}
        .stat-card .value.primary {{ color: var(--primary-light); }}
        .stat-card .value.secondary {{ color: var(--secondary-light); }}
        .stat-card .value.accent {{ color: var(--accent); }}
        .stat-card .value.info {{ color: var(--info); }}
        .charts-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(400px, 1fr));
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
            font-size: 1.1rem;
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
        .chart-container {{
            position: relative;
            height: 300px;
        }}
        .funds-table {{
            width: 100%;
            border-collapse: collapse;
            margin-top: 15px;
        }}
        .funds-table th, .funds-table td {{
            padding: 12px;
            text-align: left;
            border-bottom: 1px solid var(--border-color);
        }}
        .funds-table th {{
            color: var(--text-secondary);
            font-weight: 500;
            font-size: 0.8rem;
            text-transform: uppercase;
        }}
        .funds-table td {{
            font-size: 0.9rem;
        }}
        .funds-table tr:hover {{
            background: var(--bg-card-hover);
        }}
        .tag {{
            display: inline-block;
            padding: 3px 10px;
            border-radius: 12px;
            font-size: 0.75rem;
            font-weight: 500;
        }}
        .tag.is {{ background: rgba(46, 125, 50, 0.2); color: var(--secondary-light); }}
        .tag.esg {{ background: rgba(25, 118, 210, 0.2); color: var(--primary-light); }}
        .tag.ambiental {{ background: rgba(76, 175, 80, 0.2); color: #81C784; }}
        .tag.social {{ background: rgba(255, 152, 0, 0.2); color: #FFB74D; }}
        .tag.governanca {{ background: rgba(156, 39, 176, 0.2); color: #BA68C8; }}
        .tag.multi {{ background: rgba(0, 188, 212, 0.2); color: #4DD0E1; }}
        .fonte-info {{
            text-align: center;
            padding: 20px;
            color: var(--text-secondary);
            font-size: 0.85rem;
            border-top: 1px solid var(--border-color);
            margin-top: 30px;
        }}
        .fonte-info strong {{
            color: var(--success);
        }}
        .tabs {{
            display: flex;
            gap: 10px;
            margin-bottom: 25px;
            flex-wrap: wrap;
        }}
        .tab {{
            padding: 12px 25px;
            background: var(--bg-card);
            border: 1px solid var(--border-color);
            border-radius: 10px;
            cursor: pointer;
            transition: all 0.3s;
            color: var(--text-secondary);
        }}
        .tab:hover {{ background: var(--bg-card-hover); }}
        .tab.active {{
            background: linear-gradient(135deg, rgba(25, 118, 210, 0.3) 0%, rgba(46, 125, 50, 0.3) 100%);
            border-color: var(--primary);
            color: var(--text-primary);
        }}
        .tab-content {{ display: none; }}
        .tab-content.active {{ display: block; }}
        .full-width {{ grid-column: 1 / -1; }}
    </style>
</head>
<body>
    <div class="header">
        <h1>Dashboard ANBIMA ESG</h1>
        <p class="subtitle">Fundos de Investimento Sustentavel - Dados Reais da API ANBIMA</p>
        <span class="badge">DADOS REAIS - Atualizado em {dados['data_atualizacao']}</span>
    </div>

    <div class="stats-grid">
        <div class="stat-card">
            <div class="label">Total de Fundos ESG</div>
            <div class="value primary">{dados['total_fundos']:,}</div>
        </div>
        <div class="stat-card">
            <div class="label">Fundos IS (Investimento Sustentavel)</div>
            <div class="value secondary">{dados['total_is']:,}</div>
        </div>
        <div class="stat-card">
            <div class="label">Fundos ESG Integrado</div>
            <div class="value accent">{dados['total_esg_integrado']:,}</div>
        </div>
        <div class="stat-card">
            <div class="label">Gestoras Identificadas</div>
            <div class="value info">{len(dados['por_gestora'])}</div>
        </div>
    </div>

    <div class="tabs">
        <div class="tab active" onclick="showTab('overview')">Visao Geral</div>
        <div class="tab" onclick="showTab('fundos-is')">Fundos IS</div>
        <div class="tab" onclick="showTab('fundos-esg')">Fundos ESG Integrado</div>
        <div class="tab" onclick="showTab('gestoras')">Por Gestora</div>
    </div>

    <div id="overview" class="tab-content active">
        <div class="charts-grid">
            <div class="chart-card">
                <h3>Distribuicao por Categoria ESG</h3>
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
                <h3>Tipo de Fundo</h3>
                <div class="chart-container">
                    <canvas id="chartTipo"></canvas>
                </div>
            </div>
        </div>
    </div>

    <div id="fundos-is" class="tab-content">
        <div class="chart-card full-width">
            <h3>Fundos IS - Investimento Sustentavel (Primeiros 50)</h3>
            <table class="funds-table">
                <thead>
                    <tr>
                        <th>Nome do Fundo</th>
                        <th>CNPJ</th>
                        <th>Tipo</th>
                        <th>Foco ESG</th>
                    </tr>
                </thead>
                <tbody id="tabelaFundosIS">
                </tbody>
            </table>
        </div>
    </div>

    <div id="fundos-esg" class="tab-content">
        <div class="chart-card full-width">
            <h3>Fundos ESG Integrado (Primeiros 50)</h3>
            <table class="funds-table">
                <thead>
                    <tr>
                        <th>Nome do Fundo</th>
                        <th>CNPJ</th>
                        <th>Tipo</th>
                        <th>Foco ESG</th>
                    </tr>
                </thead>
                <tbody id="tabelaFundosESG">
                </tbody>
            </table>
        </div>
    </div>

    <div id="gestoras" class="tab-content">
        <div class="charts-grid">
            <div class="chart-card full-width">
                <h3>Fundos por Gestora (Top 10)</h3>
                <div class="chart-container" style="height: 400px;">
                    <canvas id="chartGestorasBar"></canvas>
                </div>
            </div>
        </div>
    </div>

    <div class="fonte-info">
        <p><strong>Fonte: API Oficial ANBIMA</strong> - Dados coletados em tempo real via developers.anbima.com.br</p>
        <p>Total de {dados['total_fundos']:,} fundos ESG identificados no mercado brasileiro</p>
    </div>

    <script>
        // Dados reais da ANBIMA
        const dadosReais = {json.dumps(dados, ensure_ascii=False)};

        // Funcao para mostrar abas
        function showTab(tabId) {{
            document.querySelectorAll('.tab-content').forEach(c => c.classList.remove('active'));
            document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
            document.getElementById(tabId).classList.add('active');
            event.target.classList.add('active');
        }}

        // Cores para graficos
        const cores = {{
            primary: '#42A5F5',
            secondary: '#66BB6A',
            accent: '#FFB74D',
            danger: '#ef5350',
            purple: '#AB47BC',
            info: '#26C6DA',
            pink: '#EC407A',
            teal: '#26A69A',
            lime: '#9CCC65',
            amber: '#FFCA28'
        }};

        // Grafico de Categoria
        new Chart(document.getElementById('chartCategoria'), {{
            type: 'doughnut',
            data: {{
                labels: Object.keys(dadosReais.por_categoria),
                datasets: [{{
                    data: Object.values(dadosReais.por_categoria),
                    backgroundColor: [cores.secondary, cores.primary],
                    borderWidth: 0
                }}]
            }},
            options: {{
                responsive: true,
                maintainAspectRatio: false,
                plugins: {{
                    legend: {{
                        position: 'bottom',
                        labels: {{ color: '#fff', padding: 20 }}
                    }}
                }}
            }}
        }});

        // Grafico de Foco
        new Chart(document.getElementById('chartFoco'), {{
            type: 'pie',
            data: {{
                labels: Object.keys(dadosReais.por_foco),
                datasets: [{{
                    data: Object.values(dadosReais.por_foco),
                    backgroundColor: [cores.secondary, cores.info, cores.accent, cores.purple],
                    borderWidth: 0
                }}]
            }},
            options: {{
                responsive: true,
                maintainAspectRatio: false,
                plugins: {{
                    legend: {{
                        position: 'bottom',
                        labels: {{ color: '#fff', padding: 20 }}
                    }}
                }}
            }}
        }});

        // Grafico de Gestoras (pizza)
        new Chart(document.getElementById('chartGestoras'), {{
            type: 'doughnut',
            data: {{
                labels: Object.keys(dadosReais.por_gestora),
                datasets: [{{
                    data: Object.values(dadosReais.por_gestora),
                    backgroundColor: Object.values(cores),
                    borderWidth: 0
                }}]
            }},
            options: {{
                responsive: true,
                maintainAspectRatio: false,
                plugins: {{
                    legend: {{
                        position: 'right',
                        labels: {{ color: '#fff', padding: 10, font: {{ size: 10 }} }}
                    }}
                }}
            }}
        }});

        // Grafico de Tipo
        const tipoLabels = Object.keys(dadosReais.por_tipo);
        const tipoValues = Object.values(dadosReais.por_tipo);
        new Chart(document.getElementById('chartTipo'), {{
            type: 'bar',
            data: {{
                labels: tipoLabels,
                datasets: [{{
                    label: 'Quantidade',
                    data: tipoValues,
                    backgroundColor: cores.primary,
                    borderRadius: 5
                }}]
            }},
            options: {{
                responsive: true,
                maintainAspectRatio: false,
                indexAxis: 'y',
                plugins: {{
                    legend: {{ display: false }}
                }},
                scales: {{
                    x: {{
                        grid: {{ color: 'rgba(255,255,255,0.1)' }},
                        ticks: {{ color: '#fff' }}
                    }},
                    y: {{
                        grid: {{ display: false }},
                        ticks: {{ color: '#fff' }}
                    }}
                }}
            }}
        }});

        // Grafico de Gestoras (barras)
        new Chart(document.getElementById('chartGestorasBar'), {{
            type: 'bar',
            data: {{
                labels: Object.keys(dadosReais.por_gestora),
                datasets: [{{
                    label: 'Fundos ESG',
                    data: Object.values(dadosReais.por_gestora),
                    backgroundColor: Object.values(cores),
                    borderRadius: 8
                }}]
            }},
            options: {{
                responsive: true,
                maintainAspectRatio: false,
                plugins: {{
                    legend: {{ display: false }}
                }},
                scales: {{
                    x: {{
                        grid: {{ display: false }},
                        ticks: {{ color: '#fff' }}
                    }},
                    y: {{
                        grid: {{ color: 'rgba(255,255,255,0.1)' }},
                        ticks: {{ color: '#fff' }}
                    }}
                }}
            }}
        }});

        // Preencher tabela de fundos IS
        function formatarCNPJ(cnpj) {{
            if (!cnpj) return '-';
            cnpj = cnpj.toString().padStart(14, '0');
            return cnpj.replace(/^(\\d{{2}})(\\d{{3}})(\\d{{3}})(\\d{{4}})(\\d{{2}})$/, '$1.$2.$3/$4-$5');
        }}

        function getTagFoco(foco) {{
            const classes = {{
                'Ambiental': 'ambiental',
                'Social': 'social',
                'Governanca': 'governanca',
                'Multi-tema': 'multi'
            }};
            return `<span class="tag ${{classes[foco] || 'multi'}}">${{foco || 'Multi-tema'}}</span>`;
        }}

        const tabelaIS = document.getElementById('tabelaFundosIS');
        dadosReais.fundos_is.forEach(f => {{
            const tr = document.createElement('tr');
            tr.innerHTML = `
                <td>${{f.razao_social_fundo || '-'}}</td>
                <td>${{formatarCNPJ(f.identificador_fundo)}}</td>
                <td>${{f.tipo_fundo || '-'}}</td>
                <td>${{getTagFoco(f.FocoESG)}}</td>
            `;
            tabelaIS.appendChild(tr);
        }});

        const tabelaESG = document.getElementById('tabelaFundosESG');
        dadosReais.fundos_esg.forEach(f => {{
            const tr = document.createElement('tr');
            tr.innerHTML = `
                <td>${{f.razao_social_fundo || '-'}}</td>
                <td>${{formatarCNPJ(f.identificador_fundo)}}</td>
                <td>${{f.tipo_fundo || '-'}}</td>
                <td>${{getTagFoco(f.FocoESG)}}</td>
            `;
            tabelaESG.appendChild(tr);
        }});
    </script>
</body>
</html>'''

    return html

def main():
    print("=" * 60)
    print("Gerando Dashboard com Dados Reais ANBIMA")
    print("=" * 60)

    # Carregar dados
    df = carregar_dados_esg()

    if df.empty:
        print("ERRO: Nenhum dado encontrado!")
        return

    # Processar dados
    dados = processar_dados_para_dashboard(df)

    # Gerar HTML
    html = gerar_dashboard_html(dados)

    # Salvar
    output_file = os.path.join(DASHBOARD_DIR, 'dashboard_anbima_real.html')
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(html)

    print(f"\nDashboard gerado: {output_file}")
    print(f"\nResumo:")
    print(f"  - Total de fundos ESG: {dados['total_fundos']:,}")
    print(f"  - Fundos IS: {dados['total_is']:,}")
    print(f"  - Fundos ESG Integrado: {dados['total_esg_integrado']:,}")
    print(f"  - Gestoras: {len(dados['por_gestora'])}")

    return output_file

if __name__ == '__main__':
    main()
