"""
Adiciona a aba TSB ao dashboard_anbima_real.html
================================================
Este script adiciona a secao de navegacao e o conteudo da aba TSB
ao dashboard existente, sem remover nada.
"""

import os
import json
import re

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DATA_DIR = os.path.join(BASE_DIR, 'data', 'anbima')
DASHBOARD_DIR = os.path.join(BASE_DIR, 'dashboard')


def carregar_dados_tsb():
    """Carrega dados TSB do arquivo JSON"""
    arquivo = os.path.join(DATA_DIR, 'tsb_kpis_empresas.json')
    if os.path.exists(arquivo):
        with open(arquivo, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {}


def gerar_conteudo_aba_tsb(dados_tsb):
    """Gera o HTML da aba TSB"""

    empresas = dados_tsb.get('empresas', [])
    resumo = dados_tsb.get('resumo_dashboard', {})
    kpis_setor = dados_tsb.get('kpis_obrigatorios_por_setor', {})
    objetivos = dados_tsb.get('objetivos_tsb', {})

    total_empresas = len(empresas)
    total_verde = sum(1 for e in empresas if e.get('classificacao') == 'VERDE')
    total_kpis_pendentes = resumo.get('kpis_pendentes', 0)

    # Gerar linhas da tabela de empresas
    linhas_empresas = ""
    for emp in empresas:
        cor_class = {
            'VERDE': 'background: #4CAF50',
            'TRANSICAO': 'background: #8BC34A',
            'POTENCIAL': 'background: #FF9800',
            'PENDENTE': 'background: #9E9E9E'
        }.get(emp.get('classificacao', 'PENDENTE'), 'background: #9E9E9E')

        linhas_empresas += f"""
                            <tr>
                                <td><strong>{emp.get('emissor', '-')}</strong></td>
                                <td>{emp.get('cnpj', '-')}</td>
                                <td>{emp.get('setor_tsb', '-')}</td>
                                <td><span class="tag" style="{cor_class}; color: white; padding: 3px 10px; border-radius: 12px; font-size: 0.75rem;">{emp.get('classificacao', '-')}</span></td>
                                <td>{emp.get('score', 0)}</td>
                                <td>{emp.get('titulos', 0)}</td>
                            </tr>"""

    # Gerar KPIs por setor
    kpis_html = ""
    for setor_key, setor_data in kpis_setor.items():
        setor_nome = setor_data.get('setor_nome', setor_key)
        kpis = setor_data.get('kpis', [])

        kpis_items = ""
        for kpi in kpis[:6]:  # Limitar a 6 KPIs por setor
            obrig = "Obrigatorio" if kpi.get('obrigatorio') else "Opcional"
            kpis_items += f"""
                                <div style="background: rgba(255,255,255,0.03); border-radius: 8px; padding: 12px; text-align: center;">
                                    <div style="font-size: 0.7rem; color: #a0a0b0;">{kpi.get('id', '')}</div>
                                    <div style="font-size: 0.8rem; margin: 5px 0;">{kpi.get('nome', '')}</div>
                                    <div style="font-size: 0.9rem; font-weight: 600; color: #FF9800;">{obrig}</div>
                                    <div style="font-size: 0.65rem; color: #a0a0b0;">{kpi.get('unidade', '')} | {kpi.get('frequencia', '')}</div>
                                </div>"""

        kpis_html += f"""
                        <div class="chart-card" style="margin-bottom: 20px;">
                            <h3 style="margin-bottom: 15px; display: flex; align-items: center; gap: 10px;">
                                <span style="width: 4px; height: 20px; background: linear-gradient(180deg, #4CAF50, #009688); border-radius: 2px;"></span>
                                KPIs - {setor_nome}
                            </h3>
                            <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); gap: 10px;">
                                {kpis_items}
                            </div>
                        </div>"""

    # HTML completo da aba TSB
    aba_tsb = f"""
        <!-- Tab: TSB - Taxonomia Sustentavel Brasileira -->
        <div id="tsb" class="tab-content">
            <!-- Header TSB -->
            <div class="chart-card" style="background: linear-gradient(135deg, rgba(46, 125, 50, 0.3) 0%, rgba(0, 150, 136, 0.3) 100%); margin-bottom: 25px; border-color: rgba(46, 125, 50, 0.4);">
                <div style="display: flex; align-items: center; gap: 20px;">
                    <div style="font-size: 3.5rem;">🌿</div>
                    <div>
                        <h2 style="margin: 0; color: #66BB6A; font-size: 1.6rem;">Taxonomia Sustentavel Brasileira (TSB)</h2>
                        <p style="margin: 10px 0 0 0; color: #a5d6a7; font-size: 0.9rem;">
                            Sistema de classificacao que define criterios objetivos para identificar atividades economicas sustentaveis.
                            Baseado no Decreto 12.705/2025 e nos criterios do Ministerio da Fazenda.
                        </p>
                        <div style="margin-top: 15px;">
                            <span style="background: #4CAF50; color: white; padding: 5px 15px; border-radius: 20px; font-size: 0.75rem; margin-right: 10px;">✓ {total_verde} Empresas Verde</span>
                            <span style="background: #00BCD4; color: white; padding: 5px 15px; border-radius: 20px; font-size: 0.75rem;">Fase Voluntaria 2024-2027</span>
                        </div>
                    </div>
                </div>
            </div>

            <!-- Stats TSB -->
            <div class="stats-grid" style="grid-template-columns: repeat(4, 1fr); margin-bottom: 25px;">
                <div class="stat-card" style="border-left: 4px solid #4CAF50;">
                    <div style="font-size: 2rem; margin-bottom: 10px;">🏢</div>
                    <div class="value" style="font-size: 2rem; font-weight: 700; color: #66BB6A;">{total_empresas}</div>
                    <div class="label" style="color: #a0a0b0; font-size: 0.8rem;">Empresas Analisadas</div>
                </div>
                <div class="stat-card" style="border-left: 4px solid #66BB6A;">
                    <div style="font-size: 2rem; margin-bottom: 10px;">✅</div>
                    <div class="value" style="font-size: 2rem; font-weight: 700; color: #66BB6A;">{total_verde}</div>
                    <div class="label" style="color: #a0a0b0; font-size: 0.8rem;">Elegiveis Verde</div>
                </div>
                <div class="stat-card" style="border-left: 4px solid #FF9800;">
                    <div style="font-size: 2rem; margin-bottom: 10px;">📋</div>
                    <div class="value" style="font-size: 2rem; font-weight: 700; color: #FFB74D;">{total_kpis_pendentes}</div>
                    <div class="label" style="color: #a0a0b0; font-size: 0.8rem;">KPIs Pendentes</div>
                </div>
                <div class="stat-card" style="border-left: 4px solid #00BCD4;">
                    <div style="font-size: 2rem; margin-bottom: 10px;">📊</div>
                    <div class="value" style="font-size: 2rem; font-weight: 700; color: #4DD0E1;">4</div>
                    <div class="label" style="color: #a0a0b0; font-size: 0.8rem;">Setores Cobertos</div>
                </div>
            </div>

            <!-- Graficos TSB -->
            <div class="grid-2" style="margin-bottom: 25px;">
                <div class="chart-card">
                    <h3 style="margin-bottom: 15px;">📊 Classificacao TSB</h3>
                    <div class="chart-container" style="height: 280px;">
                        <canvas id="chartTSBClassificacao"></canvas>
                    </div>
                </div>
                <div class="chart-card">
                    <h3 style="margin-bottom: 15px;">🏭 Empresas por Setor</h3>
                    <div class="chart-container" style="height: 280px;">
                        <canvas id="chartTSBSetores"></canvas>
                    </div>
                </div>
            </div>

            <!-- Tabela de Empresas TSB -->
            <div class="chart-card" style="margin-bottom: 25px;">
                <h3 style="margin-bottom: 15px;">🏢 Empresas Classificadas pela TSB</h3>
                <p style="color: #a0a0b0; margin-bottom: 15px; font-size: 0.85rem;">
                    Empresas da carteira com classificacao segundo a Taxonomia Sustentavel Brasileira
                </p>
                <div style="overflow-x: auto;">
                    <table style="width: 100%; border-collapse: collapse;">
                        <thead>
                            <tr style="background: rgba(255,255,255,0.03);">
                                <th style="padding: 12px; text-align: left; border-bottom: 1px solid #2a2a4a; color: #a0a0b0; font-size: 0.75rem; text-transform: uppercase;">Empresa</th>
                                <th style="padding: 12px; text-align: left; border-bottom: 1px solid #2a2a4a; color: #a0a0b0; font-size: 0.75rem; text-transform: uppercase;">CNPJ</th>
                                <th style="padding: 12px; text-align: left; border-bottom: 1px solid #2a2a4a; color: #a0a0b0; font-size: 0.75rem; text-transform: uppercase;">Setor TSB</th>
                                <th style="padding: 12px; text-align: left; border-bottom: 1px solid #2a2a4a; color: #a0a0b0; font-size: 0.75rem; text-transform: uppercase;">Classificacao</th>
                                <th style="padding: 12px; text-align: left; border-bottom: 1px solid #2a2a4a; color: #a0a0b0; font-size: 0.75rem; text-transform: uppercase;">Score</th>
                                <th style="padding: 12px; text-align: left; border-bottom: 1px solid #2a2a4a; color: #a0a0b0; font-size: 0.75rem; text-transform: uppercase;">Titulos</th>
                            </tr>
                        </thead>
                        <tbody>{linhas_empresas}
                        </tbody>
                    </table>
                </div>
            </div>

            <!-- KPIs por Setor -->
            <h3 style="margin: 30px 0 20px; font-size: 1.2rem;">📈 KPIs Obrigatorios por Setor</h3>
            {kpis_html}
        </div>
"""
    return aba_tsb


def gerar_script_graficos_tsb(dados_tsb):
    """Gera o JavaScript para os graficos TSB"""

    empresas = dados_tsb.get('empresas', [])
    resumo = dados_tsb.get('resumo_dashboard', {})

    por_classificacao = resumo.get('por_classificacao', {})
    por_setor = resumo.get('por_setor', {})

    # Se não tiver resumo, calcular
    if not por_classificacao:
        por_classificacao = {}
        for emp in empresas:
            cls = emp.get('classificacao', 'PENDENTE')
            por_classificacao[cls] = por_classificacao.get(cls, 0) + 1

    if not por_setor:
        por_setor = {}
        for emp in empresas:
            setor = emp.get('setor_tsb', 'Outros')
            por_setor[setor] = por_setor.get(setor, 0) + 1

    script = f"""
        // Graficos TSB
        function inicializarGraficosTSB() {{
            // Grafico de Classificacao
            const ctxClass = document.getElementById('chartTSBClassificacao');
            if (ctxClass) {{
                new Chart(ctxClass, {{
                    type: 'doughnut',
                    data: {{
                        labels: {json.dumps(list(por_classificacao.keys()))},
                        datasets: [{{
                            data: {json.dumps(list(por_classificacao.values()))},
                            backgroundColor: ['#4CAF50', '#8BC34A', '#FF9800', '#9E9E9E'],
                            borderWidth: 2,
                            borderColor: '#0a0a1a'
                        }}]
                    }},
                    options: {{
                        responsive: true,
                        maintainAspectRatio: false,
                        plugins: {{
                            legend: {{
                                position: 'bottom',
                                labels: {{ color: '#fff', padding: 15 }}
                            }}
                        }}
                    }}
                }});
            }}

            // Grafico de Setores
            const ctxSetores = document.getElementById('chartTSBSetores');
            if (ctxSetores) {{
                new Chart(ctxSetores, {{
                    type: 'bar',
                    data: {{
                        labels: {json.dumps(list(por_setor.keys()))},
                        datasets: [{{
                            label: 'Empresas',
                            data: {json.dumps(list(por_setor.values()))},
                            backgroundColor: ['#FF9800', '#2196F3', '#9C27B0', '#009688'],
                            borderRadius: 8
                        }}]
                    }},
                    options: {{
                        indexAxis: 'y',
                        responsive: true,
                        maintainAspectRatio: false,
                        plugins: {{ legend: {{ display: false }} }},
                        scales: {{
                            x: {{ grid: {{ color: 'rgba(255,255,255,0.1)' }}, ticks: {{ color: '#a0a0b0' }} }},
                            y: {{ grid: {{ display: false }}, ticks: {{ color: '#a0a0b0' }} }}
                        }}
                    }}
                }});
            }}
        }}

        // Inicializar graficos TSB quando a aba for aberta
        const originalShowTab = showTab;
        showTab = function(tabId) {{
            originalShowTab(tabId);
            if (tabId === 'tsb') {{
                setTimeout(inicializarGraficosTSB, 100);
            }}
        }};
"""
    return script


def adicionar_aba_tsb():
    """Adiciona a aba TSB ao dashboard"""

    dashboard_file = os.path.join(DASHBOARD_DIR, 'dashboard_anbima_real.html')

    if not os.path.exists(dashboard_file):
        print(f"Dashboard nao encontrado: {dashboard_file}")
        return False

    # Carregar dados TSB
    dados_tsb = carregar_dados_tsb()
    if not dados_tsb:
        print("Dados TSB nao encontrados")
        return False

    print(f"Dados TSB carregados: {len(dados_tsb.get('empresas', []))} empresas")

    # Ler dashboard existente
    with open(dashboard_file, 'r', encoding='utf-8') as f:
        html = f.read()

    # Verificar se ja tem aba TSB
    if 'id="tsb"' in html:
        print("Aba TSB ja existe no dashboard")
        return True

    # 1. Adicionar item de navegacao TSB
    # Procurar o ultimo nav-section antes de fechar a sidebar
    nav_tsb = """
        <div class="nav-section">
            <div class="nav-section-title">Sustentabilidade</div>
            <div class="nav-item" onclick="showTab('tsb')">
                <span class="icon">🌿</span> TSB - Empresas
            </div>
        </div>
    </div>"""

    # Substituir o fechamento da sidebar
    html = html.replace('    </div>\n\n    <!-- Main Content -->', nav_tsb + '\n\n    <!-- Main Content -->')

    # 2. Adicionar conteudo da aba TSB antes do fechamento do main-content
    aba_tsb_html = gerar_conteudo_aba_tsb(dados_tsb)

    # Encontrar o ultimo tab-content e adicionar depois
    # Procurar pelo padrao </div>\n    </div>\n</body>
    pattern = r'(</div>\s*</div>\s*<script>)'

    if re.search(pattern, html):
        html = re.sub(pattern, aba_tsb_html + r'\n    </div>\n\n    <script>', html)
    else:
        # Tentar outro padrao - antes do </body>
        html = html.replace('</body>', aba_tsb_html + '\n    </div>\n</body>')

    # 3. Adicionar script dos graficos TSB
    script_tsb = gerar_script_graficos_tsb(dados_tsb)

    # Adicionar antes do </script> final
    html = html.replace('</script>\n</body>', script_tsb + '\n    </script>\n</body>')

    # Salvar dashboard atualizado
    with open(dashboard_file, 'w', encoding='utf-8') as f:
        f.write(html)

    print(f"\nAba TSB adicionada ao dashboard!")
    print(f"Dashboard: {dashboard_file}")
    print(f"Empresas TSB: {len(dados_tsb.get('empresas', []))}")

    return True


if __name__ == '__main__':
    print("=" * 60)
    print("Adicionando Aba TSB ao Dashboard")
    print("=" * 60)

    # Primeiro regenerar o dashboard base
    print("\n1. Regenerando dashboard base...")
    os.system(f'py "{os.path.join(BASE_DIR, "etl", "anbima", "gerar_dashboard_completo.py")}"')

    print("\n2. Adicionando aba TSB...")
    adicionar_aba_tsb()

    print("\n" + "=" * 60)
    print("Concluido!")
    print("=" * 60)
