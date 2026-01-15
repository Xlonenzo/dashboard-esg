"""
Atualiza os dados TSB no dashboard_anbima_real.html
===================================================
Este script injeta os dados reais do arquivo tsb_kpis_empresas.json
no dashboard existente de forma segura.
"""

import os
import json
import re

# Diretorios
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


def atualizar_dashboard():
    """Atualiza o dashboard com dados TSB reais"""

    dashboard_file = os.path.join(DASHBOARD_DIR, 'dashboard_anbima_real.html')

    if not os.path.exists(dashboard_file):
        print(f"Dashboard nao encontrado: {dashboard_file}")
        return False

    # Carregar dados TSB
    dados_tsb = carregar_dados_tsb()
    if not dados_tsb:
        print("Dados TSB nao encontrados")
        return False

    # Preparar dados para injecao - formato simplificado
    empresas_tsb = dados_tsb.get('empresas', [])
    resumo = dados_tsb.get('resumo_dashboard', {})

    # Converter empresas para formato simples (evitar dados complexos)
    empresas_simples = []
    for emp in empresas_tsb:
        empresas_simples.append({
            'nome': emp.get('emissor', ''),
            'cnpj': emp.get('cnpj', ''),
            'setor': emp.get('setor_tsb', ''),
            'classificacao': emp.get('classificacao', 'PENDENTE'),
            'score': emp.get('score', 0),
            'titulos': emp.get('titulos', 0)
        })

    # Ler dashboard existente
    with open(dashboard_file, 'r', encoding='utf-8') as f:
        html = f.read()

    # Encontrar a funcao processarDadosTSB e modificar para usar dados reais
    # Vamos substituir o inicio da funcao

    # Dados em formato JSON compacto (uma linha)
    empresas_json = json.dumps(empresas_simples, ensure_ascii=False)

    # Nova funcao processarDadosTSB
    nova_funcao = f'''// Processar dados TSB das debentures
        function processarDadosTSB() {{
            // Dados TSB reais injetados
            const empresasTSBReais = {empresas_json};

            if (empresasTSBReais && empresasTSBReais.length > 0) {{
                empresasTSB = empresasTSBReais.map(e => ({{
                    nome: e.nome,
                    cnpj: e.cnpj,
                    cnae: '',
                    setor: e.setor,
                    classificacao: e.classificacao,
                    score: e.score,
                    titulos: e.titulos
                }}));
                empresasTSBFiltradas = [...empresasTSB];
                atualizarEstatisticasTSB();
                criarGraficosTSB();
                renderizarTabelaTSB();
                console.log('Dados TSB reais:', empresasTSB.length);
                return;
            }}

            // Fallback: processar dados das debentures
            if (!D.debentures || D.debentures.length === 0) return;'''

    # Substituir a funcao processarDadosTSB
    pattern = r'// Processar dados TSB das debentures\s+function processarDadosTSB\(\) \{\s+if \(!D\.debentures \|\| D\.debentures\.length === 0\) return;'

    if re.search(pattern, html):
        html = re.sub(pattern, nova_funcao, html)
        print("Funcao processarDadosTSB atualizada com dados reais")
    else:
        print("AVISO: Padrao nao encontrado, tentando abordagem alternativa...")
        # Tentar outro padrao
        pattern2 = r'(// Processar dados TSB das debentures\s+function processarDadosTSB\(\) \{)'
        if re.search(pattern2, html):
            # Injetar dados logo apos a abertura da funcao
            inject = f'''// Processar dados TSB das debentures
        function processarDadosTSB() {{
            // Dados TSB reais
            const empresasTSBReais = {empresas_json};
            if (empresasTSBReais.length > 0) {{
                empresasTSB = empresasTSBReais;
                empresasTSBFiltradas = [...empresasTSB];
                atualizarEstatisticasTSB();
                criarGraficosTSB();
                renderizarTabelaTSB();
                return;
            }}
'''
            html = re.sub(pattern2, inject, html)
            print("Dados TSB injetados (metodo alternativo)")

    # Salvar dashboard atualizado
    with open(dashboard_file, 'w', encoding='utf-8') as f:
        f.write(html)

    print(f"\nDashboard atualizado: {dashboard_file}")
    print(f"Empresas TSB: {len(empresas_simples)}")
    for emp in empresas_simples:
        print(f"  - {emp['nome']}: {emp['classificacao']} (Score: {emp['score']})")

    return True


if __name__ == '__main__':
    print("=" * 60)
    print("Atualizando Dashboard com Dados TSB Reais")
    print("=" * 60)
    atualizar_dashboard()
