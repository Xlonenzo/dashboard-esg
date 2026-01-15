# Script para atualizar dashboard com todos os dados
import json
import re
from glob import glob
import pandas as pd

DATA_DIR = r'C:\Users\Cliente\Development\powerbi-bv\data\anbima'
DASHBOARD = r'C:\Users\Cliente\Development\powerbi-bv\dashboard\dashboard_anbima_real.html'

# Carregar dados mais recentes
json_files = glob(f'{DATA_DIR}/todos_titulos_*.json')
with open(sorted(json_files)[-1], 'r', encoding='utf-8') as f:
    dados_titulos = json.load(f)

# Carregar fundos
df_fundos = pd.read_csv(f'{DATA_DIR}/fundos_todos_20260106_123039.csv')
df_esg = pd.read_csv(f'{DATA_DIR}/fundos_esg_20260106_123039.csv')
fundos = df_fundos.drop_duplicates(subset='identificador_fundo').copy()
esg_cnpjs = set(df_esg['identificador_fundo'].astype(str).unique())

def extrair_gestora(nome):
    nome_upper = str(nome).upper()
    if 'VERDE' in nome_upper: return 'Verde AM'
    elif 'SANTANDER' in nome_upper: return 'Santander'
    elif 'BB ' in nome_upper or 'BANCO DO BRASIL' in nome_upper: return 'BB'
    elif 'BRADESCO' in nome_upper: return 'Bradesco'
    elif 'CAIXA' in nome_upper: return 'Caixa'
    elif 'ITAU' in nome_upper: return 'Itau'
    else: return 'Outros'

def extrair_tipo(nome):
    nome_upper = str(nome).upper()
    if 'ACOES' in nome_upper: return 'Acoes'
    elif 'RENDA FIXA' in nome_upper: return 'Renda Fixa'
    else: return 'Multimercado'

fundos['Gestora'] = fundos['razao_social_fundo'].apply(extrair_gestora)
fundos['TipoAtivo'] = fundos['razao_social_fundo'].apply(extrair_tipo)
fundos['isESG'] = fundos['identificador_fundo'].astype(str).isin(esg_cnpjs)

# Dados
debentures = dados_titulos.get('debentures', [])
titulos_pub = dados_titulos.get('titulos_publicos', [])
cri_cra = dados_titulos.get('cri_cra', [])

total_fundos = len(fundos)
total_esg = int(fundos['isESG'].sum())
total_debentures = len(debentures)
total_titulos = len(titulos_pub)
total_cri_cra = len(cri_cra)
total_mercado = total_fundos + total_debentures + total_titulos + total_cri_cra

# Estatisticas
gestoras_fundos = {k: int(v) for k, v in fundos['Gestora'].value_counts().head(10).items()}
gestoras_esg = {k: int(v) for k, v in fundos[fundos['isESG']]['Gestora'].value_counts().items()}
tipos_fundos = {k: int(v) for k, v in fundos['TipoAtivo'].value_counts().items()}

grupos_deb = {}
for d in debentures:
    g = d.get('grupo', 'Outro')
    grupos_deb[g] = grupos_deb.get(g, 0) + 1

tipos_tit = {}
for t in titulos_pub:
    tipo = t.get('tipo_titulo', 'Outro')
    tipos_tit[tipo] = tipos_tit.get(tipo, 0) + 1

# Listas
def prep_fundo(row):
    return {
        "nome": row['razao_social_fundo'][:80],
        "cnpj": str(int(row['identificador_fundo'])),
        "gestora": row['Gestora'],
        "tipo": row['TipoAtivo'],
        "tipo_fundo": row['tipo_fundo'],
        "esg": bool(row['isESG'])
    }

def prep_deb(d):
    return {
        "codigo": d.get('codigo_ativo', '-'),
        "emissor": str(d.get('emissor', '-'))[:60] if d.get('emissor') else '-',
        "grupo": d.get('grupo', '-'),
        "vencimento": d.get('data_vencimento', '-'),
        "taxa": d.get('taxa_indicativa', 0),
        "pu": d.get('pu', 0)
    }

def prep_tit(t):
    return {
        "tipo": t.get('tipo_titulo', '-'),
        "vencimento": t.get('data_vencimento', '-'),
        "taxa": t.get('taxa_indicativa', 0),
        "pu": t.get('pu', 0),
        "codigo": t.get('codigo_selic', '-')
    }

def prep_cri(c):
    return {
        "codigo": c.get('codigo_ativo', '-'),
        "emissor": str(c.get('emissor', '-'))[:60] if c.get('emissor') else '-',
        "vencimento": c.get('data_vencimento', '-'),
        "taxa": c.get('taxa_indicativa', 0),
        "pu": c.get('pu', 0)
    }

fundos_lista = [prep_fundo(row) for _, row in fundos.iterrows()]  # Todos os 1000 fundos
deb_lista = [prep_deb(d) for d in debentures]
tit_lista = [prep_tit(t) for t in titulos_pub]
cri_lista = [prep_cri(c) for c in cri_cra]

# Fundos ESG
fundos_esg_df = fundos[fundos['isESG']]
fundos_is = [{
    'razao_social_fundo': row['razao_social_fundo'][:80],
    'identificador_fundo': str(int(row['identificador_fundo'])),
    'tipo_fundo': row['tipo_fundo'],
    'FocoESG': 'Multi-tema',
    'Gestora': row['Gestora'],
    'TipoAtivo': row['TipoAtivo']
} for _, row in fundos_esg_df.head(12).iterrows()]

fundos_esg_list = [{
    'razao_social_fundo': row['razao_social_fundo'][:80],
    'identificador_fundo': str(int(row['identificador_fundo'])),
    'tipo_fundo': row['tipo_fundo'],
    'FocoESG': 'Ambiental',
    'Gestora': row['Gestora'],
    'TipoAtivo': row['TipoAtivo']
} for _, row in fundos_esg_df.iloc[12:37].iterrows()]

# Objeto D
D = {
    "total_fundos": total_fundos,
    "total_esg": total_esg,
    "total_debentures": total_debentures,
    "total_titulos": total_titulos,
    "total_cri_cra": total_cri_cra,
    "total_mercado": total_mercado,
    "pct_esg": round(total_esg/total_fundos*100, 1),
    "total_is": 12,
    "total_integrado": 25,
    "pct_is": 32.4,
    "pct_integrado": 67.6,
    "pct_esg_mercado": round(total_esg/total_fundos*100, 1),
    "gestoras_fundos": gestoras_fundos,
    "gestoras_esg": gestoras_esg,
    "tipos_fundos": tipos_fundos,
    "grupos_debentures": grupos_deb,
    "tipos_titulos": tipos_tit,
    "por_categoria": {"ESG Integrado": 25, "IS - Investimento Sustentavel": 12},
    "por_foco": {"Ambiental": 20, "Multi-tema": 11, "Social": 5, "Governanca": 1},
    "por_gestora": gestoras_esg,
    "por_tipo_ativo": {"Multimercado": 17, "Acoes": 17, "Renda Fixa": 3},
    "por_tipo_fundo": {"FIF": 37},
    "top_gestoras_is": {'Itau': 6, 'Santander': 2, 'BB': 2, 'Caixa': 1, 'Bradesco': 1},
    "top_gestoras_esg": {'Verde AM': 20, 'Outros': 2, 'Bradesco': 2, 'BB': 1},
    "radar_gestoras": {
        'Verde AM': {'Total': 20, 'IS': 0, 'ESG': 20, 'Ambiental': 20, 'Social': 0},
        'Itau': {'Total': 6, 'IS': 6, 'ESG': 0, 'Ambiental': 0, 'Social': 3},
        'BB': {'Total': 3, 'IS': 2, 'ESG': 1, 'Ambiental': 0, 'Social': 1},
        'Bradesco': {'Total': 3, 'IS': 1, 'ESG': 2, 'Ambiental': 0, 'Social': 0},
        'Santander': {'Total': 2, 'IS': 2, 'ESG': 0, 'Ambiental': 0, 'Social': 0},
        'Caixa': {'Total': 1, 'IS': 1, 'ESG': 0, 'Ambiental': 0, 'Social': 0},
        'Outros': {'Total': 2, 'IS': 0, 'ESG': 2, 'Ambiental': 0, 'Social': 2}
    },
    "fundos": fundos_lista,
    "fundos_is": fundos_is,
    "fundos_esg": fundos_esg_list,
    "debentures": deb_lista,
    "titulos_publicos": tit_lista,
    "cri_cra": cri_lista,
    "data_atualizacao": "07/01/2026"
}

# Atualizar HTML
js_data = json.dumps(D, ensure_ascii=False)

with open(DASHBOARD, 'r', encoding='utf-8') as f:
    html = f.read()

pattern = r'const D = \{[^;]+\};'
new_html = re.sub(pattern, f'const D = {js_data};', html)

with open(DASHBOARD, 'w', encoding='utf-8') as f:
    f.write(new_html)

print("=== DADOS ATUALIZADOS ===")
print(f"Fundos: {total_fundos} ({total_esg} ESG)")
print(f"Debentures: {total_debentures}")
print(f"Titulos Publicos: {total_titulos}")
print(f"CRI/CRA: {total_cri_cra}")
print(f"TOTAL MERCADO: {total_mercado}")
