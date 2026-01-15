# Script para preparar dados de todos os 1000 fundos para o dashboard
import pandas as pd
import json
import ast
from glob import glob
from datetime import datetime

DATA_DIR = r'C:\Users\Cliente\Development\powerbi-bv\data\anbima'

# Carregar dados
csv_files = glob(f'{DATA_DIR}/fundos_todos_*.csv')
df = pd.read_csv(sorted(csv_files)[-1])

# Remover duplicatas
df_unique = df.drop_duplicates(subset='identificador_fundo').copy()

# Extrair categoria do campo classes
def extrair_categoria(classes_str):
    try:
        if pd.isna(classes_str):
            return 'Outros'
        classes = ast.literal_eval(classes_str)
        if classes and len(classes) > 0:
            cat = classes[0].get('nivel1_categoria', 'Outros')
            # Limpar encoding
            cat = cat.replace('Ã§Ãµ', 'co').replace('Ã£', 'a').replace('Ã©', 'e').replace('Ãª', 'e')
            cat = cat.replace('ç', 'c').replace('ã', 'a').replace('õ', 'o').replace('é', 'e')
            if 'Renda' in cat: return 'Renda Fixa'
            if 'Multi' in cat: return 'Multimercado'
            if 'es' in cat or 'Ac' in cat: return 'Acoes'
            if 'Prev' in cat: return 'Previdencia'
            if 'Camb' in cat: return 'Cambial'
            return cat
    except:
        pass
    return 'Outros'

def extrair_gestora(nome):
    if pd.isna(nome):
        return 'Outros'
    nome_upper = str(nome).upper()
    gestoras = [
        ('VERDE', 'Verde AM'),
        ('SANTANDER', 'Santander'),
        ('BRADESCO', 'Bradesco'),
        ('ITAU', 'Itau'),
        ('ITAÚ', 'Itau'),
        ('BB ', 'BB'),
        ('BANCO DO BRASIL', 'BB'),
        ('CAIXA', 'Caixa'),
        ('BTG', 'BTG Pactual'),
        ('XP ', 'XP'),
        ('SAFRA', 'Safra'),
        ('CREDIT SUISSE', 'Credit Suisse'),
        ('WESTERN', 'Western Asset'),
        ('ARX', 'ARX'),
        ('KINEA', 'Kinea'),
        ('VINCI', 'Vinci Partners'),
        ('MODAL', 'Modal'),
        ('OPPORTUNITY', 'Opportunity'),
        ('JGP', 'JGP'),
        ('SPX', 'SPX'),
        ('ADAM', 'Adam Capital'),
        ('BAHIA', 'Bahia AM'),
        ('KAPITALO', 'Kapitalo'),
        ('LEGACY', 'Legacy'),
        ('IBIUNA', 'Ibiuna'),
    ]
    for termo, gestora in gestoras:
        if termo in nome_upper:
            return gestora
    return 'Outros'

def limpar_nome(nome):
    if pd.isna(nome):
        return ''
    nome = str(nome)
    # Limpar encoding
    nome = nome.replace('Ã§', 'c').replace('Ã£', 'a').replace('Ãµ', 'o').replace('Ã©', 'e')
    nome = nome.replace('Ãª', 'e').replace('Ã', 'A').replace('Ã¡', 'a').replace('Ã³', 'o')
    nome = nome.replace('Ãº', 'u').replace('Â', '').replace('�', '')
    return nome[:100]  # Limitar tamanho

# Processar dados
df_unique['Categoria'] = df_unique['classes'].apply(extrair_categoria)
df_unique['Gestora'] = df_unique['razao_social_fundo'].apply(extrair_gestora)
df_unique['NomeLimpo'] = df_unique['razao_social_fundo'].apply(limpar_nome)
df_unique['isESG'] = df_unique['CategoriaESG'].notna()
df_unique['TipoESG'] = df_unique['CategoriaESG'].fillna('')
df_unique['FocoESG'] = df_unique['FocoESG'].fillna('')

# Preparar lista de fundos para JSON
fundos_lista = []
for _, row in df_unique.iterrows():
    fundo = {
        'id': str(row['codigo_fundo']),
        'cnpj': str(int(row['identificador_fundo'])) if pd.notna(row['identificador_fundo']) else '',
        'nome': row['NomeLimpo'],
        'categoria': row['Categoria'],
        'gestora': row['Gestora'],
        'tipo': row['tipo_fundo'],
        'esg': bool(row['isESG']),
        'tipoESG': row['TipoESG'],
        'focoESG': row['FocoESG'],
        'dataVigencia': str(row['data_vigencia'])[:10] if pd.notna(row['data_vigencia']) else ''
    }
    fundos_lista.append(fundo)

# Estatisticas
stats = {
    'total': len(fundos_lista),
    'por_categoria': df_unique['Categoria'].value_counts().to_dict(),
    'por_gestora': df_unique['Gestora'].value_counts().head(20).to_dict(),
    'total_esg': int(df_unique['isESG'].sum()),
    'data_atualizacao': datetime.now().strftime('%d/%m/%Y %H:%M')
}

# Salvar JSON
output = {
    'stats': stats,
    'fundos': fundos_lista
}

output_file = f'{DATA_DIR}/fundos_completo.json'
with open(output_file, 'w', encoding='utf-8') as f:
    json.dump(output, f, ensure_ascii=False, indent=2)

print(f'=== FUNDOS PREPARADOS ===')
print(f'Total: {stats["total"]}')
print(f'ESG: {stats["total_esg"]}')
print(f'Arquivo: {output_file}')
print()
print('Por Categoria:')
for cat, count in stats['por_categoria'].items():
    print(f'  {cat}: {count}')
print()
print('Top 10 Gestoras:')
for gest, count in list(stats['por_gestora'].items())[:10]:
    print(f'  {gest}: {count}')
