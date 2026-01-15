# Sistema de Mapeamento TSB - Taxonomia Sustentavel Brasileira
# Integra CNPJ/CNAE com criterios da TSB para classificacao de sustentabilidade

import json
import requests
import time
import re
from datetime import datetime

# ===========================================
# BASE DE DADOS TSB - Criterios por CNAE
# ===========================================

# Setores elegíveis TSB com CNAEs correspondentes
TSB_SETORES = {
    "energia": {
        "nome": "Energia",
        "descricao": "Geracao, transmissao e distribuicao de energia",
        "cnaes": ["35.11-5", "35.12-3", "35.13-1", "35.14-0", "35.21-2", "35.22-0", "35.23-9"],
        "criterios": [
            "Energia renovavel (solar, eolica, hidro, biomassa)",
            "Eficiencia energetica",
            "Transmissao e distribuicao para fontes limpas"
        ],
        "elegivel_verde": True
    },
    "transporte": {
        "nome": "Transporte",
        "descricao": "Transporte terrestre, ferroviario, aquaviario e aereo",
        "cnaes": ["49.11-6", "49.12-4", "49.21-3", "49.22-1", "49.23-0", "49.24-8", "49.29-9",
                  "50.11-4", "50.12-2", "50.21-1", "50.22-0", "50.30-1", "51.11-1", "51.12-9"],
        "criterios": [
            "Transporte de baixa emissao",
            "Infraestrutura para mobilidade sustentavel",
            "Logistica verde"
        ],
        "elegivel_verde": True
    },
    "construcao": {
        "nome": "Construcao",
        "descricao": "Construcao civil e infraestrutura",
        "cnaes": ["41.10-7", "41.20-4", "42.11-1", "42.12-0", "42.13-8", "42.21-9", "42.22-7",
                  "42.23-5", "42.91-0", "42.92-8", "42.99-5", "43.11-8", "43.12-6", "43.13-4"],
        "criterios": [
            "Construcao sustentavel (certificacao LEED, AQUA)",
            "Retrofit para eficiencia energetica",
            "Infraestrutura verde"
        ],
        "elegivel_verde": True
    },
    "saneamento": {
        "nome": "Saneamento e Residuos",
        "descricao": "Agua, esgoto e gestao de residuos",
        "cnaes": ["36.11-0", "36.12-8", "36.13-6", "37.01-1", "37.02-9", "38.11-4", "38.12-2",
                  "38.21-1", "38.22-0", "39.00-5"],
        "criterios": [
            "Tratamento de agua e esgoto",
            "Reciclagem e economia circular",
            "Gestao sustentavel de residuos"
        ],
        "elegivel_verde": True
    },
    "agropecuaria": {
        "nome": "Agropecuaria",
        "descricao": "Agricultura, pecuaria e silvicultura",
        "cnaes": ["01.11-3", "01.12-1", "01.13-0", "01.14-8", "01.15-6", "01.16-4", "01.19-9",
                  "01.21-1", "01.22-9", "01.31-8", "01.32-6", "01.33-4", "01.34-2", "01.39-3",
                  "01.41-5", "01.42-3", "01.51-2", "01.52-0", "01.53-9", "01.54-7", "01.55-5",
                  "02.10-1", "02.20-9", "02.30-6"],
        "criterios": [
            "Agricultura de baixo carbono",
            "Restauracao florestal",
            "Sistemas agroflorestais",
            "Pecuaria sustentavel"
        ],
        "elegivel_verde": True,
        "salvaguardas": ["Sem desmatamento apos TSB", "CAR regularizado"]
    },
    "industria": {
        "nome": "Industria de Transformacao",
        "descricao": "Manufatura e processamento industrial",
        "cnaes": ["10.", "11.", "12.", "13.", "14.", "15.", "16.", "17.", "18.", "19.",
                  "20.", "21.", "22.", "23.", "24.", "25.", "26.", "27.", "28.", "29.",
                  "30.", "31.", "32.", "33."],
        "criterios": [
            "Producao limpa",
            "Eficiencia de recursos",
            "Descarbonizacao industrial"
        ],
        "elegivel_verde": False,
        "nota": "Avaliacao caso a caso por subsetor"
    },
    "financeiro": {
        "nome": "Servicos Financeiros",
        "descricao": "Bancos, seguros e mercado de capitais",
        "cnaes": ["64.", "65.", "66."],
        "criterios": [
            "Financiamento verde",
            "Titulos sustentaveis",
            "Gestao de riscos climaticos"
        ],
        "elegivel_verde": True,
        "nota": "Elegivel se recursos destinados a atividades verdes"
    },
    "telecomunicacoes": {
        "nome": "Telecomunicacoes",
        "descricao": "Servicos de comunicacao e TI",
        "cnaes": ["61.10-8", "61.20-5", "61.30-2", "61.41-8", "61.42-6", "61.43-4", "61.90-6"],
        "criterios": [
            "Infraestrutura digital sustentavel",
            "Eficiencia energetica em data centers"
        ],
        "elegivel_verde": True
    }
}

# Mapeamento CNAE -> Setor TSB
def get_setor_tsb(cnae):
    """Retorna o setor TSB correspondente ao CNAE"""
    if not cnae:
        return None

    # Converter para string se necessario
    cnae = str(cnae)
    cnae_limpo = cnae.replace(".", "").replace("-", "").replace("/", "")[:4]
    cnae_grupo = cnae[:5] if len(cnae) >= 5 else cnae

    for setor_id, setor in TSB_SETORES.items():
        for cnae_ref in setor["cnaes"]:
            cnae_ref_limpo = cnae_ref.replace(".", "").replace("-", "")
            # Verificar correspondencia parcial
            if cnae_limpo.startswith(cnae_ref_limpo[:2]) or cnae_grupo.startswith(cnae_ref[:2]):
                return setor_id
    return None

# ===========================================
# API DE CONSULTA CNPJ
# ===========================================

def consultar_cnpj_brasilapi(cnpj):
    """Consulta CNPJ usando BrasilAPI (gratuita)"""
    cnpj_limpo = re.sub(r'\D', '', str(cnpj))
    if len(cnpj_limpo) < 14:
        cnpj_limpo = cnpj_limpo.zfill(14)

    url = f"https://brasilapi.com.br/api/cnpj/v1/{cnpj_limpo}"

    try:
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            dados = response.json()
            return {
                "cnpj": cnpj_limpo,
                "razao_social": dados.get("razao_social", ""),
                "nome_fantasia": dados.get("nome_fantasia", ""),
                "cnae_principal": dados.get("cnae_fiscal", ""),
                "cnae_descricao": dados.get("cnae_fiscal_descricao", ""),
                "cnaes_secundarios": dados.get("cnaes_secundarios", []),
                "situacao": dados.get("situacao_cadastral", ""),
                "uf": dados.get("uf", ""),
                "municipio": dados.get("municipio", ""),
                "natureza_juridica": dados.get("natureza_juridica", ""),
                "porte": dados.get("porte", ""),
                "capital_social": dados.get("capital_social", 0),
                "data_consulta": datetime.now().isoformat()
            }
        elif response.status_code == 404:
            return {"erro": "CNPJ nao encontrado", "cnpj": cnpj_limpo}
        else:
            return {"erro": f"Erro na API: {response.status_code}", "cnpj": cnpj_limpo}
    except Exception as e:
        return {"erro": str(e), "cnpj": cnpj_limpo}

def consultar_cnpj_receitaws(cnpj):
    """Consulta CNPJ usando ReceitaWS (backup)"""
    cnpj_limpo = re.sub(r'\D', '', str(cnpj))
    if len(cnpj_limpo) < 14:
        cnpj_limpo = cnpj_limpo.zfill(14)

    url = f"https://receitaws.com.br/v1/cnpj/{cnpj_limpo}"

    try:
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            dados = response.json()
            if dados.get("status") == "ERROR":
                return {"erro": dados.get("message", "Erro"), "cnpj": cnpj_limpo}

            # Extrair CNAEs secundarios
            cnaes_sec = []
            for ativ in dados.get("atividades_secundarias", []):
                cnaes_sec.append({
                    "codigo": ativ.get("code", ""),
                    "descricao": ativ.get("text", "")
                })

            return {
                "cnpj": cnpj_limpo,
                "razao_social": dados.get("nome", ""),
                "nome_fantasia": dados.get("fantasia", ""),
                "cnae_principal": dados.get("atividade_principal", [{}])[0].get("code", ""),
                "cnae_descricao": dados.get("atividade_principal", [{}])[0].get("text", ""),
                "cnaes_secundarios": cnaes_sec,
                "situacao": dados.get("situacao", ""),
                "uf": dados.get("uf", ""),
                "municipio": dados.get("municipio", ""),
                "natureza_juridica": dados.get("natureza_juridica", ""),
                "porte": dados.get("porte", ""),
                "capital_social": float(dados.get("capital_social", "0").replace(".", "").replace(",", ".")) if dados.get("capital_social") else 0,
                "data_consulta": datetime.now().isoformat()
            }
        else:
            return {"erro": f"Erro na API: {response.status_code}", "cnpj": cnpj_limpo}
    except Exception as e:
        return {"erro": str(e), "cnpj": cnpj_limpo}

def consultar_cnpj(cnpj):
    """Consulta CNPJ usando APIs disponiveis"""
    # Tentar BrasilAPI primeiro
    resultado = consultar_cnpj_brasilapi(cnpj)

    # Se falhar, tentar ReceitaWS
    if resultado.get("erro"):
        time.sleep(1)  # Rate limiting
        resultado = consultar_cnpj_receitaws(cnpj)

    return resultado

# ===========================================
# CLASSIFICACAO TSB
# ===========================================

def classificar_tsb(dados_empresa):
    """Classifica uma empresa segundo criterios TSB"""
    if dados_empresa.get("erro"):
        return {
            "elegivel": False,
            "setor_tsb": None,
            "classificacao": "NAO_CLASSIFICAVEL",
            "motivo": dados_empresa.get("erro"),
            "score": 0
        }

    cnae = dados_empresa.get("cnae_principal", "")
    setor_id = get_setor_tsb(cnae)

    if not setor_id:
        return {
            "elegivel": False,
            "setor_tsb": None,
            "setor_nome": "Nao Mapeado",
            "classificacao": "FORA_ESCOPO",
            "motivo": f"CNAE {cnae} nao mapeado na TSB",
            "score": 0,
            "criterios": [],
            "recomendacao": "Verificar CNAEs secundarios ou aguardar expansao da TSB"
        }

    setor = TSB_SETORES[setor_id]

    # Calcular score baseado em criterios
    score = 50  # Base
    if setor.get("elegivel_verde"):
        score += 30

    # Verificar CNAEs secundarios para bonus
    cnaes_sec = dados_empresa.get("cnaes_secundarios", [])
    setores_secundarios = []
    for cnae_sec in cnaes_sec:
        codigo = cnae_sec.get("codigo", "") if isinstance(cnae_sec, dict) else cnae_sec
        setor_sec = get_setor_tsb(codigo)
        if setor_sec and TSB_SETORES[setor_sec].get("elegivel_verde"):
            setores_secundarios.append(setor_sec)
            score += 5

    score = min(score, 100)  # Max 100

    # Determinar classificacao
    if score >= 80:
        classificacao = "VERDE"
        elegivel = True
    elif score >= 60:
        classificacao = "TRANSICAO"
        elegivel = True
    elif score >= 40:
        classificacao = "POTENCIAL"
        elegivel = False
    else:
        classificacao = "NAO_ELEGIVEL"
        elegivel = False

    return {
        "elegivel": elegivel,
        "setor_tsb": setor_id,
        "setor_nome": setor["nome"],
        "setor_descricao": setor["descricao"],
        "classificacao": classificacao,
        "score": score,
        "criterios": setor.get("criterios", []),
        "salvaguardas": setor.get("salvaguardas", []),
        "setores_secundarios": list(set(setores_secundarios)),
        "nota": setor.get("nota", ""),
        "recomendacao": get_recomendacao(classificacao)
    }

def get_recomendacao(classificacao):
    """Retorna recomendacao baseada na classificacao"""
    recomendacoes = {
        "VERDE": "Atividade elegivel para financiamento verde e rotulagem TSB",
        "TRANSICAO": "Atividade em transicao - verificar criterios tecnicos especificos",
        "POTENCIAL": "Potencial para adequacao - avaliar plano de transicao",
        "NAO_ELEGIVEL": "Atividade nao elegivel atualmente - monitorar atualizacoes TSB",
        "FORA_ESCOPO": "CNAE fora do escopo atual da TSB",
        "NAO_CLASSIFICAVEL": "Dados insuficientes para classificacao"
    }
    return recomendacoes.get(classificacao, "")

# ===========================================
# PROCESSAMENTO DE DEBENTURES
# ===========================================

def extrair_cnpj_emissor(nome_emissor):
    """Tenta identificar CNPJ a partir do nome do emissor"""
    # Lista de empresas conhecidas com CNPJs
    empresas_conhecidas = {
        "COMGAS": "61856571000117",
        "COMPANHIA DE GAS DE SAO PAULO": "61856571000117",
        "JSL": "52548435000179",
        "VALE": "33592510000154",
        "LOJAS AMERICANAS": "33014556000196",
        "CEMIG": "17155730000164",
        "SABESP": "43776517000180",
        "NATURA": "71673990000177",
        "MRS LOGISTICA": "01417222000177",
        "LOCALIZA": "16670085000155",
        "RAIZEN": "08070508000178",
        "ENERGISA": "00864214000106",
        "IGUATEMI": "51218147000193",
        "ECORODOVIAS": "04149454000180",
        "ALUPAR": "08364948000138",
        "COPASA": "17281106000103",
        "CELPA": "04895728000180",
        "CEMAR": "06272793000184",
        "MRV": "08343492000120",
        "SUL AMERICA": "29978814000187",
        "ALGAR TELECOM": "71208516000174",
    }

    nome_upper = nome_emissor.upper() if nome_emissor else ""

    for empresa, cnpj in empresas_conhecidas.items():
        if empresa in nome_upper:
            return cnpj

    return None

def processar_debentures_tsb(debentures):
    """Processa lista de debentures e classifica segundo TSB"""
    resultados = []
    empresas_processadas = {}

    for deb in debentures:
        emissor = deb.get("emissor", "")

        # Verificar se empresa ja foi processada
        if emissor in empresas_processadas:
            resultado_empresa = empresas_processadas[emissor]
        else:
            # Tentar encontrar CNPJ
            cnpj = extrair_cnpj_emissor(emissor)

            if cnpj:
                # Consultar dados da empresa
                dados_empresa = consultar_cnpj(cnpj)
                time.sleep(0.5)  # Rate limiting
            else:
                dados_empresa = {
                    "erro": "CNPJ nao identificado",
                    "razao_social": emissor
                }

            # Classificar TSB
            classificacao_tsb = classificar_tsb(dados_empresa)

            resultado_empresa = {
                "dados_empresa": dados_empresa,
                "classificacao_tsb": classificacao_tsb
            }
            empresas_processadas[emissor] = resultado_empresa

        # Adicionar resultado da debenture
        resultados.append({
            "codigo": deb.get("codigo", ""),
            "emissor": emissor,
            "grupo": deb.get("grupo", ""),
            "vencimento": deb.get("vencimento", ""),
            "taxa": deb.get("taxa", 0),
            "pu": deb.get("pu", 0),
            "cnpj": resultado_empresa["dados_empresa"].get("cnpj", ""),
            "cnae_principal": resultado_empresa["dados_empresa"].get("cnae_principal", ""),
            "cnae_descricao": resultado_empresa["dados_empresa"].get("cnae_descricao", ""),
            "setor_tsb": resultado_empresa["classificacao_tsb"].get("setor_nome", ""),
            "classificacao_tsb": resultado_empresa["classificacao_tsb"].get("classificacao", ""),
            "score_tsb": resultado_empresa["classificacao_tsb"].get("score", 0),
            "elegivel_verde": resultado_empresa["classificacao_tsb"].get("elegivel", False),
            "criterios_tsb": resultado_empresa["classificacao_tsb"].get("criterios", []),
            "recomendacao": resultado_empresa["classificacao_tsb"].get("recomendacao", "")
        })

    return resultados, empresas_processadas

# ===========================================
# MAIN - PROCESSAR DADOS EXISTENTES
# ===========================================

if __name__ == "__main__":
    from glob import glob

    DATA_DIR = r'C:\Users\Cliente\Development\powerbi-bv\data\anbima'

    print("=" * 60)
    print("SISTEMA DE MAPEAMENTO TSB - TAXONOMIA SUSTENTAVEL BRASILEIRA")
    print("=" * 60)

    # Carregar dados de debentures
    json_files = glob(f'{DATA_DIR}/todos_titulos_*.json')
    if not json_files:
        print("Nenhum arquivo de titulos encontrado!")
        exit(1)

    with open(sorted(json_files)[-1], 'r', encoding='utf-8') as f:
        dados = json.load(f)

    debentures = dados.get('debentures', [])
    print(f"\nTotal de debentures: {len(debentures)}")

    # Extrair empresas unicas
    empresas = {}
    for d in debentures:
        emissor = d.get('emissor', 'N/A')
        if emissor and emissor != 'N/A':
            if emissor not in empresas:
                empresas[emissor] = {
                    'titulos': 0,
                    'cnpj': extrair_cnpj_emissor(emissor)
                }
            empresas[emissor]['titulos'] += 1

    print(f"Empresas emissoras: {len(empresas)}")

    # Classificar empresas com CNPJ conhecido
    empresas_com_cnpj = [(e, d) for e, d in empresas.items() if d['cnpj']]
    print(f"Empresas com CNPJ identificado: {len(empresas_com_cnpj)}")

    # Processar empresas (limitado para demonstracao)
    print("\n" + "-" * 60)
    print("PROCESSANDO EMPRESAS (amostra)...")
    print("-" * 60)

    resultados_tsb = []
    for emissor, dados_emp in list(empresas_com_cnpj)[:10]:  # Limitar a 10 para demonstracao
        print(f"\nConsultando: {emissor[:50]}...")

        dados_empresa = consultar_cnpj(dados_emp['cnpj'])

        if not dados_empresa.get('erro'):
            classificacao = classificar_tsb(dados_empresa)

            resultado = {
                'emissor': emissor,
                'cnpj': dados_emp['cnpj'],
                'cnae': dados_empresa.get('cnae_principal', ''),
                'cnae_descricao': dados_empresa.get('cnae_descricao', ''),
                'setor_tsb': classificacao.get('setor_nome', ''),
                'classificacao': classificacao.get('classificacao', ''),
                'score': classificacao.get('score', 0),
                'elegivel': classificacao.get('elegivel', False),
                'titulos': dados_emp['titulos']
            }
            resultados_tsb.append(resultado)

            status = "VERDE" if resultado['elegivel'] else "NAO ELEGIVEL"
            print(f"  CNAE: {resultado['cnae']} - {resultado['cnae_descricao'][:40]}")
            print(f"  Setor TSB: {resultado['setor_tsb']} | Score: {resultado['score']} | {status}")
        else:
            print(f"  Erro: {dados_empresa.get('erro')}")

        time.sleep(1)  # Rate limiting

    # Salvar resultados
    output_file = f'{DATA_DIR}/tsb_classificacao.json'
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump({
            'data_processamento': datetime.now().isoformat(),
            'total_empresas': len(empresas),
            'empresas_classificadas': len(resultados_tsb),
            'resultados': resultados_tsb,
            'setores_tsb': TSB_SETORES
        }, f, ensure_ascii=False, indent=2)

    print(f"\n\nResultados salvos em: {output_file}")

    # Resumo
    print("\n" + "=" * 60)
    print("RESUMO DA CLASSIFICACAO TSB")
    print("=" * 60)

    verdes = sum(1 for r in resultados_tsb if r['elegivel'])
    print(f"Total processado: {len(resultados_tsb)}")
    print(f"Elegiveis (Verde/Transicao): {verdes}")
    print(f"Nao elegiveis: {len(resultados_tsb) - verdes}")

    # Por setor
    setores_count = {}
    for r in resultados_tsb:
        setor = r.get('setor_tsb', 'Nao Mapeado')
        setores_count[setor] = setores_count.get(setor, 0) + 1

    print("\nPor Setor TSB:")
    for setor, count in sorted(setores_count.items(), key=lambda x: -x[1]):
        print(f"  {setor}: {count}")
