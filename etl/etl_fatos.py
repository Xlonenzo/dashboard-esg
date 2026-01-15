"""
ETL para Tabelas de Fato
"""
import pandas as pd
import re
from database import db
from config import EXCEL_FILES, ANO_REFERENCIA


def limpar_valor_monetario(valor) -> float:
    """
    Limpa e converte valor monetario para float.
    """
    if pd.isna(valor):
        return None

    if isinstance(valor, (int, float)):
        return float(valor)

    # Remove caracteres de formatacao
    valor_str = str(valor)
    valor_str = re.sub(r'[R$\s.]', '', valor_str)
    valor_str = valor_str.replace(',', '.')

    try:
        return float(valor_str)
    except:
        return None


def extrair_numero(texto) -> int:
    """
    Extrai o primeiro numero de um texto.
    Ex: '9 novas instalacoes' -> 9
    """
    if pd.isna(texto):
        return None

    if isinstance(texto, (int, float)):
        return int(texto)

    # Extrai numeros do texto
    numeros = re.findall(r'\d+', str(texto))
    if numeros:
        return int(numeros[0])
    return None


def get_empresa_id(empresa_nome: str, empresa_lookup: dict) -> int:
    """
    Busca o ID da empresa pelo nome (busca parcial).
    """
    if not empresa_nome or pd.isna(empresa_nome):
        return None

    empresa_nome = str(empresa_nome).strip().lower()

    # Busca exata primeiro
    for nome, id in empresa_lookup.items():
        if nome.lower() == empresa_nome:
            return id

    # Busca parcial
    for nome, id in empresa_lookup.items():
        if empresa_nome in nome.lower() or nome.lower() in empresa_nome:
            return id

    return None


def load_fato_carteira():
    """
    Carrega valores de carteira para FatoCarteira.
    """
    print("Carregando FatoCarteira...")

    # Lookups
    empresa_lookup = db.get_lookup("DimEmpresa", "EmpresaID", "EmpresaNome")
    setor_lookup = db.get_lookup("DimSetor", "SetorID", "SetorNome")
    categoria_lookup = db.get_lookup("DimCategoria", "CategoriaID", "CategoriaNome")
    produto_lookup = db.get_lookup("DimProduto", "ProdutoID", "ProdutoNome")
    tema_lookup = db.get_lookup("DimTema", "TemaID", "TemaNome")

    carteiras = []

    # 1. Energia Renovavel
    if EXCEL_FILES["energia_renovavel"].exists():
        df = pd.read_excel(EXCEL_FILES["energia_renovavel"])
        for _, row in df.iterrows():
            empresa_id = get_empresa_id(row.get("Empresa"), empresa_lookup)
            if empresa_id:
                carteiras.append({
                    "EmpresaID": empresa_id,
                    "SetorID": setor_lookup.get(row.get("Setor", "Energia")),
                    "CategoriaID": categoria_lookup.get(row.get("Categoria")),
                    "TemaID": tema_lookup.get(row.get("Tema")),
                    "ProdutoID": produto_lookup.get(row.get("Produto")),
                    "AnoReferencia": ANO_REFERENCIA,
                    "ValorCarteira": limpar_valor_monetario(row.get("Total Carteira")),
                    "StatusLeitura": row.get("Lido", "Nao Lido")
                })

    # 2. Carteira Saude
    if EXCEL_FILES["carteira_saude"].exists():
        df = pd.read_excel(EXCEL_FILES["carteira_saude"])
        for _, row in df.iterrows():
            empresa_id = get_empresa_id(row.get("Empresa"), empresa_lookup)
            if empresa_id:
                carteiras.append({
                    "EmpresaID": empresa_id,
                    "SetorID": setor_lookup.get("Saude"),
                    "CategoriaID": categoria_lookup.get(row.get("Categoria")),
                    "TemaID": tema_lookup.get(row.get("Tema")),
                    "ProdutoID": produto_lookup.get(row.get("Produto")),
                    "AnoReferencia": ANO_REFERENCIA,
                    "ValorCarteira": limpar_valor_monetario(row.get("Total Carteira")),
                })

    # 3. Carteira Saneamento
    if EXCEL_FILES["indicadores_saneamento"].exists():
        try:
            df = pd.read_excel(EXCEL_FILES["indicadores_saneamento"], sheet_name="Carteira Saneamento")
            for _, row in df.iterrows():
                empresa_id = get_empresa_id(row.get("Empresa"), empresa_lookup)
                if empresa_id:
                    carteiras.append({
                        "EmpresaID": empresa_id,
                        "SetorID": setor_lookup.get("Saneamento"),
                        "CategoriaID": categoria_lookup.get(row.get("Categoria")),
                        "TemaID": tema_lookup.get(row.get("Tema")),
                        "AnoReferencia": ANO_REFERENCIA,
                        "ValorCarteira": limpar_valor_monetario(row.get("Carteira")),
                    })
        except Exception as e:
            print(f"  Aviso: {e}")

    # 4. Educacao
    if EXCEL_FILES["educacao"].exists():
        df = pd.read_excel(EXCEL_FILES["educacao"])
        for _, row in df.iterrows():
            empresa_id = get_empresa_id(row.get("Empresa"), empresa_lookup)
            if empresa_id:
                carteiras.append({
                    "EmpresaID": empresa_id,
                    "SetorID": setor_lookup.get("Educacao"),
                    "CategoriaID": categoria_lookup.get(row.get("Categoria")),
                    "TemaID": tema_lookup.get(row.get("Tema")),
                    "ProdutoID": produto_lookup.get(row.get("Produto")),
                    "AnoReferencia": ANO_REFERENCIA,
                    "ValorCarteira": limpar_valor_monetario(row.get("Total Carteira")),
                })

    # 5. Inclusao Digital
    if EXCEL_FILES["inclusao_digital"].exists():
        df = pd.read_excel(EXCEL_FILES["inclusao_digital"])
        for _, row in df.iterrows():
            empresa_id = get_empresa_id(row.get("Empresa"), empresa_lookup)
            if empresa_id:
                carteiras.append({
                    "EmpresaID": empresa_id,
                    "SetorID": setor_lookup.get("Inclusao Digital"),
                    "CategoriaID": categoria_lookup.get(row.get("Categoria")),
                    "TemaID": tema_lookup.get(row.get("Tema")),
                    "ProdutoID": produto_lookup.get(row.get("Produto")),
                    "AnoReferencia": ANO_REFERENCIA,
                    "ValorCarteira": limpar_valor_monetario(row.get("Total Carteira")),
                })

    # Insere no banco
    df_carteiras = pd.DataFrame(carteiras)
    if not df_carteiras.empty:
        db.to_sql(df_carteiras, "FatoCarteira", if_exists="append")
        db.log_import("FatoCarteira", "Multiplos arquivos", len(df_carteiras))
        print(f"  {len(df_carteiras)} registros de carteira carregados.")
    else:
        print("  Nenhum registro de carteira encontrado.")


def load_fato_kpi():
    """
    Carrega KPIs de todas as empresas.
    """
    print("Carregando FatoKPI...")

    empresa_lookup = db.get_lookup("DimEmpresa", "EmpresaID", "EmpresaNome")
    setor_lookup = db.get_lookup("DimSetor", "SetorID", "SetorNome")

    # Mapeamento direto de KPIs dos arquivos para TipoKPIID
    kpi_mapping = {
        "capacidade": 1,  # Capacidade Instalada de Energia Renovavel
        "energia renovavel": 2,  # Percentual de Energia Renovavel
        "co2": 3,  # Emissoes de CO2 Evitadas
        "emiss": 3,
        "carbono": 3,
        "reducao": 4,  # Reducao no Consumo de Energia
        "eficiencia": 5,  # Eficiencia Energetica
        "solar": 6,  # Geracao de Energia Solar
        "fotovoltaic": 6,
        "eolica": 7,  # Geracao de Energia Eolica
        "agua tratada": 8,  # Volume de Agua Tratada
        "agua salva": 9,
        "esgoto": 10,
        "geracao": 2,  # Geracao de energia
        "mwh": 2,
        "mw": 1,
    }

    def get_tipo_kpi_id(kpi_nome):
        """Retorna o TipoKPIID baseado no nome do KPI."""
        if not kpi_nome:
            return None
        kpi_lower = str(kpi_nome).lower()
        for keyword, tipo_id in kpi_mapping.items():
            if keyword in kpi_lower:
                return tipo_id
        return 1  # Default: Capacidade Instalada

    kpis = []

    # Mapeamento de arquivos KPI -> empresa -> setor
    kpi_files = [
        ("kpi_enel", "AMPLA", "Energia"),
        ("kpi_edp", "EDP", "Energia"),
        ("kpi_engie", "ENGIE", "Energia"),
        ("kpi_isa", "ISA", "Energia"),
        ("kpi_taesa", "TAESA", "Energia"),
        ("kpi_maz", "MARLIM", "Energia"),
        ("kpi_eneva", "ENEVA", "Energia"),
        ("kpi_alianca", "ALLIANCA", "Saude"),
        ("kpi_onco", "ONCO", "Saude"),
    ]

    for file_key, empresa_nome, setor_nome in kpi_files:
        if file_key in EXCEL_FILES and EXCEL_FILES[file_key].exists():
            try:
                df = pd.read_excel(EXCEL_FILES[file_key])
                empresa_id = get_empresa_id(empresa_nome, empresa_lookup)

                if not empresa_id:
                    print(f"  Aviso: Empresa '{empresa_nome}' nao encontrada.")
                    continue

                for _, row in df.iterrows():
                    # Tenta encontrar coluna de KPI
                    kpi_nome = None
                    for col in ["KPI", "Indicador", "Metrica"]:
                        if col in row and pd.notna(row.get(col)):
                            kpi_nome = row.get(col)
                            break

                    # Se nao tem coluna KPI, usa o nome da primeira coluna
                    if not kpi_nome:
                        kpi_nome = str(df.columns[0])

                    tipo_kpi_id = get_tipo_kpi_id(kpi_nome)

                    # Extrai valor - tenta varias colunas
                    valor = None
                    for col in df.columns:
                        col_lower = str(col).lower()
                        if any(x in col_lower for x in ["valor", "2024", "total", "quantidade"]):
                            if pd.notna(row.get(col)):
                                valor = row.get(col)
                                break

                    # Se nao encontrou, pega a segunda coluna
                    if valor is None and len(df.columns) > 1:
                        valor = row.iloc[1] if pd.notna(row.iloc[1]) else None

                    # Tenta converter para numerico
                    valor_num = None
                    valor_texto = None
                    if valor is not None:
                        try:
                            valor_str = str(valor).replace(",", ".").replace("%", "").replace(" ", "")
                            valor_num = float(valor_str)
                        except:
                            valor_texto = str(valor)[:500]

                    # Fonte
                    fonte = None
                    for col in df.columns:
                        col_lower = str(col).lower()
                        if any(x in col_lower for x in ["fonte", "observ", "justif", "explic"]):
                            if pd.notna(row.get(col)):
                                fonte = str(row.get(col))[:500]
                                break

                    kpis.append({
                        "EmpresaID": empresa_id,
                        "TipoKPIID": tipo_kpi_id,
                        "SetorID": setor_lookup.get(setor_nome),
                        "AnoReferencia": ANO_REFERENCIA,
                        "ValorNumerico": valor_num,
                        "ValorTexto": valor_texto,
                        "FonteDados": fonte,
                    })

            except Exception as e:
                print(f"  Erro ao processar {file_key}: {e}")

    # Insere no banco
    df_kpis = pd.DataFrame(kpis)
    if not df_kpis.empty:
        db.to_sql(df_kpis, "FatoKPI", if_exists="append")
        db.log_import("FatoKPI", "Arquivos KPI", len(df_kpis))
        print(f"  {len(df_kpis)} KPIs carregados.")
    else:
        print("  Nenhum KPI encontrado.")


def load_fato_indicadores_energia():
    """
    Carrega indicadores especificos de energia.
    """
    print("Carregando FatoIndicadorEnergia...")

    if not EXCEL_FILES["energia_consolidado"].exists():
        print("  Arquivo nao encontrado.")
        return

    empresa_lookup = db.get_lookup("DimEmpresa", "EmpresaID", "EmpresaNome")

    try:
        df = pd.read_excel(EXCEL_FILES["energia_consolidado"])

        indicadores = []
        for _, row in df.iterrows():
            empresa_id = get_empresa_id(row.get("Empresa"), empresa_lookup)
            if empresa_id:
                # Pega valores das colunas (os nomes tem caracteres especiais)
                capacidade = None
                geracao = None
                emissoes = None
                carteira = None

                for col in df.columns:
                    col_lower = str(col).lower()
                    val = row.get(col)
                    if pd.notna(val):
                        if "capacidade" in col_lower:
                            capacidade = limpar_valor_monetario(val)
                        elif "gera" in col_lower:
                            geracao = limpar_valor_monetario(val)
                        elif "emiss" in col_lower or "gee" in col_lower or "co2" in col_lower:
                            emissoes = limpar_valor_monetario(val)
                        elif "carteira" in col_lower or "total" in col_lower:
                            carteira = limpar_valor_monetario(val)

                indicadores.append({
                    "EmpresaID": empresa_id,
                    "AnoReferencia": ANO_REFERENCIA,
                    "CapacidadeInstaladaMW": capacidade,
                    "EnergiaRenovavelMW": geracao,
                    "EmissoesEvitadasTCO2": emissoes,
                    "ValorCarteira": carteira,
                })

        df_indicadores = pd.DataFrame(indicadores)
        if not df_indicadores.empty:
            db.to_sql(df_indicadores, "FatoIndicadorEnergia", if_exists="append")
            db.log_import("FatoIndicadorEnergia", str(EXCEL_FILES["energia_consolidado"]), len(df_indicadores))
            print(f"  {len(df_indicadores)} indicadores de energia carregados.")
        else:
            print("  Nenhum indicador de energia encontrado.")

    except Exception as e:
        print(f"  Erro: {e}")


def load_fato_indicadores_saneamento():
    """
    Carrega indicadores especificos de saneamento.
    """
    print("Carregando FatoIndicadorSaneamento...")

    if not EXCEL_FILES["indicadores_saneamento"].exists():
        print("  Arquivo nao encontrado.")
        return

    empresa_lookup = db.get_lookup("DimEmpresa", "EmpresaID", "EmpresaNome")

    # Mapeamento de nomes abreviados para nomes completos
    nome_mapping = {
        "CESAN": "CATARINENSE DE AGUAS",
        "COMPESA": "PERNAMBUCANA DE SANEAMENTO",
        "EMBASA": "BAIANA DE AGUAS",
        "IGUA RJ": "IGUA RIO",
        "CASAN": "CATARINENSE",
    }

    try:
        df = pd.read_excel(EXCEL_FILES["indicadores_saneamento"], sheet_name=0)

        indicadores = []
        for _, row in df.iterrows():
            empresa_nome = row.get("Empresa")

            # Tenta mapear nome abreviado
            if empresa_nome in nome_mapping:
                empresa_nome = nome_mapping[empresa_nome]

            empresa_id = get_empresa_id(empresa_nome, empresa_lookup)

            # Se nao encontrou, tenta buscar de forma mais flexivel
            if not empresa_id:
                empresa_nome = row.get("Empresa")
                if empresa_nome:
                    print(f"  Aviso: Empresa '{empresa_nome}' nao encontrada, pulando...")
                continue

            # Busca valores nas colunas de forma flexivel
            volume_agua = None
            volume_esgoto = None
            pop_agua = None
            pop_esgoto = None
            instalacoes = None
            carteira = None

            for col in df.columns:
                col_lower = str(col).lower()
                val = row.get(col)
                if pd.notna(val):
                    if "gua" in col_lower and "tratada" in col_lower:
                        volume_agua = limpar_valor_monetario(val)
                    elif "esgoto" in col_lower and "tratado" in col_lower:
                        volume_esgoto = limpar_valor_monetario(val)
                    elif "popula" in col_lower and "gua" in col_lower:
                        pop_agua = extrair_numero(val)
                    elif "popula" in col_lower and "esgoto" in col_lower:
                        pop_esgoto = extrair_numero(val)
                    elif "instala" in col_lower:
                        instalacoes = extrair_numero(val)
                    elif "carteira" in col_lower:
                        carteira = limpar_valor_monetario(val)

            indicadores.append({
                "EmpresaID": empresa_id,
                "AnoReferencia": ANO_REFERENCIA,
                "VolumeAguaTratada": volume_agua,
                "VolumeEsgotoTratado": volume_esgoto,
                "PopulacaoAtendidaAgua": pop_agua,
                "PopulacaoAtendidaEsgoto": pop_esgoto,
                "InstalacoesAdicionadas": instalacoes,
                "ValorCarteira": carteira,
            })

        df_indicadores = pd.DataFrame(indicadores)
        if not df_indicadores.empty:
            db.to_sql(df_indicadores, "FatoIndicadorSaneamento", if_exists="append")
            db.log_import("FatoIndicadorSaneamento", str(EXCEL_FILES["indicadores_saneamento"]), len(df_indicadores))
            print(f"  {len(df_indicadores)} indicadores de saneamento carregados.")
        else:
            print("  Nenhum indicador de saneamento encontrado.")

    except Exception as e:
        print(f"  Erro: {e}")


def load_fato_indicadores_saude():
    """
    Carrega indicadores especificos de saude.
    """
    print("Carregando FatoIndicadorSaude...")

    if not EXCEL_FILES["empresa_saude"].exists():
        print("  Arquivo nao encontrado.")
        return

    empresa_lookup = db.get_lookup("DimEmpresa", "EmpresaID", "EmpresaNome")

    try:
        df = pd.read_excel(EXCEL_FILES["empresa_saude"])

        indicadores = []
        for _, row in df.iterrows():
            empresa_id = get_empresa_id(row.get("Empresa"), empresa_lookup)
            if empresa_id:
                indicadores.append({
                    "EmpresaID": empresa_id,
                    "AnoReferencia": ANO_REFERENCIA,
                    "VagasUnidadesSaude": limpar_valor_monetario(row.get("Número de vagas em unidades de saúde ou pacientes atendidos")),
                    "AumentoCapacidadeLeitos": limpar_valor_monetario(row.get("Aumento da capacidade de leitos hospitalares e/ou diminuição da densidade")),
                    "ReducaoCustoTratamentos": limpar_valor_monetario(row.get("Redução de custos para tratamentos e medicamentos padrão")),
                    "LeitosAdicionados": limpar_valor_monetario(row.get("Número de leitos hospitalares adicionados")),
                    "PacientesBeneficiados": limpar_valor_monetario(row.get("Número de pacientes beneficiados por cuidados de saúde ou tratamentos médicos")),
                })

        df_indicadores = pd.DataFrame(indicadores)
        if not df_indicadores.empty:
            db.to_sql(df_indicadores, "FatoIndicadorSaude", if_exists="append")
            db.log_import("FatoIndicadorSaude", str(EXCEL_FILES["empresa_saude"]), len(df_indicadores))
            print(f"  {len(df_indicadores)} indicadores de saude carregados.")
        else:
            print("  Nenhum indicador de saude encontrado.")

    except Exception as e:
        print(f"  Erro: {e}")


def load_fato_meta_2030():
    """
    Carrega metas 2030.
    """
    print("Carregando FatoMeta2030...")

    if not EXCEL_FILES["status_meta_2030"].exists():
        print("  Arquivo nao encontrado.")
        return

    try:
        # Sheet "Até 2030"
        df_ate = pd.read_excel(EXCEL_FILES["status_meta_2030"], sheet_name="Até 2030")
        for _, row in df_ate.iterrows():
            indicador = row.get("Indicador")
            valor = row.get("Valor")
            if indicador and pd.notna(indicador):
                query = """
                INSERT INTO esg.FatoMeta2030 (Indicador, AnoReferencia, ValorMeta, UnidadeMedida)
                VALUES (?, 2030, ?, 'R$')
                """
                db.execute_query(query, (str(indicador), limpar_valor_monetario(valor)))

        # Sheet "YoY"
        df_yoy = pd.read_excel(EXCEL_FILES["status_meta_2030"], sheet_name="YoY")
        for _, row in df_yoy.iterrows():
            ano = row.get("Ano")
            valor = row.get("Volume ESG Executado (R$)")
            crescimento = row.get("Crescimento YoY")

            if ano and pd.notna(ano):
                # Limpa o crescimento (pode vir como % ou texto)
                crescimento_limpo = limpar_valor_monetario(crescimento)

                query = """
                INSERT INTO esg.FatoMeta2030 (Indicador, AnoReferencia, ValorRealizado, CrescimentoYoY, UnidadeMedida)
                VALUES ('Volume ESG Executado', ?, ?, ?, 'R$')
                """
                db.execute_query(query, (int(ano), limpar_valor_monetario(valor), crescimento_limpo))

        db.log_import("FatoMeta2030", str(EXCEL_FILES["status_meta_2030"]), len(df_ate) + len(df_yoy))
        print(f"  Metas 2030 carregadas.")

    except Exception as e:
        print(f"  Erro: {e}")


def load_validacao_empresas():
    """
    Carrega validacao de empresas (4 regras).
    """
    print("Carregando ValidacaoEmpresa...")

    if not EXCEL_FILES["carteira_saneamento"].exists():
        print("  Arquivo nao encontrado.")
        return

    empresa_lookup = db.get_lookup("DimEmpresa", "EmpresaID", "EmpresaNome")

    try:
        df = pd.read_excel(EXCEL_FILES["carteira_saneamento"], sheet_name="Empresas")

        validacoes = []
        for _, row in df.iterrows():
            empresa_id = get_empresa_id(row.get("Empresa"), empresa_lookup)
            if empresa_id:
                def parse_bool(val):
                    if pd.isna(val):
                        return None
                    val_str = str(val).lower().strip()
                    return val_str in ["sim", "s", "yes", "y", "1", "true", "ok"]

                validacoes.append({
                    "EmpresaID": empresa_id,
                    "AnoReferencia": ANO_REFERENCIA,
                    "CategoriaGSS": row.get("Categoria_GSS"),
                    "TaxonomiaFEBRABAN_OK": parse_bool(row.get("Taxonomia_FEBRABAN_OK")),
                    "CNAE_OK": parse_bool(row.get("CNAE_OK")),
                    "Exclusao": parse_bool(row.get("Exclusao")),
                    "Conforme": parse_bool(row.get("Confome")),
                    "EvidenciaCategoria": row.get("Evidencia_Categoria"),
                    "EvidenciaTaxonomia": row.get("Evidencia_Taxonomia"),
                    "EvidenciaCNAE": row.get("Evidencia_CNAE"),
                    "EvidenciaExclusao": row.get("Evidencia_Exclusao"),
                })

        df_validacoes = pd.DataFrame(validacoes)
        if not df_validacoes.empty:
            db.to_sql(df_validacoes, "ValidacaoEmpresa", if_exists="append")
            db.log_import("ValidacaoEmpresa", str(EXCEL_FILES["carteira_saneamento"]), len(df_validacoes))
            print(f"  {len(df_validacoes)} validacoes carregadas.")
        else:
            print("  Nenhuma validacao encontrada.")

    except Exception as e:
        print(f"  Erro: {e}")


def run_fatos():
    """
    Executa todo o ETL de fatos.
    """
    print("\n" + "=" * 60)
    print("ETL FATOS")
    print("=" * 60)

    load_fato_carteira()
    load_fato_kpi()
    load_fato_indicadores_energia()
    load_fato_indicadores_saneamento()
    load_fato_indicadores_saude()
    load_fato_meta_2030()
    load_validacao_empresas()

    print("\nETL Fatos concluido!")


if __name__ == "__main__":
    run_fatos()
