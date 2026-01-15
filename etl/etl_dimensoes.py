"""
ETL para Tabelas de Dimensao
"""
import pandas as pd
import re
from pathlib import Path
from database import db
from config import EXCEL_FILES, ANO_REFERENCIA


def limpar_cnpj(cnpj) -> tuple:
    """
    Limpa e formata o CNPJ.
    Retorna (cnpj_formatado, cnpj_numerico)
    """
    if pd.isna(cnpj):
        return None, None

    # Remove caracteres nao numericos
    cnpj_num = re.sub(r'\D', '', str(cnpj))

    # Preenche com zeros a esquerda se necessario
    cnpj_num = cnpj_num.zfill(14)

    if len(cnpj_num) != 14:
        return None, None

    # Formata XX.XXX.XXX/XXXX-XX
    cnpj_fmt = f"{cnpj_num[:2]}.{cnpj_num[2:5]}.{cnpj_num[5:8]}/{cnpj_num[8:12]}-{cnpj_num[12:14]}"

    return cnpj_fmt, int(cnpj_num)


def load_dim_empresas():
    """
    Carrega empresas de todos os arquivos Excel para DimEmpresa.
    """
    print("Carregando DimEmpresa...")
    empresas = []

    # Lookup de setores e categorias
    setor_lookup = db.get_lookup("DimSetor", "SetorID", "SetorNome")
    categoria_lookup = db.get_lookup("DimCategoria", "CategoriaID", "CategoriaNome")
    tema_lookup = db.get_lookup("DimTema", "TemaID", "TemaNome")

    # 1. Arquivo carteira.xlsx
    if EXCEL_FILES["carteira"].exists():
        df = pd.read_excel(EXCEL_FILES["carteira"])
        for _, row in df.iterrows():
            empresa = {
                "EmpresaNome": row.get("Empresa"),
                "SetorID": setor_lookup.get(row.get("Setor")),
            }
            if empresa["EmpresaNome"]:
                empresas.append(empresa)

    # 2. Arquivo energia renovavel
    if EXCEL_FILES["energia_renovavel"].exists():
        df = pd.read_excel(EXCEL_FILES["energia_renovavel"])
        for _, row in df.iterrows():
            cnpj_fmt, cnpj_num = limpar_cnpj(row.get("CNPJ"))
            setor_nome = row.get("Setor", "Energia")
            categoria_nome = row.get("Categoria")
            tema_nome = row.get("Tema")

            empresa = {
                "EmpresaNome": row.get("Empresa"),
                "CNPJ": cnpj_fmt,
                "CNPJNumerico": cnpj_num,
                "SetorID": setor_lookup.get(setor_nome, setor_lookup.get("Energia")),
                "CategoriaID": categoria_lookup.get(categoria_nome),
                "TemaID": tema_lookup.get(tema_nome),
            }
            if empresa["EmpresaNome"]:
                empresas.append(empresa)

    # 3. Carteira Saude
    if EXCEL_FILES["carteira_saude"].exists():
        df = pd.read_excel(EXCEL_FILES["carteira_saude"])
        for _, row in df.iterrows():
            cnpj_fmt, cnpj_num = limpar_cnpj(row.get("CNPJ"))
            empresa = {
                "EmpresaNome": row.get("Empresa"),
                "CNPJ": cnpj_fmt,
                "CNPJNumerico": cnpj_num,
                "SetorID": setor_lookup.get("Saude"),
                "CategoriaID": categoria_lookup.get(row.get("Categoria")),
                "TemaID": tema_lookup.get(row.get("Tema")),
            }
            if empresa["EmpresaNome"]:
                empresas.append(empresa)

    # 4. Indicadores Saneamento - aba Carteira
    if EXCEL_FILES["indicadores_saneamento"].exists():
        try:
            df = pd.read_excel(EXCEL_FILES["indicadores_saneamento"], sheet_name="Carteira Saneamento")
            for _, row in df.iterrows():
                cnpj_fmt, cnpj_num = limpar_cnpj(row.get("CNPJ"))
                empresa = {
                    "EmpresaNome": row.get("Empresa"),
                    "CNPJ": cnpj_fmt,
                    "CNPJNumerico": cnpj_num,
                    "SetorID": setor_lookup.get("Saneamento"),
                    "CategoriaID": categoria_lookup.get(row.get("Categoria")),
                    "TemaID": tema_lookup.get(row.get("Tema")),
                }
                if empresa["EmpresaNome"]:
                    empresas.append(empresa)
        except Exception as e:
            print(f"  Aviso ao ler Carteira Saneamento: {e}")

    # 5. Educacao
    if EXCEL_FILES["educacao"].exists():
        df = pd.read_excel(EXCEL_FILES["educacao"])
        for _, row in df.iterrows():
            cnpj_fmt, cnpj_num = limpar_cnpj(row.get("CNPJ"))
            empresa = {
                "EmpresaNome": row.get("Empresa"),
                "CNPJ": cnpj_fmt,
                "CNPJNumerico": cnpj_num,
                "SetorID": setor_lookup.get("Educacao"),
                "CategoriaID": categoria_lookup.get(row.get("Categoria")),
                "TemaID": tema_lookup.get(row.get("Tema")),
            }
            if empresa["EmpresaNome"]:
                empresas.append(empresa)

    # 6. Inclusao Digital
    if EXCEL_FILES["inclusao_digital"].exists():
        df = pd.read_excel(EXCEL_FILES["inclusao_digital"])
        for _, row in df.iterrows():
            cnpj_fmt, cnpj_num = limpar_cnpj(row.get("CNPJ"))
            empresa = {
                "EmpresaNome": row.get("Empresa"),
                "CNPJ": cnpj_fmt,
                "CNPJNumerico": cnpj_num,
                "SetorID": setor_lookup.get("Inclusao Digital"),
                "CategoriaID": categoria_lookup.get(row.get("Categoria")),
                "TemaID": tema_lookup.get(row.get("Tema")),
            }
            if empresa["EmpresaNome"]:
                empresas.append(empresa)

    # 7. Empresas dos arquivos KPI (para garantir que existam)
    kpi_empresas = [
        ("kpi_enel", "AMPLA ENERGIA E SERVICOS S/A", "Energia"),
        ("kpi_edp", "EDP SAO PAULO DISTRIBUICAO DE ENERGIA S.A.", "Energia"),
        ("kpi_engie", "ENGIE BRASIL ENERGIA S.A", "Energia"),
        ("kpi_isa", "ISA CTEEP", "Energia"),
        ("kpi_taesa", "TAESA", "Energia"),
        ("kpi_maz", "MARLIM AZUL ENERGIA S.A.", "Energia"),
        ("kpi_eneva", "ENEVA S.A", "Energia"),
        ("kpi_alianca", "ALLIANCA SAUDE", "Saude"),
        ("kpi_onco", "ONCOCLINICAS", "Saude"),
    ]
    for file_key, empresa_nome, setor_nome in kpi_empresas:
        if file_key in EXCEL_FILES and EXCEL_FILES[file_key].exists():
            empresas.append({
                "EmpresaNome": empresa_nome,
                "SetorID": setor_lookup.get(setor_nome),
            })

    # Criar DataFrame e remover duplicatas
    df_empresas = pd.DataFrame(empresas)

    # Remove registros com EmpresaNome nulo ou vazio
    df_empresas = df_empresas[df_empresas["EmpresaNome"].notna()]
    df_empresas = df_empresas[df_empresas["EmpresaNome"].str.strip() != ""]

    # Remove duplicatas por nome (mantendo a primeira ocorrencia com mais dados)
    df_empresas = df_empresas.drop_duplicates(subset=["EmpresaNome"], keep="first")

    # Adiciona coluna Ativo
    df_empresas["Ativo"] = True

    # Insere no banco
    if not df_empresas.empty:
        db.to_sql(df_empresas, "DimEmpresa", if_exists="append")
        db.log_import("DimEmpresa", "Multiplos arquivos", len(df_empresas))
        print(f"  {len(df_empresas)} empresas carregadas.")
    else:
        print("  Nenhuma empresa encontrada.")

    return len(df_empresas)


def load_dim_subsetores():
    """
    Carrega subsetores a partir dos arquivos Excel.
    """
    print("Carregando DimSubSetor...")
    setor_lookup = db.get_lookup("DimSetor", "SetorID", "SetorNome")

    subsetores = []

    # Le subsetores dos arquivos de carteira
    arquivos = ["energia_renovavel", "carteira_saude", "educacao", "inclusao_digital"]

    for arq in arquivos:
        if EXCEL_FILES.get(arq) and EXCEL_FILES[arq].exists():
            df = pd.read_excel(EXCEL_FILES[arq])
            if "SubSetor" in df.columns and "Setor" in df.columns:
                for _, row in df.iterrows():
                    subsetor_nome = row.get("SubSetor")
                    setor_nome = row.get("Setor")
                    if pd.notna(subsetor_nome) and pd.notna(setor_nome):
                        subsetores.append({
                            "SetorID": setor_lookup.get(setor_nome),
                            "SubSetorNome": str(subsetor_nome).strip(),
                            "Ativo": True
                        })

    # Remove duplicatas
    df_subsetores = pd.DataFrame(subsetores)
    if not df_subsetores.empty:
        df_subsetores = df_subsetores.drop_duplicates(subset=["SubSetorNome", "SetorID"])
        df_subsetores = df_subsetores[df_subsetores["SetorID"].notna()]

        # Verifica subsetores ja existentes
        existentes = db.read_sql("SELECT SubSetorNome FROM esg.DimSubSetor")
        df_subsetores = df_subsetores[~df_subsetores["SubSetorNome"].isin(existentes["SubSetorNome"])]

        if not df_subsetores.empty:
            db.to_sql(df_subsetores, "DimSubSetor", if_exists="append")
            print(f"  {len(df_subsetores)} subsetores carregados.")
        else:
            print("  Nenhum novo subsetor para carregar.")
    else:
        print("  Nenhum subsetor encontrado nos arquivos.")


def load_dim_produtos():
    """
    Carrega produtos financeiros dos arquivos Excel.
    """
    print("Carregando DimProduto...")
    produtos = set()

    # Le produtos dos arquivos de carteira
    arquivos = ["energia_renovavel", "carteira_saude", "educacao", "inclusao_digital"]

    for arq in arquivos:
        if EXCEL_FILES.get(arq) and EXCEL_FILES[arq].exists():
            df = pd.read_excel(EXCEL_FILES[arq])
            if "Produto" in df.columns:
                for produto in df["Produto"].dropna().unique():
                    produtos.add(str(produto).strip())

    # Cria DataFrame
    df_produtos = pd.DataFrame({"ProdutoNome": list(produtos), "Ativo": True})

    if not df_produtos.empty:
        # Verifica produtos ja existentes
        try:
            existentes = db.read_sql("SELECT ProdutoNome FROM esg.DimProduto")
            df_produtos = df_produtos[~df_produtos["ProdutoNome"].isin(existentes["ProdutoNome"])]
        except:
            pass

        if not df_produtos.empty:
            db.to_sql(df_produtos, "DimProduto", if_exists="append")
            print(f"  {len(df_produtos)} produtos carregados.")
        else:
            print("  Nenhum novo produto para carregar.")
    else:
        print("  Nenhum produto encontrado.")


def load_dim_cnae():
    """
    Carrega a tabela DE_PARA de CNAE.
    """
    print("Carregando DimCNAE...")

    if not EXCEL_FILES["de_para"].exists():
        print("  Arquivo DE_PARA nao encontrado.")
        return

    df = pd.read_excel(EXCEL_FILES["de_para"], sheet_name="DE-PARA")

    # Renomeia colunas para o modelo
    df_cnae = pd.DataFrame({
        "CNAEID": df["Cnae"],
        "ClasseBV": df.get("ClasseBV"),
        "SubSetorBV": df.get("SubSetorBV"),
        "SetorBV": df.get("SetorBV"),
        "ProjetoBV": df.get("ProjetoBV"),
        "CategoriaBV": df.get("CategoriaBV"),
        "ProjetoIBGE": df.get("ProjetoIBGE"),
        "CategoriaIBGE": df.get("CategoriaIBGE"),
        "MacroIBGE": df.get("MacroIBGE"),
        "Divisao": df.get("Divisao"),
        "Grupo": df.get("Grupo"),
        "Classe": df.get("Classe"),
        "Subclasse": df.get("Subsetor"),
        "Observacoes": df.get("Observacoes"),
        "Ativo": True
    })

    # Remove linhas sem CNAE
    df_cnae = df_cnae[df_cnae["CNAEID"].notna()]
    df_cnae["CNAEID"] = df_cnae["CNAEID"].astype(int)

    # Remove duplicatas
    df_cnae = df_cnae.drop_duplicates(subset=["CNAEID"])

    if not df_cnae.empty:
        # Trunca e recarrega
        try:
            db.truncate_table("DimCNAE")
        except:
            pass

        db.to_sql(df_cnae, "DimCNAE", if_exists="append")
        db.log_import("DimCNAE", str(EXCEL_FILES["de_para"]), len(df_cnae))
        print(f"  {len(df_cnae)} CNAEs carregados.")
    else:
        print("  Nenhum CNAE encontrado.")


def load_dim_meta_ods():
    """
    Carrega metas ODS do arquivo metaods.xlsx
    """
    print("Carregando DimMetaODS...")

    if not EXCEL_FILES["metaods"].exists():
        print("  Arquivo metaods.xlsx nao encontrado.")
        return

    df = pd.read_excel(EXCEL_FILES["metaods"])

    metas = []
    for _, row in df.iterrows():
        ods_id = row.get("ODS")
        if pd.notna(ods_id):
            metas.append({
                "ODSID": int(ods_id),
                "MetaCodigo": str(row.get("Meta ODS primária (código)", "")),
                "MetaDescricaoResumida": row.get("Meta ODS primária (descrição resumida)"),
                "IndicadorONUSugerido": row.get("Indicador ONU sugerido"),
                "Ativo": True
            })

    df_metas = pd.DataFrame(metas)

    if not df_metas.empty:
        db.to_sql(df_metas, "DimMetaODS", if_exists="append")
        db.log_import("DimMetaODS", str(EXCEL_FILES["metaods"]), len(df_metas))
        print(f"  {len(df_metas)} metas ODS carregadas.")
    else:
        print("  Nenhuma meta ODS encontrada.")


def load_bridge_kpi_ods():
    """
    Carrega relacionamento KPI x ODS do arquivo ods.xlsx
    """
    print("Carregando BridgeKPIODS...")

    if not EXCEL_FILES["ods"].exists():
        print("  Arquivo ods.xlsx nao encontrado.")
        return

    df = pd.read_excel(EXCEL_FILES["ods"])

    # Lookup de TipoKPI
    kpi_lookup = db.get_lookup("DimTipoKPI", "TipoKPIID", "KPINome")

    bridges = []
    for _, row in df.iterrows():
        kpi_nome = row.get("KPI")
        ods_primaria = row.get("ODS primária (nº)")
        ods_secundarias = row.get("ODS secundárias (nº)")

        # Encontra TipoKPIID (busca parcial)
        tipo_kpi_id = None
        for nome, id in kpi_lookup.items():
            if kpi_nome and nome and kpi_nome.lower() in nome.lower():
                tipo_kpi_id = id
                break

        if tipo_kpi_id and pd.notna(ods_primaria):
            # ODS Primaria
            bridges.append({
                "TipoKPIID": tipo_kpi_id,
                "ODSID": int(ods_primaria),
                "TipoRelacao": "Primaria"
            })

            # ODS Secundarias
            if pd.notna(ods_secundarias):
                # Pode ser uma lista separada por virgula
                for ods in str(ods_secundarias).split(","):
                    ods = ods.strip()
                    if ods.isdigit():
                        bridges.append({
                            "TipoKPIID": tipo_kpi_id,
                            "ODSID": int(ods),
                            "TipoRelacao": "Secundaria"
                        })

    df_bridge = pd.DataFrame(bridges)

    if not df_bridge.empty:
        df_bridge = df_bridge.drop_duplicates()
        db.to_sql(df_bridge, "BridgeKPIODS", if_exists="append")
        print(f"  {len(df_bridge)} relacoes KPI-ODS carregadas.")
    else:
        print("  Nenhuma relacao KPI-ODS encontrada.")


def run_dimensoes():
    """
    Executa todo o ETL de dimensoes.
    """
    print("\n" + "=" * 60)
    print("ETL DIMENSOES")
    print("=" * 60)

    load_dim_cnae()
    load_dim_subsetores()
    load_dim_produtos()
    load_dim_empresas()
    load_dim_meta_ods()
    load_bridge_kpi_ods()

    print("\nETL Dimensoes concluido!")


if __name__ == "__main__":
    run_dimensoes()
