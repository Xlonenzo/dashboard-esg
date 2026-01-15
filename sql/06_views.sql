-- ============================================================================
-- MODELAGEM DE DADOS ESG - BANCO VOTORANTIM
-- Azure SQL Server Database
-- Script 06: Views para Power BI
-- ============================================================================

-- ============================================================================
-- VIEW: CARTEIRA CONSOLIDADA
-- View principal para analise de carteira ESG
-- ============================================================================
IF OBJECT_ID('esg.vw_CarteiraConsolidada', 'V') IS NOT NULL DROP VIEW esg.vw_CarteiraConsolidada;
GO

CREATE VIEW esg.vw_CarteiraConsolidada AS
SELECT
    fc.CarteiraID,
    e.EmpresaNome,
    e.CNPJ,
    s.SetorNome AS Setor,
    ss.SubSetorNome AS SubSetor,
    c.CategoriaNome AS Categoria,
    t.TemaNome AS Tema,
    p.ProdutoNome AS Produto,
    fc.AnoReferencia,
    fc.ValorCarteira,
    fc.StatusLeitura,
    dt.Ano,
    dt.Trimestre,
    dt.MesNome
FROM esg.FatoCarteira fc
INNER JOIN esg.DimEmpresa e ON fc.EmpresaID = e.EmpresaID
LEFT JOIN esg.DimSetor s ON fc.SetorID = s.SetorID
LEFT JOIN esg.DimSubSetor ss ON fc.SubSetorID = ss.SubSetorID
LEFT JOIN esg.DimCategoria c ON fc.CategoriaID = c.CategoriaID
LEFT JOIN esg.DimTema t ON fc.TemaID = t.TemaID
LEFT JOIN esg.DimProduto p ON fc.ProdutoID = p.ProdutoID
LEFT JOIN esg.DimTempo dt ON fc.DataID = dt.DataID
WHERE e.Ativo = 1;
GO

-- ============================================================================
-- VIEW: KPIs POR EMPRESA
-- View para analise de KPIs
-- ============================================================================
IF OBJECT_ID('esg.vw_KPIsEmpresa', 'V') IS NOT NULL DROP VIEW esg.vw_KPIsEmpresa;
GO

CREATE VIEW esg.vw_KPIsEmpresa AS
SELECT
    fk.KPIID,
    e.EmpresaNome,
    e.CNPJ,
    s.SetorNome AS Setor,
    tk.KPINome,
    tk.UnidadeMedida,
    fk.AnoReferencia,
    fk.ValorNumerico,
    fk.ValorTexto,
    fk.VariacaoAnterior,
    fk.FonteDados
FROM esg.FatoKPI fk
INNER JOIN esg.DimEmpresa e ON fk.EmpresaID = e.EmpresaID
INNER JOIN esg.DimTipoKPI tk ON fk.TipoKPIID = tk.TipoKPIID
LEFT JOIN esg.DimSetor s ON fk.SetorID = s.SetorID
WHERE e.Ativo = 1;
GO

-- ============================================================================
-- VIEW: RESUMO POR SETOR
-- Totais de carteira por setor
-- ============================================================================
IF OBJECT_ID('esg.vw_ResumoSetor', 'V') IS NOT NULL DROP VIEW esg.vw_ResumoSetor;
GO

CREATE VIEW esg.vw_ResumoSetor AS
SELECT
    s.SetorNome,
    fc.AnoReferencia,
    COUNT(DISTINCT fc.EmpresaID) AS TotalEmpresas,
    SUM(fc.ValorCarteira) AS ValorTotalCarteira,
    AVG(fc.ValorCarteira) AS ValorMedioCarteira
FROM esg.FatoCarteira fc
INNER JOIN esg.DimSetor s ON fc.SetorID = s.SetorID
GROUP BY s.SetorNome, fc.AnoReferencia;
GO

-- ============================================================================
-- VIEW: RESUMO POR CATEGORIA ESG
-- Totais por categoria (Green, Social, Sustainable)
-- ============================================================================
IF OBJECT_ID('esg.vw_ResumoCategoria', 'V') IS NOT NULL DROP VIEW esg.vw_ResumoCategoria;
GO

CREATE VIEW esg.vw_ResumoCategoria AS
SELECT
    c.CategoriaNome,
    c.CategoriaCor,
    fc.AnoReferencia,
    COUNT(DISTINCT fc.EmpresaID) AS TotalEmpresas,
    SUM(fc.ValorCarteira) AS ValorTotalCarteira
FROM esg.FatoCarteira fc
INNER JOIN esg.DimCategoria c ON fc.CategoriaID = c.CategoriaID
GROUP BY c.CategoriaNome, c.CategoriaCor, fc.AnoReferencia;
GO

-- ============================================================================
-- VIEW: INDICADORES SANEAMENTO CONSOLIDADO
-- ============================================================================
IF OBJECT_ID('esg.vw_IndicadoresSaneamento', 'V') IS NOT NULL DROP VIEW esg.vw_IndicadoresSaneamento;
GO

CREATE VIEW esg.vw_IndicadoresSaneamento AS
SELECT
    e.EmpresaNome,
    fi.AnoReferencia,
    fi.VolumeAguaTratada,
    fi.VolumeEsgotoTratado,
    fi.PopulacaoAtendidaAgua,
    fi.PopulacaoAtendidaEsgoto,
    fi.InstalacoesAdicionadas,
    fi.ValorCarteira
FROM esg.FatoIndicadorSaneamento fi
INNER JOIN esg.DimEmpresa e ON fi.EmpresaID = e.EmpresaID;
GO

-- ============================================================================
-- VIEW: INDICADORES SAUDE CONSOLIDADO
-- ============================================================================
IF OBJECT_ID('esg.vw_IndicadoresSaude', 'V') IS NOT NULL DROP VIEW esg.vw_IndicadoresSaude;
GO

CREATE VIEW esg.vw_IndicadoresSaude AS
SELECT
    e.EmpresaNome,
    fi.AnoReferencia,
    fi.VagasUnidadesSaude,
    fi.PacientesAtendidos,
    fi.AumentoCapacidadeLeitos,
    fi.ReducaoDensidade,
    fi.ReducaoCustoTratamentos,
    fi.LeitosAdicionados,
    fi.PacientesBeneficiados,
    fi.ValorCarteira,
    fi.FonteDados,
    fi.Observacoes
FROM esg.FatoIndicadorSaude fi
INNER JOIN esg.DimEmpresa e ON fi.EmpresaID = e.EmpresaID;
GO

-- ============================================================================
-- VIEW: INDICADORES ENERGIA CONSOLIDADO
-- ============================================================================
IF OBJECT_ID('esg.vw_IndicadoresEnergia', 'V') IS NOT NULL DROP VIEW esg.vw_IndicadoresEnergia;
GO

CREATE VIEW esg.vw_IndicadoresEnergia AS
SELECT
    e.EmpresaNome,
    fi.AnoReferencia,
    fi.CapacidadeInstaladaMW,
    fi.EnergiaRenovavelMW,
    fi.PercentualRenovavel,
    fi.EmissoesEvitadasTCO2,
    fi.ValorCarteira
FROM esg.FatoIndicadorEnergia fi
INNER JOIN esg.DimEmpresa e ON fi.EmpresaID = e.EmpresaID;
GO

-- ============================================================================
-- VIEW: EMPRESAS COM ODS
-- Relacionamento entre empresas e ODS
-- ============================================================================
IF OBJECT_ID('esg.vw_EmpresaODS', 'V') IS NOT NULL DROP VIEW esg.vw_EmpresaODS;
GO

CREATE VIEW esg.vw_EmpresaODS AS
SELECT
    e.EmpresaNome,
    e.CNPJ,
    s.SetorNome AS Setor,
    o.ODSID,
    o.ODSNome,
    o.ODSCor,
    bo.TipoContribuicao
FROM esg.BridgeEmpresaODS bo
INNER JOIN esg.DimEmpresa e ON bo.EmpresaID = e.EmpresaID
INNER JOIN esg.DimODS o ON bo.ODSID = o.ODSID
LEFT JOIN esg.DimSetor s ON e.SetorID = s.SetorID
WHERE e.Ativo = 1;
GO

-- ============================================================================
-- VIEW: METAS 2030
-- Status das metas para 2030
-- ============================================================================
IF OBJECT_ID('esg.vw_Metas2030', 'V') IS NOT NULL DROP VIEW esg.vw_Metas2030;
GO

CREATE VIEW esg.vw_Metas2030 AS
SELECT
    Indicador,
    AnoReferencia,
    ValorMeta,
    ValorRealizado,
    PercentualAtingido,
    CrescimentoYoY,
    UnidadeMedida,
    CASE
        WHEN PercentualAtingido >= 100 THEN 'Atingido'
        WHEN PercentualAtingido >= 75 THEN 'Em Progresso'
        WHEN PercentualAtingido >= 50 THEN 'Atencao'
        ELSE 'Critico'
    END AS StatusMeta
FROM esg.FatoMeta2030;
GO

-- ============================================================================
-- VIEW: VALIDACAO EMPRESAS (4 Regras)
-- Status de conformidade das empresas
-- ============================================================================
IF OBJECT_ID('esg.vw_ValidacaoEmpresas', 'V') IS NOT NULL DROP VIEW esg.vw_ValidacaoEmpresas;
GO

CREATE VIEW esg.vw_ValidacaoEmpresas AS
SELECT
    e.EmpresaNome,
    e.CNPJ,
    s.SetorNome AS Setor,
    v.AnoReferencia,
    v.CategoriaGSS,
    v.TaxonomiaFEBRABAN_OK,
    v.CNAE_OK,
    v.Exclusao,
    v.Conforme,
    CASE
        WHEN v.Conforme = 1 THEN 'Conforme'
        WHEN v.Exclusao = 1 THEN 'Em Lista de Exclusao'
        ELSE 'Nao Conforme'
    END AS StatusConformidade
FROM esg.ValidacaoEmpresa v
INNER JOIN esg.DimEmpresa e ON v.EmpresaID = e.EmpresaID
LEFT JOIN esg.DimSetor s ON e.SetorID = s.SetorID;
GO

-- ============================================================================
-- VIEW: INDICADORES CONSOLIDADOS
-- Todos os indicadores de todas as empresas em uma unica view
-- ============================================================================
IF OBJECT_ID('esg.vw_IndicadoresConsolidados', 'V') IS NOT NULL DROP VIEW esg.vw_IndicadoresConsolidados;
GO

CREATE VIEW esg.vw_IndicadoresConsolidados AS
SELECT
    e.EmpresaNome,
    s.SetorNome,
    'Saneamento' AS TipoIndicador,
    f.AnoReferencia,
    f.VolumeAguaTratada,
    f.VolumeEsgotoTratado,
    f.PopulacaoAtendidaAgua,
    f.PopulacaoAtendidaEsgoto,
    f.InstalacoesAdicionadas,
    NULL AS VagasUnidadesSaude,
    NULL AS PacientesAtendidos,
    NULL AS AumentoCapacidadeLeitos,
    NULL AS ReducaoDensidade,
    NULL AS ReducaoCustoTratamentos,
    NULL AS LeitosAdicionados,
    NULL AS PacientesBeneficiados,
    NULL AS CapacidadeInstaladaMW,
    NULL AS EnergiaRenovavelMW,
    NULL AS PercentualRenovavel,
    NULL AS EmissoesEvitadasTCO2,
    f.ValorCarteira
FROM esg.FatoIndicadorSaneamento f
INNER JOIN esg.DimEmpresa e ON f.EmpresaID = e.EmpresaID
LEFT JOIN esg.DimSetor s ON e.SetorID = s.SetorID
UNION ALL
SELECT
    e.EmpresaNome,
    s.SetorNome,
    'Saude' AS TipoIndicador,
    f.AnoReferencia,
    NULL, NULL, NULL, NULL, NULL,
    f.VagasUnidadesSaude,
    f.PacientesAtendidos,
    f.AumentoCapacidadeLeitos,
    f.ReducaoDensidade,
    f.ReducaoCustoTratamentos,
    f.LeitosAdicionados,
    f.PacientesBeneficiados,
    NULL, NULL, NULL, NULL,
    f.ValorCarteira
FROM esg.FatoIndicadorSaude f
INNER JOIN esg.DimEmpresa e ON f.EmpresaID = e.EmpresaID
LEFT JOIN esg.DimSetor s ON e.SetorID = s.SetorID
UNION ALL
SELECT
    e.EmpresaNome,
    s.SetorNome,
    'Energia' AS TipoIndicador,
    f.AnoReferencia,
    NULL, NULL, NULL, NULL, NULL,
    NULL, NULL, NULL, NULL, NULL, NULL, NULL,
    f.CapacidadeInstaladaMW,
    f.EnergiaRenovavelMW,
    f.PercentualRenovavel,
    f.EmissoesEvitadasTCO2,
    f.ValorCarteira
FROM esg.FatoIndicadorEnergia f
INNER JOIN esg.DimEmpresa e ON f.EmpresaID = e.EmpresaID
LEFT JOIN esg.DimSetor s ON e.SetorID = s.SetorID;
GO

-- ============================================================================
-- VIEW: RESUMO ESG CONSOLIDADO
-- Totais gerais para dashboard principal
-- ============================================================================
IF OBJECT_ID('esg.vw_ResumoESG', 'V') IS NOT NULL DROP VIEW esg.vw_ResumoESG;
GO

CREATE VIEW esg.vw_ResumoESG AS
SELECT
    (SELECT SUM(ValorCarteira) FROM esg.FatoCarteira) AS TotalCarteiraESG,
    FORMAT((SELECT SUM(ValorCarteira) FROM esg.FatoCarteira), 'N2', 'pt-BR') AS TotalCarteiraESG_Fmt,
    (SELECT COUNT(DISTINCT EmpresaID) FROM esg.FatoCarteira) AS TotalEmpresas,
    (SELECT SUM(PopulacaoAtendidaAgua) + SUM(ISNULL(PopulacaoAtendidaEsgoto,0)) FROM esg.FatoIndicadorSaneamento)
    + (SELECT ISNULL(SUM(PacientesBeneficiados),0) FROM esg.FatoIndicadorSaude) AS PessoasImpactadas,
    FORMAT((SELECT SUM(PopulacaoAtendidaAgua) + SUM(ISNULL(PopulacaoAtendidaEsgoto,0)) FROM esg.FatoIndicadorSaneamento)
    + (SELECT ISNULL(SUM(PacientesBeneficiados),0) FROM esg.FatoIndicadorSaude), 'N0', 'pt-BR') AS PessoasImpactadas_Fmt;
GO

PRINT 'Views criadas com sucesso!'
GO
