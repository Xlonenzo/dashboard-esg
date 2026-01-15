-- ============================================================================
-- BANCO DE DADOS ANBIMA ESG
-- Script 05: Views para Dashboard
-- ============================================================================

USE ANBIMA_ESG;
GO

-- ============================================================================
-- VIEW: FUNDOS ESG CONSOLIDADO
-- ============================================================================
IF OBJECT_ID('reports.vw_FundosESG', 'V') IS NOT NULL DROP VIEW reports.vw_FundosESG;
GO

CREATE VIEW reports.vw_FundosESG AS
SELECT
    f.FundoID,
    f.FundoCNPJ,
    f.FundoNome,
    g.GestoraNome,
    a.AdministradoraNome,
    c.ClassificacaoNivel1 AS TipoFundo,
    c.ClassificacaoCompleta AS ClassificacaoAnbima,
    b.BenchmarkNome,
    ce.CategoriaNome AS CategoriaESG,
    ce.Cor AS CorCategoriaESG,
    fe.FocoNome AS FocoESG,
    fe.Cor AS CorFocoESG,
    e.EstrategiaNome AS EstrategiaESG,
    f.ESGIntegrado,
    f.SufixoIS,
    f.TaxaAdministracao,
    f.TaxaPerformance,
    f.PublicoAlvo,
    f.Situacao,
    f.DataConstituicao,
    f.Ativo
FROM fundos.FatoFundo f
LEFT JOIN fundos.DimGestora g ON f.GestoraID = g.GestoraID
LEFT JOIN fundos.DimAdministradora a ON f.AdministradoraID = a.AdministradoraID
LEFT JOIN fundos.DimClassificacaoAnbima c ON f.ClassificacaoID = c.ClassificacaoID
LEFT JOIN fundos.DimBenchmark b ON f.BenchmarkID = b.BenchmarkID
LEFT JOIN esg.DimCategoriaESG ce ON f.CategoriaESGID = ce.CategoriaESGID
LEFT JOIN esg.DimFocoESG fe ON f.FocoESGID = fe.FocoESGID
LEFT JOIN esg.DimEstrategiaESG e ON f.EstrategiaID = e.EstrategiaID
WHERE f.Ativo = 1;
GO

-- ============================================================================
-- VIEW: PATRIMONIO COM DETALHES
-- ============================================================================
IF OBJECT_ID('reports.vw_PatrimonioDetalhado', 'V') IS NOT NULL DROP VIEW reports.vw_PatrimonioDetalhado;
GO

CREATE VIEW reports.vw_PatrimonioDetalhado AS
SELECT
    pl.PLID,
    f.FundoNome,
    f.FundoCNPJ,
    g.GestoraNome,
    ce.CategoriaNome AS CategoriaESG,
    fe.FocoNome AS FocoESG,
    t.Data,
    t.Ano,
    t.Mes,
    t.MesNome,
    t.AnoMes,
    t.Trimestre,
    pl.PatrimonioLiquido,
    pl.ValorCota,
    pl.CotistasTotal,
    pl.CaptacaoLiquida,
    pl.RentabilidadeMes,
    pl.RentabilidadeAno,
    pl.Rentabilidade12M,
    pl.Volatilidade12M,
    pl.SharpeRatio
FROM fundos.FatoPatrimonioLiquido pl
INNER JOIN fundos.FatoFundo f ON pl.FundoID = f.FundoID
INNER JOIN fundos.DimTempo t ON pl.DataID = t.DataID
LEFT JOIN fundos.DimGestora g ON f.GestoraID = g.GestoraID
LEFT JOIN esg.DimCategoriaESG ce ON f.CategoriaESGID = ce.CategoriaESGID
LEFT JOIN esg.DimFocoESG fe ON f.FocoESGID = fe.FocoESGID
WHERE f.Ativo = 1;
GO

-- ============================================================================
-- VIEW: RESUMO MERCADO ESG
-- ============================================================================
IF OBJECT_ID('reports.vw_ResumoMercadoESG', 'V') IS NOT NULL DROP VIEW reports.vw_ResumoMercadoESG;
GO

CREATE VIEW reports.vw_ResumoMercadoESG AS
SELECT
    r.AnoMes,
    r.Ano,
    r.Mes,
    c.CategoriaNome,
    c.Cor,
    r.TotalFundos,
    r.PatrimonioLiquidoTotal,
    r.CaptacaoLiquidaTotal,
    r.TotalCotistas,
    r.VariacaoPLMes,
    r.VariacaoPLAno,
    r.ParticipaoPLMercado,
    -- Formatados
    FORMAT(r.PatrimonioLiquidoTotal, 'N2', 'pt-BR') AS PL_Formatado,
    FORMAT(r.CaptacaoLiquidaTotal, 'N2', 'pt-BR') AS Captacao_Formatada
FROM esg.FatoResumoMensalESG r
LEFT JOIN esg.DimCategoriaESG c ON r.CategoriaESGID = c.CategoriaESGID;
GO

-- ============================================================================
-- VIEW: TOP FUNDOS ESG POR PL
-- ============================================================================
IF OBJECT_ID('reports.vw_TopFundosESG', 'V') IS NOT NULL DROP VIEW reports.vw_TopFundosESG;
GO

CREATE VIEW reports.vw_TopFundosESG AS
SELECT
    ROW_NUMBER() OVER (ORDER BY pl.PatrimonioLiquido DESC) AS Ranking,
    f.FundoNome,
    g.GestoraNome,
    ce.CategoriaNome AS CategoriaESG,
    fe.FocoNome AS FocoESG,
    pl.PatrimonioLiquido,
    pl.CotistasTotal,
    pl.RentabilidadeAno,
    t.AnoMes
FROM fundos.FatoPatrimonioLiquido pl
INNER JOIN fundos.FatoFundo f ON pl.FundoID = f.FundoID
INNER JOIN fundos.DimTempo t ON pl.DataID = t.DataID
LEFT JOIN fundos.DimGestora g ON f.GestoraID = g.GestoraID
LEFT JOIN esg.DimCategoriaESG ce ON f.CategoriaESGID = ce.CategoriaESGID
LEFT JOIN esg.DimFocoESG fe ON f.FocoESGID = fe.FocoESGID
WHERE f.CategoriaESGID IS NOT NULL
    AND f.Ativo = 1
    AND t.DataID = (SELECT MAX(DataID) FROM fundos.FatoPatrimonioLiquido);
GO

-- ============================================================================
-- VIEW: EVOLUCAO PL POR CATEGORIA ESG
-- ============================================================================
IF OBJECT_ID('reports.vw_EvolucaoPLCategoria', 'V') IS NOT NULL DROP VIEW reports.vw_EvolucaoPLCategoria;
GO

CREATE VIEW reports.vw_EvolucaoPLCategoria AS
SELECT
    t.AnoMes,
    t.Ano,
    t.Mes,
    ce.CategoriaNome,
    ce.Cor,
    SUM(pl.PatrimonioLiquido) AS PatrimonioTotal,
    COUNT(DISTINCT pl.FundoID) AS TotalFundos,
    SUM(pl.CotistasTotal) AS TotalCotistas,
    SUM(pl.CaptacaoLiquida) AS CaptacaoTotal
FROM fundos.FatoPatrimonioLiquido pl
INNER JOIN fundos.FatoFundo f ON pl.FundoID = f.FundoID
INNER JOIN fundos.DimTempo t ON pl.DataID = t.DataID
LEFT JOIN esg.DimCategoriaESG ce ON f.CategoriaESGID = ce.CategoriaESGID
WHERE f.Ativo = 1
GROUP BY t.AnoMes, t.Ano, t.Mes, ce.CategoriaNome, ce.Cor;
GO

-- ============================================================================
-- VIEW: FUNDOS POR FOCO ESG
-- ============================================================================
IF OBJECT_ID('reports.vw_FundosPorFocoESG', 'V') IS NOT NULL DROP VIEW reports.vw_FundosPorFocoESG;
GO

CREATE VIEW reports.vw_FundosPorFocoESG AS
SELECT
    fe.FocoNome,
    fe.FocoDescricao,
    fe.Cor,
    COUNT(f.FundoID) AS TotalFundos,
    SUM(CASE WHEN f.Ativo = 1 THEN 1 ELSE 0 END) AS FundosAtivos
FROM fundos.FatoFundo f
INNER JOIN esg.DimFocoESG fe ON f.FocoESGID = fe.FocoESGID
GROUP BY fe.FocoNome, fe.FocoDescricao, fe.Cor;
GO

-- ============================================================================
-- VIEW: FUNDOS COM ODS
-- ============================================================================
IF OBJECT_ID('reports.vw_FundosODS', 'V') IS NOT NULL DROP VIEW reports.vw_FundosODS;
GO

CREATE VIEW reports.vw_FundosODS AS
SELECT
    f.FundoNome,
    g.GestoraNome,
    o.ODSID,
    o.ODSNome,
    o.ODSCor,
    bo.Relevancia,
    bo.Percentual
FROM esg.BridgeFundoODS bo
INNER JOIN fundos.FatoFundo f ON bo.FundoID = f.FundoID
INNER JOIN esg.DimODS o ON bo.ODSID = o.ODSID
LEFT JOIN fundos.DimGestora g ON f.GestoraID = g.GestoraID
WHERE f.Ativo = 1;
GO

-- ============================================================================
-- VIEW: DASHBOARD PRINCIPAL - KPIs
-- ============================================================================
IF OBJECT_ID('reports.vw_DashboardKPIs', 'V') IS NOT NULL DROP VIEW reports.vw_DashboardKPIs;
GO

CREATE VIEW reports.vw_DashboardKPIs AS
SELECT
    -- Total de fundos ESG
    (SELECT COUNT(*) FROM fundos.FatoFundo WHERE CategoriaESGID IS NOT NULL AND Ativo = 1) AS TotalFundosESG,
    -- Total de fundos IS
    (SELECT COUNT(*) FROM fundos.FatoFundo WHERE SufixoIS = 1 AND Ativo = 1) AS TotalFundosIS,
    -- Patrimonio Total ESG (ultima data)
    (SELECT SUM(pl.PatrimonioLiquido)
     FROM fundos.FatoPatrimonioLiquido pl
     INNER JOIN fundos.FatoFundo f ON pl.FundoID = f.FundoID
     WHERE f.CategoriaESGID IS NOT NULL
       AND f.Ativo = 1
       AND pl.DataID = (SELECT MAX(DataID) FROM fundos.FatoPatrimonioLiquido)
    ) AS PatrimonioTotalESG,
    -- Total Cotistas ESG
    (SELECT SUM(pl.CotistasTotal)
     FROM fundos.FatoPatrimonioLiquido pl
     INNER JOIN fundos.FatoFundo f ON pl.FundoID = f.FundoID
     WHERE f.CategoriaESGID IS NOT NULL
       AND f.Ativo = 1
       AND pl.DataID = (SELECT MAX(DataID) FROM fundos.FatoPatrimonioLiquido)
    ) AS TotalCotistasESG,
    -- Gestoras com fundos ESG
    (SELECT COUNT(DISTINCT GestoraID) FROM fundos.FatoFundo WHERE CategoriaESGID IS NOT NULL AND Ativo = 1) AS TotalGestorasESG;
GO

PRINT 'Views para Dashboard criadas com sucesso!'
GO
