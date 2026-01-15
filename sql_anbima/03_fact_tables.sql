-- ============================================================================
-- BANCO DE DADOS ANBIMA ESG
-- Script 03: Tabelas de Fato
-- ============================================================================

USE ANBIMA_ESG;
GO

-- ============================================================================
-- FATO: FUNDO DE INVESTIMENTO
-- Cadastro completo dos fundos
-- ============================================================================
IF OBJECT_ID('fundos.FatoFundo', 'U') IS NOT NULL DROP TABLE fundos.FatoFundo;
GO

CREATE TABLE fundos.FatoFundo (
    FundoID INT IDENTITY(1,1) PRIMARY KEY,
    FundoCNPJ VARCHAR(18) NOT NULL,
    FundoNome NVARCHAR(500) NOT NULL,
    FundoRazaoSocial NVARCHAR(500),
    GestoraID INT,
    AdministradoraID INT,
    ClassificacaoID INT,
    BenchmarkID INT,
    -- Classificacao ESG
    CategoriaESGID INT,
    FocoESGID INT,
    EstrategiaID INT,
    -- Flags ESG
    ESGIntegrado BIT DEFAULT 0,
    SufixoIS BIT DEFAULT 0,
    -- Informacoes gerais
    DataConstituicao DATE,
    DataInicio DATE,
    PublicoAlvo NVARCHAR(200),          -- Investidor Qualificado, Geral, etc.
    TaxaAdministracao DECIMAL(10,4),
    TaxaPerformance DECIMAL(10,4),
    AplicacaoMinima DECIMAL(18,2),
    ResgateMinimo DECIMAL(18,2),
    PrazoCotizacaoResgate INT,
    PrazoLiquidacaoResgate INT,
    -- Status
    Ativo BIT DEFAULT 1,
    Situacao NVARCHAR(50),              -- Em funcionamento, Fechado, etc.
    DataCriacao DATETIME2 DEFAULT GETDATE(),
    DataAtualizacao DATETIME2 DEFAULT GETDATE(),
    -- Foreign Keys
    CONSTRAINT FK_FatoFundo_Gestora FOREIGN KEY (GestoraID) REFERENCES fundos.DimGestora(GestoraID),
    CONSTRAINT FK_FatoFundo_Admin FOREIGN KEY (AdministradoraID) REFERENCES fundos.DimAdministradora(AdministradoraID),
    CONSTRAINT FK_FatoFundo_Class FOREIGN KEY (ClassificacaoID) REFERENCES fundos.DimClassificacaoAnbima(ClassificacaoID),
    CONSTRAINT FK_FatoFundo_Bench FOREIGN KEY (BenchmarkID) REFERENCES fundos.DimBenchmark(BenchmarkID),
    CONSTRAINT FK_FatoFundo_CatESG FOREIGN KEY (CategoriaESGID) REFERENCES esg.DimCategoriaESG(CategoriaESGID),
    CONSTRAINT FK_FatoFundo_FocoESG FOREIGN KEY (FocoESGID) REFERENCES esg.DimFocoESG(FocoESGID),
    CONSTRAINT FK_FatoFundo_Estrat FOREIGN KEY (EstrategiaID) REFERENCES esg.DimEstrategiaESG(EstrategiaID)
);
GO

CREATE UNIQUE INDEX IX_FatoFundo_CNPJ ON fundos.FatoFundo(FundoCNPJ);
CREATE INDEX IX_FatoFundo_Nome ON fundos.FatoFundo(FundoNome);
CREATE INDEX IX_FatoFundo_ESG ON fundos.FatoFundo(CategoriaESGID);
GO

-- ============================================================================
-- FATO: PATRIMONIO LIQUIDO (Serie Temporal)
-- Historico de PL dos fundos
-- ============================================================================
IF OBJECT_ID('fundos.FatoPatrimonioLiquido', 'U') IS NOT NULL DROP TABLE fundos.FatoPatrimonioLiquido;
GO

CREATE TABLE fundos.FatoPatrimonioLiquido (
    PLID BIGINT IDENTITY(1,1) PRIMARY KEY,
    FundoID INT NOT NULL,
    DataID INT NOT NULL,
    PatrimonioLiquido DECIMAL(18,2),
    ValorCota DECIMAL(18,8),
    CotistasTotal INT,
    CaptacaoLiquida DECIMAL(18,2),
    CaptacaoBruta DECIMAL(18,2),
    Resgates DECIMAL(18,2),
    RentabilidadeDia DECIMAL(10,6),
    RentabilidadeMes DECIMAL(10,6),
    RentabilidadeAno DECIMAL(10,6),
    Rentabilidade12M DECIMAL(10,6),
    Rentabilidade24M DECIMAL(10,6),
    Volatilidade12M DECIMAL(10,6),
    SharpeRatio DECIMAL(10,6),
    DataCriacao DATETIME2 DEFAULT GETDATE(),
    CONSTRAINT FK_FatoPL_Fundo FOREIGN KEY (FundoID) REFERENCES fundos.FatoFundo(FundoID),
    CONSTRAINT FK_FatoPL_Tempo FOREIGN KEY (DataID) REFERENCES fundos.DimTempo(DataID)
);
GO

CREATE INDEX IX_FatoPL_Fundo ON fundos.FatoPatrimonioLiquido(FundoID);
CREATE INDEX IX_FatoPL_Data ON fundos.FatoPatrimonioLiquido(DataID);
CREATE INDEX IX_FatoPL_FundoData ON fundos.FatoPatrimonioLiquido(FundoID, DataID);
GO

-- ============================================================================
-- FATO: RESUMO MENSAL ESG
-- Agregacao mensal do mercado ESG
-- ============================================================================
IF OBJECT_ID('esg.FatoResumoMensalESG', 'U') IS NOT NULL DROP TABLE esg.FatoResumoMensalESG;
GO

CREATE TABLE esg.FatoResumoMensalESG (
    ResumoID INT IDENTITY(1,1) PRIMARY KEY,
    AnoMes VARCHAR(7) NOT NULL,          -- YYYY-MM
    Ano INT NOT NULL,
    Mes INT NOT NULL,
    CategoriaESGID INT,
    -- Metricas
    TotalFundos INT,
    PatrimonioLiquidoTotal DECIMAL(18,2),
    CaptacaoLiquidaTotal DECIMAL(18,2),
    TotalCotistas INT,
    -- Variacao
    VariacaoPLMes DECIMAL(10,4),          -- % vs mes anterior
    VariacaoPLAno DECIMAL(10,4),          -- % vs mesmo mes ano anterior
    -- Participacao
    ParticipaoPLMercado DECIMAL(10,4),    -- % do PL total da industria
    DataCriacao DATETIME2 DEFAULT GETDATE(),
    CONSTRAINT FK_ResumoESG_Cat FOREIGN KEY (CategoriaESGID) REFERENCES esg.DimCategoriaESG(CategoriaESGID)
);
GO

CREATE INDEX IX_ResumoESG_AnoMes ON esg.FatoResumoMensalESG(AnoMes);
GO

-- ============================================================================
-- FATO: SCORE ESG DO FUNDO
-- Scores ESG atribuidos aos fundos
-- ============================================================================
IF OBJECT_ID('esg.FatoScoreESG', 'U') IS NOT NULL DROP TABLE esg.FatoScoreESG;
GO

CREATE TABLE esg.FatoScoreESG (
    ScoreID INT IDENTITY(1,1) PRIMARY KEY,
    FundoID INT NOT NULL,
    DataAvaliacao DATE NOT NULL,
    -- Scores (0-100)
    ScoreAmbiental DECIMAL(5,2),
    ScoreSocial DECIMAL(5,2),
    ScoreGovernanca DECIMAL(5,2),
    ScoreESGTotal DECIMAL(5,2),
    -- Rating
    RatingESG VARCHAR(10),               -- AAA, AA, A, BBB, BB, B, CCC, CC, C
    -- Detalhes
    FonteAvaliacao NVARCHAR(200),        -- MSCI, Sustainalytics, Refinitiv, etc.
    Metodologia NVARCHAR(500),
    Observacoes NVARCHAR(1000),
    DataCriacao DATETIME2 DEFAULT GETDATE(),
    CONSTRAINT FK_ScoreESG_Fundo FOREIGN KEY (FundoID) REFERENCES fundos.FatoFundo(FundoID)
);
GO

CREATE INDEX IX_ScoreESG_Fundo ON esg.FatoScoreESG(FundoID);
GO

-- ============================================================================
-- FATO: INDICADORES DE MERCADO
-- Dados historicos de indices de referencia
-- ============================================================================
IF OBJECT_ID('mercado.FatoIndicador', 'U') IS NOT NULL DROP TABLE mercado.FatoIndicador;
GO

CREATE TABLE mercado.FatoIndicador (
    IndicadorID BIGINT IDENTITY(1,1) PRIMARY KEY,
    BenchmarkID INT NOT NULL,
    DataID INT NOT NULL,
    ValorFechamento DECIMAL(18,6),
    ValorAbertura DECIMAL(18,6),
    ValorMaximo DECIMAL(18,6),
    ValorMinimo DECIMAL(18,6),
    VariacaoDia DECIMAL(10,6),
    VariacaoPontos DECIMAL(18,6),
    Volume DECIMAL(18,2),
    DataCriacao DATETIME2 DEFAULT GETDATE(),
    CONSTRAINT FK_Indicador_Bench FOREIGN KEY (BenchmarkID) REFERENCES fundos.DimBenchmark(BenchmarkID),
    CONSTRAINT FK_Indicador_Tempo FOREIGN KEY (DataID) REFERENCES fundos.DimTempo(DataID)
);
GO

CREATE INDEX IX_Indicador_Bench ON mercado.FatoIndicador(BenchmarkID);
CREATE INDEX IX_Indicador_Data ON mercado.FatoIndicador(DataID);
GO

-- ============================================================================
-- BRIDGE: FUNDO x ODS
-- Quais ODS cada fundo contribui
-- ============================================================================
IF OBJECT_ID('esg.BridgeFundoODS', 'U') IS NOT NULL DROP TABLE esg.BridgeFundoODS;
GO

CREATE TABLE esg.BridgeFundoODS (
    BridgeID INT IDENTITY(1,1) PRIMARY KEY,
    FundoID INT NOT NULL,
    ODSID INT NOT NULL,
    Relevancia VARCHAR(20),              -- Primaria, Secundaria
    Percentual DECIMAL(5,2),             -- % do fundo alinhado ao ODS
    DataCriacao DATETIME2 DEFAULT GETDATE(),
    CONSTRAINT FK_BridgeFundoODS_Fundo FOREIGN KEY (FundoID) REFERENCES fundos.FatoFundo(FundoID),
    CONSTRAINT FK_BridgeFundoODS_ODS FOREIGN KEY (ODSID) REFERENCES esg.DimODS(ODSID),
    CONSTRAINT UQ_BridgeFundoODS UNIQUE (FundoID, ODSID)
);
GO

-- ============================================================================
-- BRIDGE: FUNDO x ESTRATEGIA ESG
-- Fundos podem ter multiplas estrategias
-- ============================================================================
IF OBJECT_ID('esg.BridgeFundoEstrategia', 'U') IS NOT NULL DROP TABLE esg.BridgeFundoEstrategia;
GO

CREATE TABLE esg.BridgeFundoEstrategia (
    BridgeID INT IDENTITY(1,1) PRIMARY KEY,
    FundoID INT NOT NULL,
    EstrategiaID INT NOT NULL,
    Principal BIT DEFAULT 0,
    DataCriacao DATETIME2 DEFAULT GETDATE(),
    CONSTRAINT FK_BridgeFundoEstrat_Fundo FOREIGN KEY (FundoID) REFERENCES fundos.FatoFundo(FundoID),
    CONSTRAINT FK_BridgeFundoEstrat_Estrat FOREIGN KEY (EstrategiaID) REFERENCES esg.DimEstrategiaESG(EstrategiaID),
    CONSTRAINT UQ_BridgeFundoEstrat UNIQUE (FundoID, EstrategiaID)
);
GO

PRINT 'Tabelas de Fato criadas com sucesso!'
GO
