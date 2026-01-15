-- ============================================================================
-- BANCO DE DADOS ANBIMA ESG
-- Script 04: Tabelas de Staging (ETL)
-- ============================================================================

USE ANBIMA_ESG;
GO

-- ============================================================================
-- STAGING: DADOS BRUTOS ANBIMA
-- ============================================================================
IF OBJECT_ID('stg.FundosAnbima', 'U') IS NOT NULL DROP TABLE stg.FundosAnbima;
GO

CREATE TABLE stg.FundosAnbima (
    ID INT IDENTITY(1,1) PRIMARY KEY,
    CNPJ VARCHAR(20),
    Nome NVARCHAR(500),
    RazaoSocial NVARCHAR(500),
    Gestora NVARCHAR(300),
    Administradora NVARCHAR(300),
    ClassificacaoAnbima NVARCHAR(500),
    Benchmark NVARCHAR(200),
    DataConstituicao VARCHAR(20),
    PatrimonioLiquido VARCHAR(50),
    CotaValor VARCHAR(50),
    TotalCotistas VARCHAR(20),
    ESGFlag VARCHAR(50),
    Situacao NVARCHAR(100),
    DataExtracao DATETIME2 DEFAULT GETDATE(),
    Processado BIT DEFAULT 0,
    ErroProcessamento NVARCHAR(1000)
);
GO

-- ============================================================================
-- STAGING: PATRIMONIO LIQUIDO BRUTO
-- ============================================================================
IF OBJECT_ID('stg.PatrimonioLiquido', 'U') IS NOT NULL DROP TABLE stg.PatrimonioLiquido;
GO

CREATE TABLE stg.PatrimonioLiquido (
    ID INT IDENTITY(1,1) PRIMARY KEY,
    CNPJ VARCHAR(20),
    DataReferencia VARCHAR(20),
    PatrimonioLiquido VARCHAR(50),
    ValorCota VARCHAR(50),
    CaptacaoLiquida VARCHAR(50),
    TotalCotistas VARCHAR(20),
    DataExtracao DATETIME2 DEFAULT GETDATE(),
    Processado BIT DEFAULT 0
);
GO

-- ============================================================================
-- STAGING: INDICES DE MERCADO
-- ============================================================================
IF OBJECT_ID('stg.IndicesMercado', 'U') IS NOT NULL DROP TABLE stg.IndicesMercado;
GO

CREATE TABLE stg.IndicesMercado (
    ID INT IDENTITY(1,1) PRIMARY KEY,
    Indice VARCHAR(50),
    DataReferencia VARCHAR(20),
    ValorFechamento VARCHAR(50),
    VariacaoDia VARCHAR(20),
    DataExtracao DATETIME2 DEFAULT GETDATE(),
    Processado BIT DEFAULT 0
);
GO

-- ============================================================================
-- LOG: CONTROLE DE IMPORTACOES
-- ============================================================================
IF OBJECT_ID('stg.LogImportacao', 'U') IS NOT NULL DROP TABLE stg.LogImportacao;
GO

CREATE TABLE stg.LogImportacao (
    LogID INT IDENTITY(1,1) PRIMARY KEY,
    FonteDados NVARCHAR(100) NOT NULL,
    TabelaDestino NVARCHAR(100) NOT NULL,
    DataInicioExtracao DATETIME2,
    DataFimExtracao DATETIME2,
    RegistrosExtraidos INT,
    RegistrosInseridos INT,
    RegistrosAtualizados INT,
    RegistrosComErro INT,
    Status VARCHAR(20),                  -- Sucesso, Erro, Parcial
    MensagemErro NVARCHAR(MAX),
    UsuarioExecucao NVARCHAR(100)
);
GO

PRINT 'Tabelas de Staging criadas com sucesso!'
GO
