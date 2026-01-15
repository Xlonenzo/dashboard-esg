-- ============================================================================
-- BANCO DE DADOS ANBIMA ESG
-- Script 07: Tabelas de Dimensao TSB (Taxonomia Sustentavel Brasileira)
-- ============================================================================

USE ANBIMA_ESG;
GO

-- Criar schema TSB se nao existir
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'tsb')
BEGIN
    EXEC('CREATE SCHEMA tsb');
END
GO

PRINT 'Criando tabelas de dimensao TSB...'
GO

-- ============================================================================
-- DIMENSAO: SETOR TSB
-- Setores da Taxonomia Sustentavel Brasileira
-- ============================================================================
IF OBJECT_ID('tsb.DimSetorTSB', 'U') IS NOT NULL DROP TABLE tsb.DimSetorTSB;
GO

CREATE TABLE tsb.DimSetorTSB (
    SetorTSBID INT IDENTITY(1,1) PRIMARY KEY,
    SetorCodigo VARCHAR(20) NOT NULL,
    SetorNome NVARCHAR(100) NOT NULL,
    SetorDescricao NVARCHAR(500),
    CNAEsPrincipais NVARCHAR(500),          -- Lista de CNAEs do setor
    ElegivelVerde BIT DEFAULT 1,
    Nota NVARCHAR(500),                      -- Observacoes especificas
    Cor NVARCHAR(7),                         -- Hex color para dashboard
    Icone NVARCHAR(50),
    Ativo BIT DEFAULT 1,
    DataCriacao DATETIME2 DEFAULT GETDATE(),
    DataAtualizacao DATETIME2 DEFAULT GETDATE()
);
GO

CREATE INDEX IX_DimSetorTSB_Codigo ON tsb.DimSetorTSB(SetorCodigo);
GO

-- ============================================================================
-- DIMENSAO: OBJETIVO TSB
-- 11 Objetivos da Taxonomia Sustentavel Brasileira
-- ============================================================================
IF OBJECT_ID('tsb.DimObjetivoTSB', 'U') IS NOT NULL DROP TABLE tsb.DimObjetivoTSB;
GO

CREATE TABLE tsb.DimObjetivoTSB (
    ObjetivoTSBID INT PRIMARY KEY,           -- 1 a 11
    ObjetivoNome NVARCHAR(200) NOT NULL,
    ObjetivoDescricao NVARCHAR(1000),
    TipoObjetivo NVARCHAR(50) NOT NULL,      -- Ambiental, Social
    Cor NVARCHAR(7),
    Icone NVARCHAR(50),
    Ativo BIT DEFAULT 1,
    DataCriacao DATETIME2 DEFAULT GETDATE()
);
GO

-- ============================================================================
-- DIMENSAO: CLASSIFICACAO TSB
-- Niveis de classificacao (VERDE, TRANSICAO, etc)
-- ============================================================================
IF OBJECT_ID('tsb.DimClassificacaoTSB', 'U') IS NOT NULL DROP TABLE tsb.DimClassificacaoTSB;
GO

CREATE TABLE tsb.DimClassificacaoTSB (
    ClassificacaoTSBID INT IDENTITY(1,1) PRIMARY KEY,
    ClassificacaoCodigo VARCHAR(20) NOT NULL,
    ClassificacaoNome NVARCHAR(100) NOT NULL,
    ClassificacaoDescricao NVARCHAR(500),
    ScoreMinimo INT,
    ScoreMaximo INT,
    Elegivel BIT DEFAULT 0,
    Cor NVARCHAR(7),
    Ordenacao INT,
    Ativo BIT DEFAULT 1,
    DataCriacao DATETIME2 DEFAULT GETDATE()
);
GO

-- ============================================================================
-- DIMENSAO: KPI TSB
-- KPIs Obrigatorios por Setor
-- ============================================================================
IF OBJECT_ID('tsb.DimKPITSB', 'U') IS NOT NULL DROP TABLE tsb.DimKPITSB;
GO

CREATE TABLE tsb.DimKPITSB (
    KPIID INT IDENTITY(1,1) PRIMARY KEY,
    KPICodigo VARCHAR(10) NOT NULL,          -- E01, S01, T01, etc
    KPINome NVARCHAR(200) NOT NULL,
    KPIDescricao NVARCHAR(500),
    SetorTSBID INT NOT NULL,
    Unidade NVARCHAR(50) NOT NULL,           -- tCO2e, MW, MWh, m3, %, etc
    Frequencia NVARCHAR(50) DEFAULT 'Anual',
    Obrigatorio BIT DEFAULT 1,
    LimiteMinimo DECIMAL(18,4),
    LimiteMaximo DECIMAL(18,4),
    MetaTSB NVARCHAR(200),                   -- Meta especifica da TSB
    FonteDados NVARCHAR(200),                -- Onde buscar o dado
    Ordenacao INT,
    Ativo BIT DEFAULT 1,
    DataCriacao DATETIME2 DEFAULT GETDATE(),
    CONSTRAINT FK_KPITSB_Setor FOREIGN KEY (SetorTSBID) REFERENCES tsb.DimSetorTSB(SetorTSBID)
);
GO

CREATE INDEX IX_DimKPITSB_Codigo ON tsb.DimKPITSB(KPICodigo);
CREATE INDEX IX_DimKPITSB_Setor ON tsb.DimKPITSB(SetorTSBID);
GO

-- ============================================================================
-- DIMENSAO: SALVAGUARDA TSB
-- Salvaguardas Minimas Obrigatorias
-- ============================================================================
IF OBJECT_ID('tsb.DimSalvaguardaTSB', 'U') IS NOT NULL DROP TABLE tsb.DimSalvaguardaTSB;
GO

CREATE TABLE tsb.DimSalvaguardaTSB (
    SalvaguardaID INT IDENTITY(1,1) PRIMARY KEY,
    SalvaguardaCodigo VARCHAR(20) NOT NULL,  -- SAL01, SAL02, etc
    SalvaguardaNome NVARCHAR(200) NOT NULL,
    SalvaguardaDescricao NVARCHAR(500),
    TipoSalvaguarda NVARCHAR(50),            -- Geral, Setorial
    SetorTSBID INT,                           -- NULL = aplica a todos
    Verificacao NVARCHAR(200),               -- Como verificar
    Obrigatoria BIT DEFAULT 1,
    Ordenacao INT,
    Ativo BIT DEFAULT 1,
    DataCriacao DATETIME2 DEFAULT GETDATE(),
    CONSTRAINT FK_Salvaguarda_Setor FOREIGN KEY (SetorTSBID) REFERENCES tsb.DimSetorTSB(SetorTSBID)
);
GO

-- ============================================================================
-- DIMENSAO: CRITERIO ELEGIBILIDADE TSB
-- Criterios de elegibilidade por setor
-- ============================================================================
IF OBJECT_ID('tsb.DimCriterioTSB', 'U') IS NOT NULL DROP TABLE tsb.DimCriterioTSB;
GO

CREATE TABLE tsb.DimCriterioTSB (
    CriterioID INT IDENTITY(1,1) PRIMARY KEY,
    SetorTSBID INT NOT NULL,
    CriterioNome NVARCHAR(200) NOT NULL,
    CriterioDescricao NVARCHAR(500),
    Ordenacao INT,
    Ativo BIT DEFAULT 1,
    DataCriacao DATETIME2 DEFAULT GETDATE(),
    CONSTRAINT FK_Criterio_Setor FOREIGN KEY (SetorTSBID) REFERENCES tsb.DimSetorTSB(SetorTSBID)
);
GO

-- ============================================================================
-- DIMENSAO: REQUISITO MRV
-- Requisitos de Monitoramento, Relato e Verificacao
-- ============================================================================
IF OBJECT_ID('tsb.DimRequisitoMRV', 'U') IS NOT NULL DROP TABLE tsb.DimRequisitoMRV;
GO

CREATE TABLE tsb.DimRequisitoMRV (
    RequisitoMRVID INT IDENTITY(1,1) PRIMARY KEY,
    RequisitoCodigo VARCHAR(20) NOT NULL,
    RequisitoNome NVARCHAR(200) NOT NULL,
    RequisitoDescricao NVARCHAR(500),
    Obrigatorio BIT DEFAULT 1,
    Ordenacao INT,
    Ativo BIT DEFAULT 1,
    DataCriacao DATETIME2 DEFAULT GETDATE()
);
GO

-- ============================================================================
-- DIMENSAO: FASE IMPLEMENTACAO TSB
-- Cronograma de implementacao
-- ============================================================================
IF OBJECT_ID('tsb.DimFaseImplementacao', 'U') IS NOT NULL DROP TABLE tsb.DimFaseImplementacao;
GO

CREATE TABLE tsb.DimFaseImplementacao (
    FaseID INT IDENTITY(1,1) PRIMARY KEY,
    FaseNome NVARCHAR(100) NOT NULL,
    FaseDescricao NVARCHAR(500),
    DataInicio DATE,
    DataFim DATE,
    Obrigatoria BIT DEFAULT 0,
    Status NVARCHAR(50),                     -- Concluida, Em Vigor, Planejada
    Ordenacao INT,
    Ativo BIT DEFAULT 1,
    DataCriacao DATETIME2 DEFAULT GETDATE()
);
GO

PRINT 'Tabelas de dimensao TSB criadas com sucesso!'
GO
