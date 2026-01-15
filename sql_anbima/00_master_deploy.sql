-- ============================================================================
-- BANCO DE DADOS ANBIMA ESG
-- SCRIPT MASTER DE DEPLOY
--
-- Execute este script para criar toda a estrutura do banco de dados
-- Execute no SQL Server Management Studio conectado ao 'master'
-- ============================================================================

PRINT '============================================================'
PRINT 'DEPLOY DO BANCO DE DADOS ANBIMA ESG'
PRINT 'Iniciando em: ' + CONVERT(VARCHAR, GETDATE(), 120)
PRINT '============================================================'
PRINT ''

-- ============================================================================
-- PASSO 1: CRIAR DATABASE
-- ============================================================================
PRINT '[1/6] Criando Database...'

USE master;
GO

IF EXISTS (SELECT name FROM sys.databases WHERE name = 'ANBIMA_ESG')
BEGIN
    PRINT '  -> Database ja existe, recriando...'
    ALTER DATABASE ANBIMA_ESG SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE ANBIMA_ESG;
END

CREATE DATABASE ANBIMA_ESG;
GO

ALTER DATABASE ANBIMA_ESG SET RECOVERY SIMPLE;
GO

PRINT '  -> Database ANBIMA_ESG criado!'
PRINT ''

-- ============================================================================
-- PASSO 2: CRIAR SCHEMAS
-- ============================================================================
USE ANBIMA_ESG;
GO

PRINT '[2/6] Criando Schemas...'

IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'fundos')
    EXEC('CREATE SCHEMA fundos');

IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'esg')
    EXEC('CREATE SCHEMA esg');

IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'mercado')
    EXEC('CREATE SCHEMA mercado');

IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'stg')
    EXEC('CREATE SCHEMA stg');

IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'reports')
    EXEC('CREATE SCHEMA reports');

PRINT '  -> Schemas criados: fundos, esg, mercado, stg, reports'
PRINT ''
GO

-- ============================================================================
-- PASSO 3: CRIAR TABELAS DE DIMENSAO
-- ============================================================================
PRINT '[3/6] Criando Tabelas de Dimensao...'
GO

-- DimTempo
CREATE TABLE fundos.DimTempo (
    DataID INT PRIMARY KEY,
    Data DATE NOT NULL,
    Ano INT NOT NULL,
    Trimestre INT NOT NULL,
    Mes INT NOT NULL,
    MesNome NVARCHAR(20),
    MesAbrev NVARCHAR(3),
    Semestre INT,
    DiaSemana INT,
    DiaSemanaName NVARCHAR(20),
    AnoMes VARCHAR(7),
    AnoTrimestre VARCHAR(7),
    DiaUtil BIT DEFAULT 1
);
CREATE INDEX IX_DimTempo_Ano ON fundos.DimTempo(Ano);
GO

-- DimGestora
CREATE TABLE fundos.DimGestora (
    GestoraID INT IDENTITY(1,1) PRIMARY KEY,
    GestoraCodigo VARCHAR(20),
    GestoraNome NVARCHAR(300) NOT NULL,
    GestoraCNPJ VARCHAR(18),
    GestoraRazaoSocial NVARCHAR(300),
    TipoGestora NVARCHAR(100),
    Website NVARCHAR(500),
    Ativo BIT DEFAULT 1,
    DataCriacao DATETIME2 DEFAULT GETDATE(),
    DataAtualizacao DATETIME2 DEFAULT GETDATE()
);
GO

-- DimAdministradora
CREATE TABLE fundos.DimAdministradora (
    AdministradoraID INT IDENTITY(1,1) PRIMARY KEY,
    AdministradoraCodigo VARCHAR(20),
    AdministradoraNome NVARCHAR(300) NOT NULL,
    AdministradoraCNPJ VARCHAR(18),
    Ativo BIT DEFAULT 1,
    DataCriacao DATETIME2 DEFAULT GETDATE(),
    DataAtualizacao DATETIME2 DEFAULT GETDATE()
);
GO

-- DimClassificacaoAnbima
CREATE TABLE fundos.DimClassificacaoAnbima (
    ClassificacaoID INT IDENTITY(1,1) PRIMARY KEY,
    ClassificacaoNivel1 NVARCHAR(100),
    ClassificacaoNivel2 NVARCHAR(100),
    ClassificacaoNivel3 NVARCHAR(200),
    ClassificacaoCompleta NVARCHAR(500),
    Descricao NVARCHAR(1000),
    Ativo BIT DEFAULT 1,
    DataCriacao DATETIME2 DEFAULT GETDATE()
);
GO

-- DimBenchmark
CREATE TABLE fundos.DimBenchmark (
    BenchmarkID INT IDENTITY(1,1) PRIMARY KEY,
    BenchmarkCodigo VARCHAR(50),
    BenchmarkNome NVARCHAR(200) NOT NULL,
    BenchmarkDescricao NVARCHAR(500),
    TipoBenchmark NVARCHAR(100),
    Ativo BIT DEFAULT 1
);

INSERT INTO fundos.DimBenchmark (BenchmarkCodigo, BenchmarkNome, TipoBenchmark) VALUES
('CDI', 'CDI', 'Renda Fixa'),
('IBOV', 'Ibovespa', 'Renda Variavel'),
('IBRX', 'IBrX-100', 'Renda Variavel'),
('IPCA', 'IPCA', 'Inflacao'),
('IMAB', 'IMA-B', 'Renda Fixa'),
('ISE', 'ISE B3', 'ESG'),
('ICO2', 'ICO2 B3', 'ESG');
GO

-- DimCategoriaESG
CREATE TABLE esg.DimCategoriaESG (
    CategoriaESGID INT IDENTITY(1,1) PRIMARY KEY,
    CategoriaNome NVARCHAR(100) NOT NULL,
    CategoriaDescricao NVARCHAR(500),
    SufixoIS BIT DEFAULT 0,
    Cor NVARCHAR(7),
    Ordenacao INT,
    Ativo BIT DEFAULT 1
);

INSERT INTO esg.DimCategoriaESG (CategoriaNome, CategoriaDescricao, SufixoIS, Cor, Ordenacao) VALUES
('IS - Investimento Sustentavel', 'Fundos com objetivo 100% sustentavel - usam sufixo IS no nome', 1, '#2E7D32', 1),
('ESG Integrado', 'Fundos que integram questoes ESG na gestao', 0, '#1976D2', 2),
('Convencional', 'Fundos sem integracao ESG declarada', 0, '#757575', 3);
GO

-- DimFocoESG
CREATE TABLE esg.DimFocoESG (
    FocoESGID INT IDENTITY(1,1) PRIMARY KEY,
    FocoNome NVARCHAR(100) NOT NULL,
    FocoDescricao NVARCHAR(500),
    Cor NVARCHAR(7),
    Icone NVARCHAR(100),
    Ativo BIT DEFAULT 1
);

INSERT INTO esg.DimFocoESG (FocoNome, FocoDescricao, Cor) VALUES
('Ambiental', 'Foco em mudanca climatica, transicao energetica', '#4CAF50'),
('Social', 'Foco em impacto social, inclusao', '#FF9800'),
('Governanca', 'Foco em governanca corporativa', '#9C27B0'),
('Multi-tema', 'Abordagem integrada ESG', '#2196F3');
GO

-- DimEstrategiaESG
CREATE TABLE esg.DimEstrategiaESG (
    EstrategiaID INT IDENTITY(1,1) PRIMARY KEY,
    EstrategiaNome NVARCHAR(200) NOT NULL,
    EstrategiaDescricao NVARCHAR(1000),
    Exemplo NVARCHAR(500),
    Ativo BIT DEFAULT 1
);

INSERT INTO esg.DimEstrategiaESG (EstrategiaNome, EstrategiaDescricao) VALUES
('Exclusao/Screening Negativo', 'Exclui setores ou empresas que nao atendem criterios ESG'),
('Best-in-Class', 'Seleciona empresas com melhor desempenho ESG em cada setor'),
('Integracao ESG', 'Incorpora fatores ESG na analise financeira'),
('Investimento Tematico', 'Foca em temas especificos de sustentabilidade'),
('Investimento de Impacto', 'Busca gerar impacto social/ambiental mensuravel'),
('Engajamento Acionario', 'Usa participacao acionaria para influenciar empresas');
GO

-- DimODS
CREATE TABLE esg.DimODS (
    ODSID INT PRIMARY KEY,
    ODSNome NVARCHAR(200) NOT NULL,
    ODSDescricao NVARCHAR(1000),
    ODSCor NVARCHAR(7),
    URLIcone NVARCHAR(500),
    Ativo BIT DEFAULT 1
);

INSERT INTO esg.DimODS (ODSID, ODSNome, ODSCor) VALUES
(1, 'Erradicacao da Pobreza', '#E5243B'),
(2, 'Fome Zero', '#DDA63A'),
(3, 'Saude e Bem-Estar', '#4C9F38'),
(4, 'Educacao de Qualidade', '#C5192D'),
(5, 'Igualdade de Genero', '#FF3A21'),
(6, 'Agua Potavel e Saneamento', '#26BDE2'),
(7, 'Energia Limpa', '#FCC30B'),
(8, 'Trabalho Decente', '#A21942'),
(9, 'Industria e Inovacao', '#FD6925'),
(10, 'Reducao das Desigualdades', '#DD1367'),
(11, 'Cidades Sustentaveis', '#FD9D24'),
(12, 'Consumo Responsavel', '#BF8B2E'),
(13, 'Acao Climatica', '#3F7E44'),
(14, 'Vida na Agua', '#0A97D9'),
(15, 'Vida Terrestre', '#56C02B'),
(16, 'Paz e Justica', '#00689D'),
(17, 'Parcerias', '#19486A');
GO

PRINT '  -> Tabelas de Dimensao criadas!'
PRINT ''

-- ============================================================================
-- PASSO 4: CRIAR TABELAS DE FATO
-- ============================================================================
PRINT '[4/6] Criando Tabelas de Fato...'
GO

-- FatoFundo
CREATE TABLE fundos.FatoFundo (
    FundoID INT IDENTITY(1,1) PRIMARY KEY,
    FundoCNPJ VARCHAR(18) NOT NULL,
    FundoNome NVARCHAR(500) NOT NULL,
    FundoRazaoSocial NVARCHAR(500),
    GestoraID INT,
    AdministradoraID INT,
    ClassificacaoID INT,
    BenchmarkID INT,
    CategoriaESGID INT,
    FocoESGID INT,
    EstrategiaID INT,
    ESGIntegrado BIT DEFAULT 0,
    SufixoIS BIT DEFAULT 0,
    DataConstituicao DATE,
    DataInicio DATE,
    PublicoAlvo NVARCHAR(200),
    TaxaAdministracao DECIMAL(10,4),
    TaxaPerformance DECIMAL(10,4),
    AplicacaoMinima DECIMAL(18,2),
    Ativo BIT DEFAULT 1,
    Situacao NVARCHAR(50),
    DataCriacao DATETIME2 DEFAULT GETDATE(),
    DataAtualizacao DATETIME2 DEFAULT GETDATE(),
    CONSTRAINT FK_FatoFundo_Gestora FOREIGN KEY (GestoraID) REFERENCES fundos.DimGestora(GestoraID),
    CONSTRAINT FK_FatoFundo_Admin FOREIGN KEY (AdministradoraID) REFERENCES fundos.DimAdministradora(AdministradoraID),
    CONSTRAINT FK_FatoFundo_Class FOREIGN KEY (ClassificacaoID) REFERENCES fundos.DimClassificacaoAnbima(ClassificacaoID),
    CONSTRAINT FK_FatoFundo_CatESG FOREIGN KEY (CategoriaESGID) REFERENCES esg.DimCategoriaESG(CategoriaESGID),
    CONSTRAINT FK_FatoFundo_FocoESG FOREIGN KEY (FocoESGID) REFERENCES esg.DimFocoESG(FocoESGID)
);
CREATE UNIQUE INDEX IX_FatoFundo_CNPJ ON fundos.FatoFundo(FundoCNPJ);
GO

-- FatoPatrimonioLiquido
CREATE TABLE fundos.FatoPatrimonioLiquido (
    PLID BIGINT IDENTITY(1,1) PRIMARY KEY,
    FundoID INT NOT NULL,
    DataID INT NOT NULL,
    PatrimonioLiquido DECIMAL(18,2),
    ValorCota DECIMAL(18,8),
    CotistasTotal INT,
    CaptacaoLiquida DECIMAL(18,2),
    RentabilidadeDia DECIMAL(10,6),
    RentabilidadeMes DECIMAL(10,6),
    RentabilidadeAno DECIMAL(10,6),
    Rentabilidade12M DECIMAL(10,6),
    Volatilidade12M DECIMAL(10,6),
    SharpeRatio DECIMAL(10,6),
    DataCriacao DATETIME2 DEFAULT GETDATE(),
    CONSTRAINT FK_FatoPL_Fundo FOREIGN KEY (FundoID) REFERENCES fundos.FatoFundo(FundoID),
    CONSTRAINT FK_FatoPL_Tempo FOREIGN KEY (DataID) REFERENCES fundos.DimTempo(DataID)
);
CREATE INDEX IX_FatoPL_Fundo ON fundos.FatoPatrimonioLiquido(FundoID);
CREATE INDEX IX_FatoPL_Data ON fundos.FatoPatrimonioLiquido(DataID);
GO

-- FatoResumoMensalESG
CREATE TABLE esg.FatoResumoMensalESG (
    ResumoID INT IDENTITY(1,1) PRIMARY KEY,
    AnoMes VARCHAR(7) NOT NULL,
    Ano INT NOT NULL,
    Mes INT NOT NULL,
    CategoriaESGID INT,
    TotalFundos INT,
    PatrimonioLiquidoTotal DECIMAL(18,2),
    CaptacaoLiquidaTotal DECIMAL(18,2),
    TotalCotistas INT,
    VariacaoPLMes DECIMAL(10,4),
    VariacaoPLAno DECIMAL(10,4),
    ParticipaoPLMercado DECIMAL(10,4),
    DataCriacao DATETIME2 DEFAULT GETDATE(),
    CONSTRAINT FK_ResumoESG_Cat FOREIGN KEY (CategoriaESGID) REFERENCES esg.DimCategoriaESG(CategoriaESGID)
);
GO

-- BridgeFundoODS
CREATE TABLE esg.BridgeFundoODS (
    BridgeID INT IDENTITY(1,1) PRIMARY KEY,
    FundoID INT NOT NULL,
    ODSID INT NOT NULL,
    Relevancia VARCHAR(20),
    Percentual DECIMAL(5,2),
    DataCriacao DATETIME2 DEFAULT GETDATE(),
    CONSTRAINT FK_BridgeFundoODS_Fundo FOREIGN KEY (FundoID) REFERENCES fundos.FatoFundo(FundoID),
    CONSTRAINT FK_BridgeFundoODS_ODS FOREIGN KEY (ODSID) REFERENCES esg.DimODS(ODSID)
);
GO

-- Tabelas de Staging
CREATE TABLE stg.FundosAnbima (
    ID INT IDENTITY(1,1) PRIMARY KEY,
    CNPJ VARCHAR(20),
    Nome NVARCHAR(500),
    Gestora NVARCHAR(300),
    ClassificacaoAnbima NVARCHAR(500),
    PatrimonioLiquido VARCHAR(50),
    ESGFlag VARCHAR(50),
    DataExtracao DATETIME2 DEFAULT GETDATE(),
    Processado BIT DEFAULT 0
);
GO

CREATE TABLE stg.LogImportacao (
    LogID INT IDENTITY(1,1) PRIMARY KEY,
    FonteDados NVARCHAR(100) NOT NULL,
    TabelaDestino NVARCHAR(100) NOT NULL,
    DataInicioExtracao DATETIME2,
    DataFimExtracao DATETIME2,
    RegistrosExtraidos INT,
    RegistrosInseridos INT,
    Status VARCHAR(20),
    MensagemErro NVARCHAR(MAX)
);
GO

PRINT '  -> Tabelas de Fato criadas!'
PRINT ''

-- ============================================================================
-- PASSO 5: POPULAR DIMENSAO TEMPO
-- ============================================================================
PRINT '[5/6] Populando Dimensao Tempo...'
GO

DECLARE @DataInicio DATE = '2020-01-01';
DECLARE @DataFim DATE = '2030-12-31';
DECLARE @Data DATE = @DataInicio;

WHILE @Data <= @DataFim
BEGIN
    INSERT INTO fundos.DimTempo (DataID, Data, Ano, Trimestre, Mes, MesNome, MesAbrev,
        Semestre, DiaSemana, DiaSemanaName, AnoMes, AnoTrimestre, DiaUtil)
    VALUES (
        CONVERT(INT, FORMAT(@Data, 'yyyyMMdd')),
        @Data,
        YEAR(@Data),
        DATEPART(QUARTER, @Data),
        MONTH(@Data),
        DATENAME(MONTH, @Data),
        LEFT(DATENAME(MONTH, @Data), 3),
        CASE WHEN MONTH(@Data) <= 6 THEN 1 ELSE 2 END,
        DATEPART(WEEKDAY, @Data),
        DATENAME(WEEKDAY, @Data),
        FORMAT(@Data, 'yyyy-MM'),
        CONCAT(YEAR(@Data), '-Q', DATEPART(QUARTER, @Data)),
        CASE WHEN DATEPART(WEEKDAY, @Data) IN (1, 7) THEN 0 ELSE 1 END
    );
    SET @Data = DATEADD(DAY, 1, @Data);
END;
GO

PRINT '  -> Dimensao Tempo populada (2020-2030)!'
PRINT ''

-- ============================================================================
-- PASSO 6: CRIAR VIEWS
-- ============================================================================
PRINT '[6/6] Criando Views para Dashboard...'
GO

CREATE VIEW reports.vw_FundosESG AS
SELECT
    f.FundoID,
    f.FundoCNPJ,
    f.FundoNome,
    g.GestoraNome,
    c.ClassificacaoNivel1 AS TipoFundo,
    ce.CategoriaNome AS CategoriaESG,
    ce.Cor AS CorCategoriaESG,
    fe.FocoNome AS FocoESG,
    f.ESGIntegrado,
    f.SufixoIS,
    f.TaxaAdministracao,
    f.Situacao,
    f.Ativo
FROM fundos.FatoFundo f
LEFT JOIN fundos.DimGestora g ON f.GestoraID = g.GestoraID
LEFT JOIN fundos.DimClassificacaoAnbima c ON f.ClassificacaoID = c.ClassificacaoID
LEFT JOIN esg.DimCategoriaESG ce ON f.CategoriaESGID = ce.CategoriaESGID
LEFT JOIN esg.DimFocoESG fe ON f.FocoESGID = fe.FocoESGID
WHERE f.Ativo = 1;
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
    r.ParticipaoPLMercado
FROM esg.FatoResumoMensalESG r
LEFT JOIN esg.DimCategoriaESG c ON r.CategoriaESGID = c.CategoriaESGID;
GO

CREATE VIEW reports.vw_DashboardKPIs AS
SELECT
    (SELECT COUNT(*) FROM fundos.FatoFundo WHERE CategoriaESGID IS NOT NULL AND Ativo = 1) AS TotalFundosESG,
    (SELECT COUNT(*) FROM fundos.FatoFundo WHERE SufixoIS = 1 AND Ativo = 1) AS TotalFundosIS,
    (SELECT COUNT(DISTINCT GestoraID) FROM fundos.FatoFundo WHERE CategoriaESGID IS NOT NULL) AS TotalGestorasESG;
GO

PRINT '  -> Views criadas!'
PRINT ''

-- ============================================================================
-- CONCLUSAO
-- ============================================================================
PRINT '============================================================'
PRINT 'DEPLOY CONCLUIDO COM SUCESSO!'
PRINT 'Finalizado em: ' + CONVERT(VARCHAR, GETDATE(), 120)
PRINT '============================================================'
PRINT ''
PRINT 'Estrutura criada:'
PRINT '  - 5 Schemas: fundos, esg, mercado, stg, reports'
PRINT '  - 12 Tabelas de Dimensao'
PRINT '  - 5 Tabelas de Fato'
PRINT '  - 2 Tabelas de Staging'
PRINT '  - 3 Views para Dashboard'
PRINT '  - DimTempo populada: 2020-2030'
PRINT ''
PRINT 'Proximos passos:'
PRINT '  1. Execute o scraper Python: python scraper_anbima.py'
PRINT '  2. Execute o ETL: python etl_sql_server.py'
PRINT '  3. Abra o dashboard: dashboard_anbima_esg.html'
GO
