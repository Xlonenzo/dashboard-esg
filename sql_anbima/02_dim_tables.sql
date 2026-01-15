-- ============================================================================
-- BANCO DE DADOS ANBIMA ESG
-- Script 02: Tabelas de Dimensao
-- ============================================================================

USE ANBIMA_ESG;
GO

-- ============================================================================
-- DIMENSAO: TEMPO
-- ============================================================================
IF OBJECT_ID('fundos.DimTempo', 'U') IS NOT NULL DROP TABLE fundos.DimTempo;
GO

CREATE TABLE fundos.DimTempo (
    DataID INT PRIMARY KEY,              -- YYYYMMDD
    Data DATE NOT NULL,
    Ano INT NOT NULL,
    Trimestre INT NOT NULL,
    Mes INT NOT NULL,
    MesNome NVARCHAR(20),
    MesAbrev NVARCHAR(3),
    Semestre INT,
    DiaSemana INT,
    DiaSemanaName NVARCHAR(20),
    AnoMes VARCHAR(7),                   -- YYYY-MM
    AnoTrimestre VARCHAR(7),             -- YYYY-QN
    DiaUtil BIT DEFAULT 1
);
GO

CREATE INDEX IX_DimTempo_Ano ON fundos.DimTempo(Ano);
CREATE INDEX IX_DimTempo_Data ON fundos.DimTempo(Data);
GO

-- ============================================================================
-- DIMENSAO: GESTORA
-- ============================================================================
IF OBJECT_ID('fundos.DimGestora', 'U') IS NOT NULL DROP TABLE fundos.DimGestora;
GO

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

CREATE INDEX IX_DimGestora_CNPJ ON fundos.DimGestora(GestoraCNPJ);
GO

-- ============================================================================
-- DIMENSAO: ADMINISTRADORA
-- ============================================================================
IF OBJECT_ID('fundos.DimAdministradora', 'U') IS NOT NULL DROP TABLE fundos.DimAdministradora;
GO

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

-- ============================================================================
-- DIMENSAO: CLASSIFICACAO ANBIMA
-- ============================================================================
IF OBJECT_ID('fundos.DimClassificacaoAnbima', 'U') IS NOT NULL DROP TABLE fundos.DimClassificacaoAnbima;
GO

CREATE TABLE fundos.DimClassificacaoAnbima (
    ClassificacaoID INT IDENTITY(1,1) PRIMARY KEY,
    ClassificacaoNivel1 NVARCHAR(100),   -- Ex: Renda Fixa, Renda Variavel, Multimercado
    ClassificacaoNivel2 NVARCHAR(100),   -- Ex: Indexados, Ativos, Investimento no Exterior
    ClassificacaoNivel3 NVARCHAR(200),   -- Ex: Soberano, Credito Privado
    ClassificacaoCompleta NVARCHAR(500),
    Descricao NVARCHAR(1000),
    Ativo BIT DEFAULT 1,
    DataCriacao DATETIME2 DEFAULT GETDATE()
);
GO

-- ============================================================================
-- DIMENSAO: CATEGORIA ESG
-- ============================================================================
IF OBJECT_ID('esg.DimCategoriaESG', 'U') IS NOT NULL DROP TABLE esg.DimCategoriaESG;
GO

CREATE TABLE esg.DimCategoriaESG (
    CategoriaESGID INT IDENTITY(1,1) PRIMARY KEY,
    CategoriaNome NVARCHAR(100) NOT NULL,    -- IS (Investimento Sustentavel), ESG Integrado, Convencional
    CategoriaDescricao NVARCHAR(500),
    SufixoIS BIT DEFAULT 0,                   -- Se usa sufixo IS no nome
    Cor NVARCHAR(7),                          -- Hex color
    Ordenacao INT,
    Ativo BIT DEFAULT 1
);
GO

INSERT INTO esg.DimCategoriaESG (CategoriaNome, CategoriaDescricao, SufixoIS, Cor, Ordenacao) VALUES
('IS - Investimento Sustentavel', 'Fundos com objetivo 100% sustentavel - usam sufixo IS no nome', 1, '#2E7D32', 1),
('ESG Integrado', 'Fundos que integram questoes ESG na gestao, mas nao como objetivo principal', 0, '#1976D2', 2),
('Convencional', 'Fundos sem integracao ESG declarada', 0, '#757575', 3);
GO

-- ============================================================================
-- DIMENSAO: FOCO ESG
-- ============================================================================
IF OBJECT_ID('esg.DimFocoESG', 'U') IS NOT NULL DROP TABLE esg.DimFocoESG;
GO

CREATE TABLE esg.DimFocoESG (
    FocoESGID INT IDENTITY(1,1) PRIMARY KEY,
    FocoNome NVARCHAR(100) NOT NULL,         -- Ambiental, Social, Governanca, Multi-tema
    FocoDescricao NVARCHAR(500),
    Cor NVARCHAR(7),
    Icone NVARCHAR(100),
    Ativo BIT DEFAULT 1
);
GO

INSERT INTO esg.DimFocoESG (FocoNome, FocoDescricao, Cor) VALUES
('Ambiental', 'Foco em mudanca climatica, transicao energetica, biodiversidade', '#4CAF50'),
('Social', 'Foco em impacto social, inclusao, direitos humanos', '#FF9800'),
('Governanca', 'Foco em governanca corporativa, transparencia, etica', '#9C27B0'),
('Multi-tema', 'Abordagem integrada ESG sem foco unico', '#2196F3');
GO

-- ============================================================================
-- DIMENSAO: ESTRATEGIA ESG
-- ============================================================================
IF OBJECT_ID('esg.DimEstrategiaESG', 'U') IS NOT NULL DROP TABLE esg.DimEstrategiaESG;
GO

CREATE TABLE esg.DimEstrategiaESG (
    EstrategiaID INT IDENTITY(1,1) PRIMARY KEY,
    EstrategiaNome NVARCHAR(200) NOT NULL,
    EstrategiaDescricao NVARCHAR(1000),
    Exemplo NVARCHAR(500),
    Ativo BIT DEFAULT 1
);
GO

INSERT INTO esg.DimEstrategiaESG (EstrategiaNome, EstrategiaDescricao, Exemplo) VALUES
('Exclusao/Screening Negativo', 'Exclui setores ou empresas que nao atendem criterios ESG', 'Excluir empresas de tabaco, armas, combustiveis fosseis'),
('Best-in-Class', 'Seleciona empresas com melhor desempenho ESG em cada setor', 'Escolher petroleiras com melhor score ambiental'),
('Integracao ESG', 'Incorpora fatores ESG na analise financeira tradicional', 'Avaliar riscos climaticos no valuation'),
('Investimento Tematico', 'Foca em temas especificos de sustentabilidade', 'Fundos de energia renovavel, economia circular'),
('Investimento de Impacto', 'Busca gerar impacto social/ambiental mensuravel', 'Fundos de microfinancas, habitacao popular'),
('Engajamento Acionario', 'Usa participacao acionaria para influenciar empresas', 'Votar em assembleias, dialogar com gestao');
GO

-- ============================================================================
-- DIMENSAO: BENCHMARK
-- ============================================================================
IF OBJECT_ID('fundos.DimBenchmark', 'U') IS NOT NULL DROP TABLE fundos.DimBenchmark;
GO

CREATE TABLE fundos.DimBenchmark (
    BenchmarkID INT IDENTITY(1,1) PRIMARY KEY,
    BenchmarkCodigo VARCHAR(50),
    BenchmarkNome NVARCHAR(200) NOT NULL,
    BenchmarkDescricao NVARCHAR(500),
    TipoBenchmark NVARCHAR(100),
    Ativo BIT DEFAULT 1
);
GO

INSERT INTO fundos.DimBenchmark (BenchmarkCodigo, BenchmarkNome, TipoBenchmark) VALUES
('CDI', 'CDI', 'Renda Fixa'),
('IBOV', 'Ibovespa', 'Renda Variavel'),
('IBRX', 'IBrX-100', 'Renda Variavel'),
('IPCA', 'IPCA', 'Inflacao'),
('IMAB', 'IMA-B', 'Renda Fixa'),
('IDIV', 'IDIV', 'Renda Variavel'),
('ISE', 'ISE B3', 'ESG'),
('ICO2', 'ICO2 B3', 'ESG'),
('IGPTW', 'IGPTW B3', 'ESG');
GO

-- ============================================================================
-- DIMENSAO: ODS (Objetivos de Desenvolvimento Sustentavel)
-- ============================================================================
IF OBJECT_ID('esg.DimODS', 'U') IS NOT NULL DROP TABLE esg.DimODS;
GO

CREATE TABLE esg.DimODS (
    ODSID INT PRIMARY KEY,
    ODSNome NVARCHAR(200) NOT NULL,
    ODSDescricao NVARCHAR(1000),
    ODSCor NVARCHAR(7),
    URLIcone NVARCHAR(500),
    Ativo BIT DEFAULT 1
);
GO

INSERT INTO esg.DimODS (ODSID, ODSNome, ODSCor) VALUES
(1, 'Erradicacao da Pobreza', '#E5243B'),
(2, 'Fome Zero e Agricultura Sustentavel', '#DDA63A'),
(3, 'Saude e Bem-Estar', '#4C9F38'),
(4, 'Educacao de Qualidade', '#C5192D'),
(5, 'Igualdade de Genero', '#FF3A21'),
(6, 'Agua Potavel e Saneamento', '#26BDE2'),
(7, 'Energia Limpa e Acessivel', '#FCC30B'),
(8, 'Trabalho Decente e Crescimento Economico', '#A21942'),
(9, 'Industria, Inovacao e Infraestrutura', '#FD6925'),
(10, 'Reducao das Desigualdades', '#DD1367'),
(11, 'Cidades e Comunidades Sustentaveis', '#FD9D24'),
(12, 'Consumo e Producao Responsaveis', '#BF8B2E'),
(13, 'Acao Contra a Mudanca Global do Clima', '#3F7E44'),
(14, 'Vida na Agua', '#0A97D9'),
(15, 'Vida Terrestre', '#56C02B'),
(16, 'Paz, Justica e Instituicoes Eficazes', '#00689D'),
(17, 'Parcerias e Meios de Implementacao', '#19486A');
GO

PRINT 'Tabelas de Dimensao criadas com sucesso!'
GO
