-- ============================================================================
-- MODELAGEM DE DADOS ESG - BANCO VOTORANTIM
-- Azure SQL Server Database
-- Script 02: Tabelas de Dimensao
-- ============================================================================

-- ============================================================================
-- DIMENSAO: SETOR
-- Setores principais: Energia, Saneamento, Saude, Educacao, Inclusao Digital
-- ============================================================================
IF OBJECT_ID('esg.DimSetor', 'U') IS NOT NULL DROP TABLE esg.DimSetor;
GO

CREATE TABLE esg.DimSetor (
    SetorID INT IDENTITY(1,1) PRIMARY KEY,
    SetorNome NVARCHAR(100) NOT NULL,
    SetorDescricao NVARCHAR(500),
    Ativo BIT DEFAULT 1,
    DataCriacao DATETIME2 DEFAULT GETDATE(),
    DataAtualizacao DATETIME2 DEFAULT GETDATE()
);
GO

-- ============================================================================
-- DIMENSAO: SUBSETOR
-- ============================================================================
IF OBJECT_ID('esg.DimSubSetor', 'U') IS NOT NULL DROP TABLE esg.DimSubSetor;
GO

CREATE TABLE esg.DimSubSetor (
    SubSetorID INT IDENTITY(1,1) PRIMARY KEY,
    SetorID INT NOT NULL,
    SubSetorNome NVARCHAR(200) NOT NULL,
    SubSetorDescricao NVARCHAR(500),
    Ativo BIT DEFAULT 1,
    DataCriacao DATETIME2 DEFAULT GETDATE(),
    DataAtualizacao DATETIME2 DEFAULT GETDATE(),
    CONSTRAINT FK_SubSetor_Setor FOREIGN KEY (SetorID) REFERENCES esg.DimSetor(SetorID)
);
GO

-- ============================================================================
-- DIMENSAO: CATEGORIA
-- Categorias ESG: Green, Social, Sustainable
-- ============================================================================
IF OBJECT_ID('esg.DimCategoria', 'U') IS NOT NULL DROP TABLE esg.DimCategoria;
GO

CREATE TABLE esg.DimCategoria (
    CategoriaID INT IDENTITY(1,1) PRIMARY KEY,
    CategoriaNome NVARCHAR(100) NOT NULL,  -- Green, Social, Sustainable
    CategoriaDescricao NVARCHAR(500),
    CategoriaCor NVARCHAR(7),  -- Hex color para visualizacao
    Ativo BIT DEFAULT 1,
    DataCriacao DATETIME2 DEFAULT GETDATE(),
    DataAtualizacao DATETIME2 DEFAULT GETDATE()
);
GO

-- ============================================================================
-- DIMENSAO: TEMA
-- Temas: Meio Ambiente, Social, Governanca
-- ============================================================================
IF OBJECT_ID('esg.DimTema', 'U') IS NOT NULL DROP TABLE esg.DimTema;
GO

CREATE TABLE esg.DimTema (
    TemaID INT IDENTITY(1,1) PRIMARY KEY,
    TemaNome NVARCHAR(100) NOT NULL,
    TemaDescricao NVARCHAR(500),
    Ativo BIT DEFAULT 1,
    DataCriacao DATETIME2 DEFAULT GETDATE(),
    DataAtualizacao DATETIME2 DEFAULT GETDATE()
);
GO

-- ============================================================================
-- DIMENSAO: EMPRESA
-- Cadastro de empresas da carteira
-- ============================================================================
IF OBJECT_ID('esg.DimEmpresa', 'U') IS NOT NULL DROP TABLE esg.DimEmpresa;
GO

CREATE TABLE esg.DimEmpresa (
    EmpresaID INT IDENTITY(1,1) PRIMARY KEY,
    EmpresaNome NVARCHAR(300) NOT NULL,
    CNPJ VARCHAR(18),  -- Formato: XX.XXX.XXX/XXXX-XX
    CNPJNumerico BIGINT,  -- Apenas numeros para busca
    SetorID INT,
    SubSetorID INT,
    CategoriaID INT,
    TemaID INT,
    CNAEPrincipal INT,
    RazaoSocial NVARCHAR(300),
    Ativo BIT DEFAULT 1,
    DataCriacao DATETIME2 DEFAULT GETDATE(),
    DataAtualizacao DATETIME2 DEFAULT GETDATE(),
    CONSTRAINT FK_Empresa_Setor FOREIGN KEY (SetorID) REFERENCES esg.DimSetor(SetorID),
    CONSTRAINT FK_Empresa_SubSetor FOREIGN KEY (SubSetorID) REFERENCES esg.DimSubSetor(SubSetorID),
    CONSTRAINT FK_Empresa_Categoria FOREIGN KEY (CategoriaID) REFERENCES esg.DimCategoria(CategoriaID),
    CONSTRAINT FK_Empresa_Tema FOREIGN KEY (TemaID) REFERENCES esg.DimTema(TemaID)
);
GO

CREATE INDEX IX_DimEmpresa_CNPJ ON esg.DimEmpresa(CNPJNumerico);
CREATE INDEX IX_DimEmpresa_Setor ON esg.DimEmpresa(SetorID);
GO

-- ============================================================================
-- DIMENSAO: PRODUTO
-- Produtos financeiros (Debenture, Emprestimo, etc.)
-- ============================================================================
IF OBJECT_ID('esg.DimProduto', 'U') IS NOT NULL DROP TABLE esg.DimProduto;
GO

CREATE TABLE esg.DimProduto (
    ProdutoID INT IDENTITY(1,1) PRIMARY KEY,
    ProdutoNome NVARCHAR(200) NOT NULL,
    ProdutoDescricao NVARCHAR(500),
    TipoProduto NVARCHAR(100),
    Ativo BIT DEFAULT 1,
    DataCriacao DATETIME2 DEFAULT GETDATE(),
    DataAtualizacao DATETIME2 DEFAULT GETDATE()
);
GO

-- ============================================================================
-- DIMENSAO: ODS (Objetivos de Desenvolvimento Sustentavel)
-- 17 ODS da ONU
-- ============================================================================
IF OBJECT_ID('esg.DimODS', 'U') IS NOT NULL DROP TABLE esg.DimODS;
GO

CREATE TABLE esg.DimODS (
    ODSID INT PRIMARY KEY,  -- 1 a 17
    ODSNome NVARCHAR(200) NOT NULL,
    ODSDescricao NVARCHAR(1000),
    ODSCor NVARCHAR(7),  -- Hex color oficial
    URLImagem NVARCHAR(500),
    Ativo BIT DEFAULT 1,
    DataCriacao DATETIME2 DEFAULT GETDATE(),
    DataAtualizacao DATETIME2 DEFAULT GETDATE()
);
GO

-- ============================================================================
-- DIMENSAO: META ODS
-- Metas especificas de cada ODS
-- ============================================================================
IF OBJECT_ID('esg.DimMetaODS', 'U') IS NOT NULL DROP TABLE esg.DimMetaODS;
GO

CREATE TABLE esg.DimMetaODS (
    MetaODSID INT IDENTITY(1,1) PRIMARY KEY,
    ODSID INT NOT NULL,
    MetaCodigo NVARCHAR(10) NOT NULL,  -- Ex: 3.8, 6.1, 7.2
    MetaDescricao NVARCHAR(1000),
    MetaDescricaoResumida NVARCHAR(300),
    IndicadorONUSugerido NVARCHAR(500),
    Ativo BIT DEFAULT 1,
    DataCriacao DATETIME2 DEFAULT GETDATE(),
    DataAtualizacao DATETIME2 DEFAULT GETDATE(),
    CONSTRAINT FK_MetaODS_ODS FOREIGN KEY (ODSID) REFERENCES esg.DimODS(ODSID)
);
GO

-- ============================================================================
-- DIMENSAO: TIPO KPI
-- Tipos de indicadores de performance
-- ============================================================================
IF OBJECT_ID('esg.DimTipoKPI', 'U') IS NOT NULL DROP TABLE esg.DimTipoKPI;
GO

CREATE TABLE esg.DimTipoKPI (
    TipoKPIID INT IDENTITY(1,1) PRIMARY KEY,
    SetorID INT,  -- Alguns KPIs sao especificos de setor
    KPINome NVARCHAR(300) NOT NULL,
    KPIDescricao NVARCHAR(1000),
    UnidadeMedida NVARCHAR(100),  -- MW, m3, pessoas, R$, etc.
    TipoValor NVARCHAR(50),  -- Numerico, Percentual, Monetario, Texto
    Ativo BIT DEFAULT 1,
    DataCriacao DATETIME2 DEFAULT GETDATE(),
    DataAtualizacao DATETIME2 DEFAULT GETDATE(),
    CONSTRAINT FK_TipoKPI_Setor FOREIGN KEY (SetorID) REFERENCES esg.DimSetor(SetorID)
);
GO

-- ============================================================================
-- DIMENSAO: TEMPO
-- Dimensao temporal para analises
-- ============================================================================
IF OBJECT_ID('esg.DimTempo', 'U') IS NOT NULL DROP TABLE esg.DimTempo;
GO

CREATE TABLE esg.DimTempo (
    DataID INT PRIMARY KEY,  -- YYYYMMDD
    Data DATE NOT NULL,
    Ano INT NOT NULL,
    Trimestre INT NOT NULL,
    Mes INT NOT NULL,
    MesNome NVARCHAR(20),
    Semestre INT,
    AnoMes VARCHAR(7),  -- YYYY-MM
    AnoTrimestre VARCHAR(7),  -- YYYY-Q#
    Ativo BIT DEFAULT 1
);
GO

CREATE INDEX IX_DimTempo_Ano ON esg.DimTempo(Ano);
GO

-- ============================================================================
-- DIMENSAO: CNAE
-- Classificacao Nacional de Atividades Economicas
-- ============================================================================
IF OBJECT_ID('esg.DimCNAE', 'U') IS NOT NULL DROP TABLE esg.DimCNAE;
GO

CREATE TABLE esg.DimCNAE (
    CNAEID INT PRIMARY KEY,  -- Codigo CNAE
    CNAEDescricao NVARCHAR(500),
    Divisao NVARCHAR(200),
    Grupo NVARCHAR(200),
    Classe NVARCHAR(200),
    Subclasse NVARCHAR(200),
    -- Mapeamento BV
    ClasseBV NVARCHAR(200),
    SubSetorBV NVARCHAR(200),
    SetorBV NVARCHAR(200),
    ProjetoBV NVARCHAR(200),
    CategoriaBV NVARCHAR(200),
    -- Mapeamento IBGE
    ProjetoIBGE NVARCHAR(200),
    CategoriaIBGE NVARCHAR(200),
    MacroIBGE NVARCHAR(200),
    Observacoes NVARCHAR(1000),
    Ativo BIT DEFAULT 1,
    DataCriacao DATETIME2 DEFAULT GETDATE(),
    DataAtualizacao DATETIME2 DEFAULT GETDATE()
);
GO

PRINT 'Tabelas de Dimensao criadas com sucesso!'
GO
