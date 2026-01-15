-- ============================================================================
-- MODELAGEM DE DADOS ESG - BANCO VOTORANTIM
-- Azure SQL Server Database
-- Script 03: Tabelas de Fato
-- ============================================================================

-- ============================================================================
-- FATO: CARTEIRA
-- Valores de carteira ESG por empresa
-- ============================================================================
IF OBJECT_ID('esg.FatoCarteira', 'U') IS NOT NULL DROP TABLE esg.FatoCarteira;
GO

CREATE TABLE esg.FatoCarteira (
    CarteiraID INT IDENTITY(1,1) PRIMARY KEY,
    EmpresaID INT NOT NULL,
    ProdutoID INT,
    SetorID INT,
    SubSetorID INT,
    CategoriaID INT,
    TemaID INT,
    DataID INT,  -- Referencia DimTempo
    AnoReferencia INT NOT NULL,
    ValorCarteira DECIMAL(18,2),  -- Valor em R$
    ValorCarteiraUSD DECIMAL(18,2),  -- Valor em USD (opcional)
    StatusLeitura VARCHAR(20),  -- Lido, Nao Lido
    Observacoes NVARCHAR(1000),
    DataCriacao DATETIME2 DEFAULT GETDATE(),
    DataAtualizacao DATETIME2 DEFAULT GETDATE(),
    CONSTRAINT FK_FatoCarteira_Empresa FOREIGN KEY (EmpresaID) REFERENCES esg.DimEmpresa(EmpresaID),
    CONSTRAINT FK_FatoCarteira_Produto FOREIGN KEY (ProdutoID) REFERENCES esg.DimProduto(ProdutoID),
    CONSTRAINT FK_FatoCarteira_Setor FOREIGN KEY (SetorID) REFERENCES esg.DimSetor(SetorID),
    CONSTRAINT FK_FatoCarteira_SubSetor FOREIGN KEY (SubSetorID) REFERENCES esg.DimSubSetor(SubSetorID),
    CONSTRAINT FK_FatoCarteira_Categoria FOREIGN KEY (CategoriaID) REFERENCES esg.DimCategoria(CategoriaID),
    CONSTRAINT FK_FatoCarteira_Tema FOREIGN KEY (TemaID) REFERENCES esg.DimTema(TemaID),
    CONSTRAINT FK_FatoCarteira_Tempo FOREIGN KEY (DataID) REFERENCES esg.DimTempo(DataID)
);
GO

CREATE INDEX IX_FatoCarteira_Empresa ON esg.FatoCarteira(EmpresaID);
CREATE INDEX IX_FatoCarteira_Setor ON esg.FatoCarteira(SetorID);
CREATE INDEX IX_FatoCarteira_Ano ON esg.FatoCarteira(AnoReferencia);
GO

-- ============================================================================
-- FATO: KPI (Indicadores de Performance)
-- Valores de KPIs por empresa/periodo
-- ============================================================================
IF OBJECT_ID('esg.FatoKPI', 'U') IS NOT NULL DROP TABLE esg.FatoKPI;
GO

CREATE TABLE esg.FatoKPI (
    KPIID INT IDENTITY(1,1) PRIMARY KEY,
    EmpresaID INT NOT NULL,
    TipoKPIID INT NOT NULL,
    SetorID INT,
    DataID INT,
    AnoReferencia INT NOT NULL,
    ValorNumerico DECIMAL(18,4),
    ValorTexto NVARCHAR(500),  -- Para KPIs qualitativos
    UnidadeMedida NVARCHAR(100),
    VariacaoAnterior DECIMAL(10,4),  -- Variacao vs ano anterior (%)
    FonteDados NVARCHAR(500),
    Observacoes NVARCHAR(1000),
    DataCriacao DATETIME2 DEFAULT GETDATE(),
    DataAtualizacao DATETIME2 DEFAULT GETDATE(),
    CONSTRAINT FK_FatoKPI_Empresa FOREIGN KEY (EmpresaID) REFERENCES esg.DimEmpresa(EmpresaID),
    CONSTRAINT FK_FatoKPI_TipoKPI FOREIGN KEY (TipoKPIID) REFERENCES esg.DimTipoKPI(TipoKPIID),
    CONSTRAINT FK_FatoKPI_Setor FOREIGN KEY (SetorID) REFERENCES esg.DimSetor(SetorID),
    CONSTRAINT FK_FatoKPI_Tempo FOREIGN KEY (DataID) REFERENCES esg.DimTempo(DataID)
);
GO

CREATE INDEX IX_FatoKPI_Empresa ON esg.FatoKPI(EmpresaID);
CREATE INDEX IX_FatoKPI_TipoKPI ON esg.FatoKPI(TipoKPIID);
CREATE INDEX IX_FatoKPI_Ano ON esg.FatoKPI(AnoReferencia);
GO

-- ============================================================================
-- FATO: META 2030
-- Metas e progressos para 2030
-- ============================================================================
IF OBJECT_ID('esg.FatoMeta2030', 'U') IS NOT NULL DROP TABLE esg.FatoMeta2030;
GO

CREATE TABLE esg.FatoMeta2030 (
    Meta2030ID INT IDENTITY(1,1) PRIMARY KEY,
    Indicador NVARCHAR(300) NOT NULL,
    AnoReferencia INT NOT NULL,
    ValorMeta DECIMAL(18,2),
    ValorRealizado DECIMAL(18,2),
    PercentualAtingido DECIMAL(10,4),
    CrescimentoYoY DECIMAL(10,4),  -- Year over Year
    UnidadeMedida NVARCHAR(100),
    Observacoes NVARCHAR(1000),
    DataCriacao DATETIME2 DEFAULT GETDATE(),
    DataAtualizacao DATETIME2 DEFAULT GETDATE()
);
GO

-- ============================================================================
-- FATO: INDICADORES SANEAMENTO
-- KPIs especificos do setor de saneamento
-- ============================================================================
IF OBJECT_ID('esg.FatoIndicadorSaneamento', 'U') IS NOT NULL DROP TABLE esg.FatoIndicadorSaneamento;
GO

CREATE TABLE esg.FatoIndicadorSaneamento (
    IndicadorID INT IDENTITY(1,1) PRIMARY KEY,
    EmpresaID INT NOT NULL,
    AnoReferencia INT NOT NULL,
    VolumeAguaTratada DECIMAL(18,2),  -- m3
    VolumeAguaSalva DECIMAL(18,2),  -- m3
    VolumeAguaReduzida DECIMAL(18,2),  -- m3
    VolumeEsgotoTratado DECIMAL(18,2),  -- m3
    PopulacaoAtendidaAgua BIGINT,
    PopulacaoAtendidaEsgoto BIGINT,
    InstalacoesAdicionadas INT,
    ValorCarteira DECIMAL(18,2),
    FonteDados NVARCHAR(500),
    Observacoes NVARCHAR(1000),
    DataCriacao DATETIME2 DEFAULT GETDATE(),
    DataAtualizacao DATETIME2 DEFAULT GETDATE(),
    CONSTRAINT FK_FatoSaneamento_Empresa FOREIGN KEY (EmpresaID) REFERENCES esg.DimEmpresa(EmpresaID)
);
GO

CREATE INDEX IX_FatoSaneamento_Empresa ON esg.FatoIndicadorSaneamento(EmpresaID);
CREATE INDEX IX_FatoSaneamento_Ano ON esg.FatoIndicadorSaneamento(AnoReferencia);
GO

-- ============================================================================
-- FATO: INDICADORES SAUDE
-- KPIs especificos do setor de saude
-- ============================================================================
IF OBJECT_ID('esg.FatoIndicadorSaude', 'U') IS NOT NULL DROP TABLE esg.FatoIndicadorSaude;
GO

CREATE TABLE esg.FatoIndicadorSaude (
    IndicadorID INT IDENTITY(1,1) PRIMARY KEY,
    EmpresaID INT NOT NULL,
    AnoReferencia INT NOT NULL,
    VagasUnidadesSaude BIGINT,
    PacientesAtendidos BIGINT,
    AumentoCapacidadeLeitos DECIMAL(18,2),
    ReducaoDensidade DECIMAL(10,4),
    ReducaoCustoTratamentos DECIMAL(18,2),
    LeitosAdicionados INT,
    PacientesBeneficiados BIGINT,
    ValorCarteira DECIMAL(18,2),
    FonteDados NVARCHAR(500),
    Observacoes NVARCHAR(1000),
    DataCriacao DATETIME2 DEFAULT GETDATE(),
    DataAtualizacao DATETIME2 DEFAULT GETDATE(),
    CONSTRAINT FK_FatoSaude_Empresa FOREIGN KEY (EmpresaID) REFERENCES esg.DimEmpresa(EmpresaID)
);
GO

CREATE INDEX IX_FatoSaude_Empresa ON esg.FatoIndicadorSaude(EmpresaID);
CREATE INDEX IX_FatoSaude_Ano ON esg.FatoIndicadorSaude(AnoReferencia);
GO

-- ============================================================================
-- FATO: INDICADORES ENERGIA
-- KPIs especificos do setor de energia
-- ============================================================================
IF OBJECT_ID('esg.FatoIndicadorEnergia', 'U') IS NOT NULL DROP TABLE esg.FatoIndicadorEnergia;
GO

CREATE TABLE esg.FatoIndicadorEnergia (
    IndicadorID INT IDENTITY(1,1) PRIMARY KEY,
    EmpresaID INT NOT NULL,
    AnoReferencia INT NOT NULL,
    CapacidadeInstaladaMW DECIMAL(18,4),
    EnergiaRenovavelMW DECIMAL(18,4),
    PercentualRenovavel DECIMAL(10,4),
    EmissoesEvitadasTCO2 DECIMAL(18,4),  -- Toneladas CO2 evitadas
    EficienciaEnergetica DECIMAL(10,4),
    ReducaoConsumo DECIMAL(10,4),  -- Percentual
    ValorCarteira DECIMAL(18,2),
    FonteDados NVARCHAR(500),
    Observacoes NVARCHAR(1000),
    DataCriacao DATETIME2 DEFAULT GETDATE(),
    DataAtualizacao DATETIME2 DEFAULT GETDATE(),
    CONSTRAINT FK_FatoEnergia_Empresa FOREIGN KEY (EmpresaID) REFERENCES esg.DimEmpresa(EmpresaID)
);
GO

CREATE INDEX IX_FatoEnergia_Empresa ON esg.FatoIndicadorEnergia(EmpresaID);
CREATE INDEX IX_FatoEnergia_Ano ON esg.FatoIndicadorEnergia(AnoReferencia);
GO

PRINT 'Tabelas de Fato criadas com sucesso!'
GO
