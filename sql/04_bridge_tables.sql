-- ============================================================================
-- MODELAGEM DE DADOS ESG - BANCO VOTORANTIM
-- Azure SQL Server Database
-- Script 04: Tabelas Bridge e Auxiliares
-- ============================================================================

-- ============================================================================
-- BRIDGE: KPI x ODS
-- Relacionamento N:N entre KPIs e ODS
-- ============================================================================
IF OBJECT_ID('esg.BridgeKPIODS', 'U') IS NOT NULL DROP TABLE esg.BridgeKPIODS;
GO

CREATE TABLE esg.BridgeKPIODS (
    BridgeID INT IDENTITY(1,1) PRIMARY KEY,
    TipoKPIID INT NOT NULL,
    ODSID INT NOT NULL,
    TipoRelacao VARCHAR(20) NOT NULL,  -- Primaria, Secundaria
    MetaODSID INT,  -- Meta especifica do ODS
    Observacao NVARCHAR(500),
    DataCriacao DATETIME2 DEFAULT GETDATE(),
    CONSTRAINT FK_BridgeKPIODS_TipoKPI FOREIGN KEY (TipoKPIID) REFERENCES esg.DimTipoKPI(TipoKPIID),
    CONSTRAINT FK_BridgeKPIODS_ODS FOREIGN KEY (ODSID) REFERENCES esg.DimODS(ODSID),
    CONSTRAINT FK_BridgeKPIODS_MetaODS FOREIGN KEY (MetaODSID) REFERENCES esg.DimMetaODS(MetaODSID),
    CONSTRAINT UQ_BridgeKPIODS UNIQUE (TipoKPIID, ODSID, TipoRelacao)
);
GO

-- ============================================================================
-- BRIDGE: EMPRESA x CNAE
-- Empresas podem ter multiplos CNAEs
-- ============================================================================
IF OBJECT_ID('esg.BridgeEmpresaCNAE', 'U') IS NOT NULL DROP TABLE esg.BridgeEmpresaCNAE;
GO

CREATE TABLE esg.BridgeEmpresaCNAE (
    BridgeID INT IDENTITY(1,1) PRIMARY KEY,
    EmpresaID INT NOT NULL,
    CNAEID INT NOT NULL,
    TipoCNAE VARCHAR(20) NOT NULL,  -- Principal, Secundario
    DataCriacao DATETIME2 DEFAULT GETDATE(),
    CONSTRAINT FK_BridgeEmpCNAE_Empresa FOREIGN KEY (EmpresaID) REFERENCES esg.DimEmpresa(EmpresaID),
    CONSTRAINT FK_BridgeEmpCNAE_CNAE FOREIGN KEY (CNAEID) REFERENCES esg.DimCNAE(CNAEID),
    CONSTRAINT UQ_BridgeEmpresaCNAE UNIQUE (EmpresaID, CNAEID)
);
GO

-- ============================================================================
-- BRIDGE: EMPRESA x ODS
-- Empresas podem contribuir para multiplas ODS
-- ============================================================================
IF OBJECT_ID('esg.BridgeEmpresaODS', 'U') IS NOT NULL DROP TABLE esg.BridgeEmpresaODS;
GO

CREATE TABLE esg.BridgeEmpresaODS (
    BridgeID INT IDENTITY(1,1) PRIMARY KEY,
    EmpresaID INT NOT NULL,
    ODSID INT NOT NULL,
    TipoContribuicao VARCHAR(20),  -- Direta, Indireta
    Observacao NVARCHAR(500),
    DataCriacao DATETIME2 DEFAULT GETDATE(),
    CONSTRAINT FK_BridgeEmpODS_Empresa FOREIGN KEY (EmpresaID) REFERENCES esg.DimEmpresa(EmpresaID),
    CONSTRAINT FK_BridgeEmpODS_ODS FOREIGN KEY (ODSID) REFERENCES esg.DimODS(ODSID),
    CONSTRAINT UQ_BridgeEmpresaODS UNIQUE (EmpresaID, ODSID)
);
GO

-- ============================================================================
-- TABELA: VALIDACAO EMPRESA (Regras de Conformidade)
-- Baseado no arquivo Carteira_Saneamento_4Regras
-- ============================================================================
IF OBJECT_ID('esg.ValidacaoEmpresa', 'U') IS NOT NULL DROP TABLE esg.ValidacaoEmpresa;
GO

CREATE TABLE esg.ValidacaoEmpresa (
    ValidacaoID INT IDENTITY(1,1) PRIMARY KEY,
    EmpresaID INT NOT NULL,
    AnoReferencia INT NOT NULL,
    -- Regras de Conformidade
    CategoriaGSS NVARCHAR(100),  -- Green, Social, Sustainable
    TaxonomiaFEBRABAN_OK BIT,
    CNAE_OK BIT,
    Exclusao BIT,  -- Se empresa esta em lista de exclusao
    Conforme BIT,  -- Se atende todas as regras
    -- Evidencias
    EvidenciaCategoria NVARCHAR(1000),
    EvidenciaTaxonomia NVARCHAR(1000),
    EvidenciaCNAE NVARCHAR(1000),
    EvidenciaExclusao NVARCHAR(1000),
    -- Auditoria
    DataValidacao DATETIME2,
    ValidadoPor NVARCHAR(200),
    DataCriacao DATETIME2 DEFAULT GETDATE(),
    DataAtualizacao DATETIME2 DEFAULT GETDATE(),
    CONSTRAINT FK_Validacao_Empresa FOREIGN KEY (EmpresaID) REFERENCES esg.DimEmpresa(EmpresaID)
);
GO

CREATE INDEX IX_Validacao_Empresa ON esg.ValidacaoEmpresa(EmpresaID);
CREATE INDEX IX_Validacao_Conforme ON esg.ValidacaoEmpresa(Conforme);
GO

-- ============================================================================
-- TABELA: LOG DE IMPORTACAO
-- Controle de importacoes de dados
-- ============================================================================
IF OBJECT_ID('esg.LogImportacao', 'U') IS NOT NULL DROP TABLE esg.LogImportacao;
GO

CREATE TABLE esg.LogImportacao (
    LogID INT IDENTITY(1,1) PRIMARY KEY,
    TabelaDestino NVARCHAR(100) NOT NULL,
    ArquivoOrigem NVARCHAR(500),
    DataImportacao DATETIME2 DEFAULT GETDATE(),
    RegistrosImportados INT,
    RegistrosComErro INT,
    Status VARCHAR(20),  -- Sucesso, Erro, Parcial
    MensagemErro NVARCHAR(MAX),
    UsuarioImportacao NVARCHAR(200)
);
GO

-- ============================================================================
-- TABELA: INDICADOR FRAMEWORK
-- Mapeamento de indicadores ESG para frameworks de financas sustentaveis
-- ============================================================================
IF OBJECT_ID('esg.IndicadorFramework', 'U') IS NOT NULL DROP TABLE esg.IndicadorFramework;
GO

CREATE TABLE esg.IndicadorFramework (
    IndicadorID INT IDENTITY(1,1) PRIMARY KEY,
    Indicador NVARCHAR(100),
    Setor NVARCHAR(50),
    GreenBond CHAR(1),
    SocialBond CHAR(1),
    SLB CHAR(1),
    SLL CHAR(1),
    TaxonomiaBR CHAR(1),
    TaxonomiaFEBRABAN CHAR(1),
    BlueEconomy CHAR(1),
    FrameworkBV CHAR(1)
);
GO

INSERT INTO esg.IndicadorFramework (Indicador, Setor, GreenBond, SocialBond, SLB, SLL, TaxonomiaBR, TaxonomiaFEBRABAN, BlueEconomy, FrameworkBV) VALUES
-- SANEAMENTO
('VolumeAguaTratada','Saneamento','X','','X','X','X','X','X','X'),
('VolumeEsgotoTratado','Saneamento','X','','X','X','X','X','X','X'),
('PopulacaoAtendidaAgua','Saneamento','X','X','X','X','X','X','X','X'),
('PopulacaoAtendidaEsgoto','Saneamento','X','X','X','X','X','X','X','X'),
('InstalacoesAdicionadas','Saneamento','X','','X','X','X','X','X','X'),
-- SAUDE
('VagasUnidadesSaude','Saude','','X','X','X','X','','','X'),
('PacientesAtendidos','Saude','','X','X','X','X','','','X'),
('AumentoCapacidadeLeitos','Saude','','X','X','X','X','','','X'),
('ReducaoDensidade','Saude','','X','X','X','X','','','X'),
('ReducaoCustoTratamentos','Saude','','X','X','X','X','','','X'),
('LeitosAdicionados','Saude','','X','X','X','X','','','X'),
('PacientesBeneficiados','Saude','','X','X','X','X','','','X'),
-- ENERGIA
('CapacidadeInstaladaMW','Energia','X','','X','X','X','X','','X'),
('EnergiaRenovavelMW','Energia','X','','X','X','X','X','','X'),
('PercentualRenovavel','Energia','X','','X','X','X','X','','X'),
('EmissoesEvitadasTCO2','Energia','X','','X','X','X','X','','X');
GO

PRINT 'Tabelas Bridge e Auxiliares criadas com sucesso!'
GO
