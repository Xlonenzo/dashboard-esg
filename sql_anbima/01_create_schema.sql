-- ============================================================================
-- BANCO DE DADOS ANBIMA ESG
-- Script 01: Criacao de Schemas
-- ============================================================================

USE ANBIMA_ESG;
GO

-- Schema para dados de fundos
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'fundos')
    EXEC('CREATE SCHEMA fundos');
GO

-- Schema para dados ESG
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'esg')
    EXEC('CREATE SCHEMA esg');
GO

-- Schema para dados de mercado
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'mercado')
    EXEC('CREATE SCHEMA mercado');
GO

-- Schema para staging (ETL)
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'stg')
    EXEC('CREATE SCHEMA stg');
GO

-- Schema para views e reports
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'reports')
    EXEC('CREATE SCHEMA reports');
GO

PRINT 'Schemas criados com sucesso!'
GO
