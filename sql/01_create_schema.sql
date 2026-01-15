-- ============================================================================
-- MODELAGEM DE DADOS ESG - BANCO VOTORANTIM
-- Azure SQL Server Database
-- Script 01: Criacao do Schema
-- ============================================================================

-- Criar schema para organizar as tabelas
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'esg')
BEGIN
    EXEC('CREATE SCHEMA esg')
END
GO

PRINT 'Schema ESG criado com sucesso!'
GO
