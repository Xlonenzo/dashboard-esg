-- ============================================================================
-- BANCO DE DADOS ANBIMA ESG
-- Estrutura para Fundos Sustentaveis e Dados ESG
-- Versao: 1.0
-- ============================================================================

USE master;
GO

-- Verifica se o database ja existe
IF EXISTS (SELECT name FROM sys.databases WHERE name = 'ANBIMA_ESG')
BEGIN
    -- Fecha conexoes ativas
    ALTER DATABASE ANBIMA_ESG SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE ANBIMA_ESG;
END
GO

-- Cria o database
CREATE DATABASE ANBIMA_ESG
ON PRIMARY (
    NAME = 'ANBIMA_ESG_Data',
    FILENAME = 'C:\SQLData\ANBIMA_ESG.mdf',
    SIZE = 200MB,
    MAXSIZE = UNLIMITED,
    FILEGROWTH = 50MB
)
LOG ON (
    NAME = 'ANBIMA_ESG_Log',
    FILENAME = 'C:\SQLData\ANBIMA_ESG_log.ldf',
    SIZE = 100MB,
    MAXSIZE = UNLIMITED,
    FILEGROWTH = 25MB
);
GO

-- Configura o database
USE ANBIMA_ESG;
GO

ALTER DATABASE ANBIMA_ESG SET RECOVERY SIMPLE;
ALTER DATABASE ANBIMA_ESG SET AUTO_SHRINK OFF;
ALTER DATABASE ANBIMA_ESG SET AUTO_CREATE_STATISTICS ON;
ALTER DATABASE ANBIMA_ESG SET AUTO_UPDATE_STATISTICS ON;
GO

PRINT 'Database ANBIMA_ESG criado com sucesso!'
GO
