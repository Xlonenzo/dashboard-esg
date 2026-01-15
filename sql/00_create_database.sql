-- ============================================================================
-- MODELAGEM DE DADOS ESG - BANCO VOTORANTIM
-- Script para criar o Database (executar no master)
--
-- INSTRUCOES:
-- 1. Abra o SQL Server Management Studio (SSMS)
-- 2. Conecte ao seu servidor local
-- 3. Execute este script conectado ao database 'master'
-- ============================================================================

USE master;
GO

-- Verifica se o database ja existe
IF EXISTS (SELECT name FROM sys.databases WHERE name = 'ESG_BV')
BEGIN
    PRINT 'Database ESG_BV ja existe.'
END
ELSE
BEGIN
    -- Cria o database
    CREATE DATABASE ESG_BV
    ON PRIMARY (
        NAME = 'ESG_BV_Data',
        FILENAME = 'C:\SQLData\ESG_BV.mdf',  -- Ajuste o caminho conforme necessario
        SIZE = 100MB,
        MAXSIZE = UNLIMITED,
        FILEGROWTH = 10MB
    )
    LOG ON (
        NAME = 'ESG_BV_Log',
        FILENAME = 'C:\SQLData\ESG_BV_log.ldf',  -- Ajuste o caminho conforme necessario
        SIZE = 50MB,
        MAXSIZE = UNLIMITED,
        FILEGROWTH = 10MB
    );

    PRINT 'Database ESG_BV criado com sucesso!'
END
GO

-- Alterna para o novo database
USE ESG_BV;
GO

-- Configura o database
ALTER DATABASE ESG_BV SET RECOVERY SIMPLE;
ALTER DATABASE ESG_BV SET AUTO_SHRINK OFF;
ALTER DATABASE ESG_BV SET AUTO_CREATE_STATISTICS ON;
GO

PRINT 'Database configurado. Agora execute os scripts 01 a 06.'
GO
