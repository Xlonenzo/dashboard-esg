-- ============================================================================
-- CRIAR DATABASE ESG_BV (Versao Simplificada)
-- Execute este script no SQL Server Management Studio conectado ao 'master'
-- ============================================================================

USE master;
GO

-- Verifica se ja existe e dropa se necessario (CUIDADO: apaga dados!)
-- IF EXISTS (SELECT name FROM sys.databases WHERE name = 'ESG_BV')
-- BEGIN
--     ALTER DATABASE ESG_BV SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
--     DROP DATABASE ESG_BV;
-- END
-- GO

-- Cria o database com configuracoes padrao
IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = 'ESG_BV')
BEGIN
    CREATE DATABASE ESG_BV;
    PRINT 'Database ESG_BV criado!'
END
ELSE
BEGIN
    PRINT 'Database ESG_BV ja existe.'
END
GO

USE ESG_BV;
GO

PRINT 'Conectado ao ESG_BV. Execute agora os scripts 01 a 06.'
GO
