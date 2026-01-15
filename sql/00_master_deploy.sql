-- ============================================================================
-- MODELAGEM DE DADOS ESG - BANCO VOTORANTIM
-- Azure SQL Server Database
-- Script Master: Executa todos os scripts na ordem correta
-- ============================================================================
--
-- INSTRUCOES DE USO:
-- 1. Conecte-se ao Azure SQL Server
-- 2. Crie um database (ex: CREATE DATABASE ESG_BV)
-- 3. Execute este script no database criado
-- 4. Os scripts serao executados na ordem correta
--
-- ORDEM DE EXECUCAO:
-- 01. Schema
-- 02. Tabelas de Dimensao
-- 03. Tabelas de Fato
-- 04. Tabelas Bridge e Auxiliares
-- 05. Carga Inicial de Dados
-- 06. Views para Power BI
--
-- ============================================================================

PRINT '============================================================'
PRINT 'INICIANDO DEPLOY DA MODELAGEM ESG - BANCO VOTORANTIM'
PRINT 'Data: ' + CONVERT(VARCHAR, GETDATE(), 120)
PRINT '============================================================'
GO

-- Execute os scripts individualmente na ordem:
-- :r 01_create_schema.sql
-- :r 02_dim_tables.sql
-- :r 03_fact_tables.sql
-- :r 04_bridge_tables.sql
-- :r 05_initial_data.sql
-- :r 06_views.sql

PRINT ''
PRINT '============================================================'
PRINT 'DEPLOY CONCLUIDO COM SUCESSO!'
PRINT '============================================================'
PRINT ''
PRINT 'Proximos passos:'
PRINT '1. Verifique se todas as tabelas foram criadas no schema ESG'
PRINT '2. Configure a conexao do Power BI com o Azure SQL'
PRINT '3. Importe os dados dos arquivos Excel para as tabelas'
PRINT '4. Use as Views para criar os visuais no Power BI'
PRINT ''
GO
