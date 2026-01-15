-- ============================================================================
-- BANCO DE DADOS ANBIMA ESG
-- Script 06: Popular Dimensao Tempo
-- ============================================================================

USE ANBIMA_ESG;
GO

-- Limpa tabela
TRUNCATE TABLE fundos.DimTempo;
GO

-- Popula DimTempo de 2020 a 2030
DECLARE @DataInicio DATE = '2020-01-01';
DECLARE @DataFim DATE = '2030-12-31';
DECLARE @Data DATE = @DataInicio;

WHILE @Data <= @DataFim
BEGIN
    INSERT INTO fundos.DimTempo (
        DataID,
        Data,
        Ano,
        Trimestre,
        Mes,
        MesNome,
        MesAbrev,
        Semestre,
        DiaSemana,
        DiaSemanaName,
        AnoMes,
        AnoTrimestre,
        DiaUtil
    )
    VALUES (
        CONVERT(INT, FORMAT(@Data, 'yyyyMMdd')),
        @Data,
        YEAR(@Data),
        DATEPART(QUARTER, @Data),
        MONTH(@Data),
        DATENAME(MONTH, @Data),
        LEFT(DATENAME(MONTH, @Data), 3),
        CASE WHEN MONTH(@Data) <= 6 THEN 1 ELSE 2 END,
        DATEPART(WEEKDAY, @Data),
        DATENAME(WEEKDAY, @Data),
        FORMAT(@Data, 'yyyy-MM'),
        CONCAT(YEAR(@Data), '-Q', DATEPART(QUARTER, @Data)),
        CASE WHEN DATEPART(WEEKDAY, @Data) IN (1, 7) THEN 0 ELSE 1 END
    );

    SET @Data = DATEADD(DAY, 1, @Data);
END;
GO

PRINT 'Dimensao Tempo populada com sucesso!'
GO

-- Verifica
SELECT
    MIN(Data) AS DataInicio,
    MAX(Data) AS DataFim,
    COUNT(*) AS TotalDias
FROM fundos.DimTempo;
GO
