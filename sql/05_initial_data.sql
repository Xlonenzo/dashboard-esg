-- ============================================================================
-- MODELAGEM DE DADOS ESG - BANCO VOTORANTIM
-- Azure SQL Server Database
-- Script 05: Carga Inicial de Dados
-- ============================================================================

-- ============================================================================
-- CARGA: SETORES
-- ============================================================================
INSERT INTO esg.DimSetor (SetorNome, SetorDescricao) VALUES
('Energia', 'Setor de geracao, transmissao e distribuicao de energia'),
('Saneamento', 'Setor de agua e saneamento basico'),
('Saude', 'Setor de servicos de saude e bem-estar'),
('Educacao', 'Setor de educacao e formacao profissional'),
('Inclusao Digital', 'Setor de tecnologia e inclusao digital');
GO

-- ============================================================================
-- CARGA: CATEGORIAS ESG
-- ============================================================================
INSERT INTO esg.DimCategoria (CategoriaNome, CategoriaDescricao, CategoriaCor) VALUES
('Green', 'Projetos com foco ambiental - energia renovavel, eficiencia energetica, gestao de residuos', '#28A745'),
('Social', 'Projetos com foco social - saude, educacao, inclusao, habitacao', '#007BFF'),
('Sustainable', 'Projetos que combinam aspectos ambientais e sociais', '#6F42C1');
GO

-- ============================================================================
-- CARGA: TEMAS
-- ============================================================================
INSERT INTO esg.DimTema (TemaNome, TemaDescricao) VALUES
('Meio Ambiente', 'Projetos focados em sustentabilidade ambiental'),
('Social', 'Projetos focados em impacto social'),
('Governanca', 'Projetos focados em governanca corporativa');
GO

-- ============================================================================
-- CARGA: ODS (17 Objetivos de Desenvolvimento Sustentavel)
-- ============================================================================
INSERT INTO esg.DimODS (ODSID, ODSNome, ODSDescricao, ODSCor) VALUES
(1, 'Erradicacao da Pobreza', 'Acabar com a pobreza em todas as suas formas, em todos os lugares', '#E5243B'),
(2, 'Fome Zero', 'Acabar com a fome, alcançar a seguranca alimentar e melhoria da nutricao', '#DDA63A'),
(3, 'Saude e Bem-Estar', 'Assegurar uma vida saudavel e promover o bem-estar para todos', '#4C9F38'),
(4, 'Educacao de Qualidade', 'Assegurar a educacao inclusiva, equitativa e de qualidade', '#C5192D'),
(5, 'Igualdade de Genero', 'Alcancar a igualdade de genero e empoderar todas as mulheres e meninas', '#FF3A21'),
(6, 'Agua Potavel e Saneamento', 'Garantir disponibilidade e gestao sustentavel da agua e saneamento', '#26BDE2'),
(7, 'Energia Limpa e Acessivel', 'Garantir acesso a energia barata, confiavel, sustentavel e renovavel', '#FCC30B'),
(8, 'Trabalho Decente', 'Promover o crescimento economico sustentado, inclusivo e sustentavel', '#A21942'),
(9, 'Industria, Inovacao e Infraestrutura', 'Construir infraestruturas resilientes, promover industrializacao', '#FD6925'),
(10, 'Reducao das Desigualdades', 'Reduzir a desigualdade dentro dos paises e entre eles', '#DD1367'),
(11, 'Cidades e Comunidades Sustentaveis', 'Tornar as cidades e assentamentos humanos inclusivos e sustentaveis', '#FD9D24'),
(12, 'Consumo e Producao Responsaveis', 'Assegurar padroes de producao e de consumo sustentaveis', '#BF8B2E'),
(13, 'Acao Contra Mudanca Global do Clima', 'Tomar medidas urgentes para combater a mudanca climatica', '#3F7E44'),
(14, 'Vida na Agua', 'Conservacao e uso sustentavel dos oceanos, mares e recursos marinhos', '#0A97D9'),
(15, 'Vida Terrestre', 'Proteger, recuperar e promover uso sustentavel dos ecossistemas terrestres', '#56C02B'),
(16, 'Paz, Justica e Instituicoes Eficazes', 'Promover sociedades pacificas e inclusivas', '#00689D'),
(17, 'Parcerias e Meios de Implementacao', 'Fortalecer os meios de implementacao e parcerias globais', '#19486A');
GO

-- ============================================================================
-- CARGA: DIMENSAO TEMPO (2020-2035)
-- ============================================================================
DECLARE @StartDate DATE = '2020-01-01'
DECLARE @EndDate DATE = '2035-12-31'
DECLARE @CurrentDate DATE = @StartDate

WHILE @CurrentDate <= @EndDate
BEGIN
    INSERT INTO esg.DimTempo (DataID, Data, Ano, Trimestre, Mes, MesNome, Semestre, AnoMes, AnoTrimestre)
    VALUES (
        CONVERT(INT, FORMAT(@CurrentDate, 'yyyyMMdd')),
        @CurrentDate,
        YEAR(@CurrentDate),
        DATEPART(QUARTER, @CurrentDate),
        MONTH(@CurrentDate),
        DATENAME(MONTH, @CurrentDate),
        CASE WHEN MONTH(@CurrentDate) <= 6 THEN 1 ELSE 2 END,
        FORMAT(@CurrentDate, 'yyyy-MM'),
        CONCAT(YEAR(@CurrentDate), '-Q', DATEPART(QUARTER, @CurrentDate))
    )
    SET @CurrentDate = DATEADD(DAY, 1, @CurrentDate)
END
GO

-- ============================================================================
-- CARGA: TIPOS DE KPI POR SETOR
-- ============================================================================

-- KPIs de ENERGIA
INSERT INTO esg.DimTipoKPI (SetorID, KPINome, UnidadeMedida, TipoValor) VALUES
((SELECT SetorID FROM esg.DimSetor WHERE SetorNome = 'Energia'), 'Capacidade Instalada de Energia Renovavel', 'MW', 'Numerico'),
((SELECT SetorID FROM esg.DimSetor WHERE SetorNome = 'Energia'), 'Percentual de Energia Renovavel', '%', 'Percentual'),
((SELECT SetorID FROM esg.DimSetor WHERE SetorNome = 'Energia'), 'Emissoes de CO2 Evitadas', 'tCO2e', 'Numerico'),
((SELECT SetorID FROM esg.DimSetor WHERE SetorNome = 'Energia'), 'Reducao no Consumo de Energia', '%', 'Percentual'),
((SELECT SetorID FROM esg.DimSetor WHERE SetorNome = 'Energia'), 'Eficiencia Energetica', '%', 'Percentual'),
((SELECT SetorID FROM esg.DimSetor WHERE SetorNome = 'Energia'), 'Geracao de Energia Solar', 'MWh', 'Numerico'),
((SELECT SetorID FROM esg.DimSetor WHERE SetorNome = 'Energia'), 'Geracao de Energia Eolica', 'MWh', 'Numerico');
GO

-- KPIs de SANEAMENTO
INSERT INTO esg.DimTipoKPI (SetorID, KPINome, UnidadeMedida, TipoValor) VALUES
((SELECT SetorID FROM esg.DimSetor WHERE SetorNome = 'Saneamento'), 'Volume de Agua Tratada', 'm3', 'Numerico'),
((SELECT SetorID FROM esg.DimSetor WHERE SetorNome = 'Saneamento'), 'Volume de Agua Salva/Reduzida', 'm3', 'Numerico'),
((SELECT SetorID FROM esg.DimSetor WHERE SetorNome = 'Saneamento'), 'Volume de Esgoto Tratado', 'm3', 'Numerico'),
((SELECT SetorID FROM esg.DimSetor WHERE SetorNome = 'Saneamento'), 'Populacao Atendida com Agua', 'pessoas', 'Numerico'),
((SELECT SetorID FROM esg.DimSetor WHERE SetorNome = 'Saneamento'), 'Populacao Atendida com Esgoto', 'pessoas', 'Numerico'),
((SELECT SetorID FROM esg.DimSetor WHERE SetorNome = 'Saneamento'), 'Instalacoes de Tratamento Adicionadas', 'unidades', 'Numerico');
GO

-- KPIs de SAUDE
INSERT INTO esg.DimTipoKPI (SetorID, KPINome, UnidadeMedida, TipoValor) VALUES
((SELECT SetorID FROM esg.DimSetor WHERE SetorNome = 'Saude'), 'Vagas em Unidades de Saude', 'vagas', 'Numerico'),
((SELECT SetorID FROM esg.DimSetor WHERE SetorNome = 'Saude'), 'Pacientes Atendidos', 'pacientes', 'Numerico'),
((SELECT SetorID FROM esg.DimSetor WHERE SetorNome = 'Saude'), 'Aumento de Capacidade de Leitos', 'leitos', 'Numerico'),
((SELECT SetorID FROM esg.DimSetor WHERE SetorNome = 'Saude'), 'Reducao de Densidade Hospitalar', '%', 'Percentual'),
((SELECT SetorID FROM esg.DimSetor WHERE SetorNome = 'Saude'), 'Reducao de Custos de Tratamentos', 'R$', 'Monetario'),
((SELECT SetorID FROM esg.DimSetor WHERE SetorNome = 'Saude'), 'Leitos Hospitalares Adicionados', 'leitos', 'Numerico'),
((SELECT SetorID FROM esg.DimSetor WHERE SetorNome = 'Saude'), 'Pacientes Beneficiados', 'pacientes', 'Numerico');
GO

-- KPIs GERAIS (aplicaveis a todos setores)
INSERT INTO esg.DimTipoKPI (SetorID, KPINome, UnidadeMedida, TipoValor) VALUES
(NULL, 'Valor Total da Carteira ESG', 'R$', 'Monetario'),
(NULL, 'Crescimento YoY da Carteira', '%', 'Percentual'),
(NULL, 'Numero de Empresas na Carteira', 'empresas', 'Numerico');
GO

-- ============================================================================
-- CARGA: SUBSETORES
-- ============================================================================

-- SubSetores de ENERGIA
INSERT INTO esg.DimSubSetor (SetorID, SubSetorNome) VALUES
((SELECT SetorID FROM esg.DimSetor WHERE SetorNome = 'Energia'), 'Energia Renovavel'),
((SELECT SetorID FROM esg.DimSetor WHERE SetorNome = 'Energia'), 'Eficiencia Energetica'),
((SELECT SetorID FROM esg.DimSetor WHERE SetorNome = 'Energia'), 'Transmissao de Energia'),
((SELECT SetorID FROM esg.DimSetor WHERE SetorNome = 'Energia'), 'Distribuicao de Energia'),
((SELECT SetorID FROM esg.DimSetor WHERE SetorNome = 'Energia'), 'Geracao Termica');
GO

-- SubSetores de SANEAMENTO
INSERT INTO esg.DimSubSetor (SetorID, SubSetorNome) VALUES
((SELECT SetorID FROM esg.DimSetor WHERE SetorNome = 'Saneamento'), 'Tratamento de Agua'),
((SELECT SetorID FROM esg.DimSetor WHERE SetorNome = 'Saneamento'), 'Tratamento de Esgoto'),
((SELECT SetorID FROM esg.DimSetor WHERE SetorNome = 'Saneamento'), 'Distribuicao de Agua'),
((SELECT SetorID FROM esg.DimSetor WHERE SetorNome = 'Saneamento'), 'Coleta de Esgoto');
GO

-- SubSetores de SAUDE
INSERT INTO esg.DimSubSetor (SetorID, SubSetorNome) VALUES
((SELECT SetorID FROM esg.DimSetor WHERE SetorNome = 'Saude'), 'Hospitais'),
((SELECT SetorID FROM esg.DimSetor WHERE SetorNome = 'Saude'), 'Clinicas'),
((SELECT SetorID FROM esg.DimSetor WHERE SetorNome = 'Saude'), 'Laboratorios'),
((SELECT SetorID FROM esg.DimSetor WHERE SetorNome = 'Saude'), 'Planos de Saude'),
((SELECT SetorID FROM esg.DimSetor WHERE SetorNome = 'Saude'), 'Oncologia');
GO

PRINT 'Carga inicial de dados concluida com sucesso!'
GO
