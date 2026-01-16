-- ============================================================================
-- MODELAGEM DE DADOS ESG - BANCO VOTORANTIM
-- PostgreSQL Database
-- Script 00: Criar Database e Schemas
-- ============================================================================
-- INSTRUCOES:
-- 1. Conecte ao PostgreSQL como superuser (postgres)
-- 2. Execute este script no psql ou pgAdmin
-- ============================================================================

-- Criar database (execute conectado ao postgres)
-- CREATE DATABASE esg_bv;

-- Conecte ao database esg_bv antes de executar o resto
-- \c esg_bv

-- ============================================================================
-- CRIAR SCHEMAS
-- ============================================================================
CREATE SCHEMA IF NOT EXISTS esg;
CREATE SCHEMA IF NOT EXISTS fundos;
CREATE SCHEMA IF NOT EXISTS titulos;
CREATE SCHEMA IF NOT EXISTS tsb;
CREATE SCHEMA IF NOT EXISTS emissores;

-- ============================================================================
-- DIMENSAO: SETOR
-- ============================================================================
DROP TABLE IF EXISTS esg.dimsetor CASCADE;
CREATE TABLE esg.dimsetor (
    setorid SERIAL PRIMARY KEY,
    setornome VARCHAR(100) NOT NULL,
    setordescricao VARCHAR(500),
    ativo BOOLEAN DEFAULT true,
    datacriacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    dataatualizacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================================
-- DIMENSAO: SUBSETOR
-- ============================================================================
DROP TABLE IF EXISTS esg.dimsubsetor CASCADE;
CREATE TABLE esg.dimsubsetor (
    subsetorid SERIAL PRIMARY KEY,
    setorid INT NOT NULL,
    subsetornome VARCHAR(200) NOT NULL,
    subsetordescricao VARCHAR(500),
    ativo BOOLEAN DEFAULT true,
    datacriacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    dataatualizacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_subsetor_setor FOREIGN KEY (setorid) REFERENCES esg.dimsetor(setorid)
);

-- ============================================================================
-- DIMENSAO: CATEGORIA
-- ============================================================================
DROP TABLE IF EXISTS esg.dimcategoria CASCADE;
CREATE TABLE esg.dimcategoria (
    categoriaid SERIAL PRIMARY KEY,
    categorianome VARCHAR(100) NOT NULL,
    categoriadescricao VARCHAR(500),
    categoriacor VARCHAR(7),
    ativo BOOLEAN DEFAULT true,
    datacriacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    dataatualizacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================================
-- DIMENSAO: TEMA
-- ============================================================================
DROP TABLE IF EXISTS esg.dimtema CASCADE;
CREATE TABLE esg.dimtema (
    temaid SERIAL PRIMARY KEY,
    temanome VARCHAR(100) NOT NULL,
    temadescricao VARCHAR(500),
    ativo BOOLEAN DEFAULT true,
    datacriacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    dataatualizacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================================
-- DIMENSAO: EMPRESA
-- ============================================================================
DROP TABLE IF EXISTS esg.dimempresa CASCADE;
CREATE TABLE esg.dimempresa (
    empresaid SERIAL PRIMARY KEY,
    empresanome VARCHAR(300) NOT NULL,
    cnpj VARCHAR(18),
    cnpjnumerico BIGINT,
    setorid INT,
    subsetorid INT,
    categoriaid INT,
    temaid INT,
    cnaeprincipal INT,
    razaosocial VARCHAR(300),
    ativo BOOLEAN DEFAULT true,
    datacriacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    dataatualizacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_empresa_setor FOREIGN KEY (setorid) REFERENCES esg.dimsetor(setorid),
    CONSTRAINT fk_empresa_subsetor FOREIGN KEY (subsetorid) REFERENCES esg.dimsubsetor(subsetorid),
    CONSTRAINT fk_empresa_categoria FOREIGN KEY (categoriaid) REFERENCES esg.dimcategoria(categoriaid),
    CONSTRAINT fk_empresa_tema FOREIGN KEY (temaid) REFERENCES esg.dimtema(temaid)
);

CREATE INDEX ix_dimempresa_cnpj ON esg.dimempresa(cnpjnumerico);
CREATE INDEX ix_dimempresa_setor ON esg.dimempresa(setorid);

-- ============================================================================
-- DIMENSAO: ODS
-- ============================================================================
DROP TABLE IF EXISTS esg.dimods CASCADE;
CREATE TABLE esg.dimods (
    odsid INT PRIMARY KEY,
    odsnome VARCHAR(200) NOT NULL,
    odsdescricao VARCHAR(1000),
    odscor VARCHAR(7),
    urlimagem VARCHAR(500),
    ativo BOOLEAN DEFAULT true,
    datacriacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    dataatualizacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================================
-- DIMENSAO: META ODS
-- ============================================================================
DROP TABLE IF EXISTS esg.dimmetaods CASCADE;
CREATE TABLE esg.dimmetaods (
    metaodsid SERIAL PRIMARY KEY,
    odsid INT NOT NULL,
    metacodigo VARCHAR(10) NOT NULL,
    metadescricao VARCHAR(1000),
    metadescricaoresumida VARCHAR(300),
    indicadoronusugerido VARCHAR(500),
    ativo BOOLEAN DEFAULT true,
    datacriacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    dataatualizacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_metaods_ods FOREIGN KEY (odsid) REFERENCES esg.dimods(odsid)
);

-- ============================================================================
-- DIMENSAO: TEMPO
-- ============================================================================
DROP TABLE IF EXISTS esg.dimtempo CASCADE;
CREATE TABLE esg.dimtempo (
    dataid INT PRIMARY KEY,
    data DATE NOT NULL,
    ano INT NOT NULL,
    trimestre INT NOT NULL,
    mes INT NOT NULL,
    mesnome VARCHAR(20),
    semestre INT,
    anomes VARCHAR(7),
    anotrimestre VARCHAR(7),
    ativo BOOLEAN DEFAULT true
);

CREATE INDEX ix_dimtempo_ano ON esg.dimtempo(ano);

-- ============================================================================
-- TABELA: LOG IMPORTACAO
-- ============================================================================
DROP TABLE IF EXISTS esg.logimportacao CASCADE;
CREATE TABLE esg.logimportacao (
    logid SERIAL PRIMARY KEY,
    tabeladestino VARCHAR(100),
    arquivoorigem VARCHAR(500),
    registrosimportados INT,
    registroscomerro INT,
    status VARCHAR(50),
    mensagemerro TEXT,
    datacriacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================================
-- SCHEMA FUNDOS - Tabelas de Fundos de Investimento
-- ============================================================================
DROP TABLE IF EXISTS fundos.todosfundos CASCADE;
CREATE TABLE fundos.todosfundos (
    fundoid SERIAL PRIMARY KEY,
    codigofundo VARCHAR(50),
    cnpj VARCHAR(18),
    razaosocial VARCHAR(500),
    nomecomercial VARCHAR(500),
    tipofundo VARCHAR(100),
    categoria VARCHAR(200),
    categoriaesg VARCHAR(100),
    focoesg VARCHAR(200),
    ativo BOOLEAN DEFAULT true,
    datacriacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

DROP TABLE IF EXISTS fundos.gestorassimilares CASCADE;
CREATE TABLE fundos.gestorassimilares (
    id SERIAL PRIMARY KEY,
    cnpj VARCHAR(18),
    nomecompleto VARCHAR(500),
    tipofundo VARCHAR(100),
    classeanbima VARCHAR(200),
    gestora VARCHAR(200),
    publicoalvo VARCHAR(200),
    datacriacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================================
-- SCHEMA TSB - Taxonomia Sustentavel Brasileira
-- ============================================================================
DROP TABLE IF EXISTS tsb.empresastsb CASCADE;
CREATE TABLE tsb.empresastsb (
    empresaid SERIAL PRIMARY KEY,
    emissor VARCHAR(300),
    cnpj VARCHAR(18),
    setortsb VARCHAR(100),
    classificacao VARCHAR(50),
    score DECIMAL(5,2),
    titulos TEXT,
    datacriacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

DROP TABLE IF EXISTS tsb.kpistsb CASCADE;
CREATE TABLE tsb.kpistsb (
    kpiid SERIAL PRIMARY KEY,
    setor VARCHAR(200),
    codigokpi VARCHAR(50),
    nomekpi VARCHAR(500),
    unidade VARCHAR(100),
    frequencia VARCHAR(50),
    obrigatorio BOOLEAN DEFAULT false,
    datacriacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

DROP TABLE IF EXISTS tsb.kpisempresa CASCADE;
CREATE TABLE tsb.kpisempresa (
    id SERIAL PRIMARY KEY,
    empresaid INT REFERENCES tsb.empresastsb(empresaid),
    codigokpi VARCHAR(50),
    valor VARCHAR(200),
    status VARCHAR(50),
    datacriacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================================
-- SCHEMA TITULOS - Titulos de Renda Fixa
-- ============================================================================
DROP TABLE IF EXISTS titulos.debentures CASCADE;
CREATE TABLE titulos.debentures (
    id SERIAL PRIMARY KEY,
    codigoativo VARCHAR(50),
    emissor VARCHAR(300),
    grupo VARCHAR(100),
    percentualtaxa DECIMAL(10,4),
    taxaindicativa DECIMAL(10,4),
    pu DECIMAL(15,6),
    duration INT,
    datacriacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

DROP TABLE IF EXISTS titulos.cricra CASCADE;
CREATE TABLE titulos.cricra (
    id SERIAL PRIMARY KEY,
    codigoativo VARCHAR(50),
    tipocontrato VARCHAR(10),
    emissor VARCHAR(300),
    originador VARCHAR(300),
    serie VARCHAR(50),
    emissao VARCHAR(50),
    datavencimento DATE,
    taxaindicativa DECIMAL(10,4),
    pu DECIMAL(15,6),
    duration INT,
    tiporemuneracao VARCHAR(100),
    taxacorrecao VARCHAR(100),
    datacriacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

DROP TABLE IF EXISTS titulos.titulospublicos CASCADE;
CREATE TABLE titulos.titulospublicos (
    id SERIAL PRIMARY KEY,
    tipo VARCHAR(100),
    vencimento DATE,
    taxaindicativa DECIMAL(10,4),
    pu DECIMAL(15,6),
    datacriacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================================
-- SCHEMA EMISSORES - Empresas de Capital Aberto
-- ============================================================================
DROP TABLE IF EXISTS emissores.empresas CASCADE;
CREATE TABLE emissores.empresas (
    empresaid SERIAL PRIMARY KEY,
    cnpj VARCHAR(18),
    razaosocial VARCHAR(500),
    codigocvm VARCHAR(50),
    setor VARCHAR(200),
    datacriacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

DROP TABLE IF EXISTS emissores.demonstracoesfinanceiras CASCADE;
CREATE TABLE emissores.demonstracoesfinanceiras (
    id SERIAL PRIMARY KEY,
    cnpj VARCHAR(18),
    tipodemonstracao VARCHAR(50),
    codigoconta VARCHAR(50),
    descricaoconta VARCHAR(500),
    valor DECIMAL(20,2),
    anoexercicio INT,
    datacriacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

DROP TABLE IF EXISTS emissores.governanca CASCADE;
CREATE TABLE emissores.governanca (
    id SERIAL PRIMARY KEY,
    cnpj VARCHAR(18),
    capitulo VARCHAR(200),
    principio VARCHAR(500),
    praticaadotada VARCHAR(50),
    anoreferencia INT,
    datacriacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================================
-- Mensagem de conclusao
-- ============================================================================
DO $$
BEGIN
    RAISE NOTICE 'Database PostgreSQL criado com sucesso!';
    RAISE NOTICE 'Schemas: esg, fundos, titulos, tsb, emissores';
END $$;
