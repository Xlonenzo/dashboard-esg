# Modelagem de Dados ESG - Banco Votorantim

## Visao Geral

Esta modelagem segue o padrao **Star Schema** (Esquema Estrela), otimizado para analises em Power BI e ferramentas de BI.

## Diagrama de Entidade-Relacionamento

```
                                    +------------------+
                                    |    DimTema       |
                                    +------------------+
                                           |
                                           |
+------------------+              +------------------+              +------------------+
|   DimCategoria   |--------------|    DimEmpresa    |--------------|    DimSetor      |
+------------------+              +------------------+              +------------------+
                                    |         |                            |
                                    |         |                            |
                           +--------+         +--------+                   |
                           |                           |                   |
                           v                           v                   v
                  +------------------+       +------------------+  +------------------+
                  |   FatoCarteira   |       |     FatoKPI      |  |   DimSubSetor    |
                  +------------------+       +------------------+  +------------------+
                           |                           |
                           |                           |
                           v                           v
                  +------------------+       +------------------+
                  |    DimProduto    |       |    DimTipoKPI    |
                  +------------------+       +------------------+
                                                       |
                                                       |
                                                       v
                  +------------------+       +------------------+
                  |     DimODS       |<------|   BridgeKPIODS   |
                  +------------------+       +------------------+
                           |
                           v
                  +------------------+
                  |   DimMetaODS     |
                  +------------------+
```

## Tabelas de Dimensao

### DimEmpresa
Cadastro central de empresas da carteira ESG.

| Coluna | Tipo | Descricao |
|--------|------|-----------|
| EmpresaID | INT | Chave primaria |
| EmpresaNome | NVARCHAR(300) | Nome da empresa |
| CNPJ | VARCHAR(18) | CNPJ formatado |
| SetorID | INT | FK para DimSetor |
| CategoriaID | INT | FK para DimCategoria |

### DimSetor
Setores de atuacao.

| Setor | Descricao |
|-------|-----------|
| Energia | Geracao, transmissao e distribuicao |
| Saneamento | Agua e saneamento basico |
| Saude | Servicos de saude e bem-estar |
| Educacao | Educacao e formacao |
| Inclusao Digital | Tecnologia e inclusao |

### DimCategoria
Categorias ESG.

| Categoria | Cor | Descricao |
|-----------|-----|-----------|
| Green | #28A745 | Foco ambiental |
| Social | #007BFF | Foco social |
| Sustainable | #6F42C1 | Misto |

### DimODS
17 Objetivos de Desenvolvimento Sustentavel da ONU.

### DimTipoKPI
Tipos de indicadores por setor (KPIs de energia, saneamento, saude, etc.)

### DimTempo
Dimensao temporal para analises (2020-2035).

### DimCNAE
Classificacao Nacional de Atividades Economicas com mapeamento BV/IBGE.

## Tabelas de Fato

### FatoCarteira
Valores de carteira ESG por empresa/periodo.

| Coluna | Descricao |
|--------|-----------|
| ValorCarteira | Valor em R$ |
| AnoReferencia | Ano de referencia |
| EmpresaID, SetorID, etc. | Chaves para dimensoes |

### FatoKPI
Valores de indicadores por empresa.

| Coluna | Descricao |
|--------|-----------|
| ValorNumerico | Valor do KPI |
| VariacaoAnterior | Variacao % vs ano anterior |
| FonteDados | Fonte da informacao |

### FatoIndicadorSaneamento
KPIs especificos: volume agua/esgoto, populacao atendida.

### FatoIndicadorSaude
KPIs especificos: leitos, pacientes, vagas.

### FatoIndicadorEnergia
KPIs especificos: capacidade MW, emissoes evitadas, energia renovavel.

### FatoMeta2030
Metas e progresso para 2030.

## Tabelas Bridge

### BridgeKPIODS
Relacionamento N:N entre KPIs e ODS (primarias e secundarias).

### BridgeEmpresaODS
Relacionamento N:N entre Empresas e ODS.

### BridgeEmpresaCNAE
Relacionamento N:N entre Empresas e CNAEs.

## Views para Power BI

| View | Descricao |
|------|-----------|
| vw_CarteiraConsolidada | Carteira completa com todas dimensoes |
| vw_KPIsEmpresa | KPIs por empresa |
| vw_ResumoSetor | Totais por setor |
| vw_ResumoCategoria | Totais por categoria ESG |
| vw_IndicadoresSaneamento | KPIs de saneamento |
| vw_IndicadoresSaude | KPIs de saude |
| vw_IndicadoresEnergia | KPIs de energia |
| vw_EmpresaODS | Empresas x ODS |
| vw_Metas2030 | Status das metas 2030 |
| vw_ValidacaoEmpresas | Conformidade (4 regras) |

## Mapeamento Excel -> SQL

| Arquivo Excel | Tabela(s) SQL |
|---------------|---------------|
| carteira.xlsx | DimEmpresa, FatoCarteira |
| ods.xlsx | BridgeKPIODS |
| metaods.xlsx | DimMetaODS |
| DE_PARA CARTEIRA TAXONOMIA.xlsx | DimCNAE |
| Energia*.xlsx | FatoCarteira, FatoKPI, FatoIndicadorEnergia |
| KPIs_*.xlsx | FatoKPI |
| Indicadores_ESG_Saneamento.xlsx | FatoIndicadorSaneamento |
| empresasaude.xlsx | FatoIndicadorSaude |
| Carteira_Saneamento_4Regras.xlsx | ValidacaoEmpresa |

## Instrucoes de Deploy

1. Criar database no Azure SQL Server
2. Executar scripts na ordem (01 a 06)
3. Importar dados dos Excel (ETL)
4. Conectar Power BI usando as Views

## Conexao Power BI

```
Servidor: seu-servidor.database.windows.net
Banco: ESG_BV
Autenticacao: Azure Active Directory ou SQL Auth
```

No Power BI, use **Obter Dados > SQL Server** e importe as views do schema `esg`.
