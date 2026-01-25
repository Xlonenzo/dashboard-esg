# Briefing para Apresentação - Dashboard ESG Banco Votorantim

## Instruções para o Claude
Crie uma apresentação profissional para investidores com tema LARANJA e PRETO. Use um design moderno, elegante e corporativo. A apresentação deve ter slides visuais com ícones, gráficos conceituais e layout limpo.

---

## SLIDE 1 - CAPA
**Título:** Dashboard ESG - Plataforma de Investimentos Sustentáveis
**Subtítulo:** Transformando dados em decisões sustentáveis
**Empresa:** Banco Votorantim
**Tagline:** "Inteligência de dados para finanças verdes"

---

## SLIDE 2 - O PROBLEMA
### O Desafio do Mercado de Investimentos Sustentáveis

- **Fragmentação de Dados:** Informações de fundos ESG espalhadas em múltiplas fontes
- **Falta de Padronização:** Dificuldade em comparar fundos e títulos sustentáveis
- **Complexidade Regulatória:** Taxonomia Sustentável Brasileira (TSB) exige conformidade
- **Análise Manual:** Gestores gastam horas consolidando informações
- **Risco de Greenwashing:** Sem ferramentas adequadas para validar credenciais ESG

**Impacto:** Decisões de investimento mais lentas e menos precisas

---

## SLIDE 3 - A SOLUÇÃO
### Dashboard ESG: Plataforma Integrada de Análise

Uma plataforma completa que consolida, analisa e apresenta dados de investimentos sustentáveis em tempo real.

**Proposta de Valor:**
- Centralização de +100.000 fundos ANBIMA
- Classificação automática ESG (IS, ESG Integrado, Convencional)
- Conformidade com Taxonomia Sustentável Brasileira
- Análise de risco integrada (ESG, Crédito, Liquidez, Clima)
- Inteligência Artificial para consultas em linguagem natural

---

## SLIDE 4 - FUNCIONALIDADES PRINCIPAIS

### 1. Catálogo de Fundos
- +100.000 fundos da ANBIMA
- Filtros por categoria, gestora, tipo ESG
- Selo IS (Investimento Sustentável)
- Comparativo de performance

### 2. Títulos Sustentáveis
- 250+ debêntures com taxas e indexadores
- Títulos públicos (LTN, NTN-B, NTN-F, LFT)
- CRI/CRA (Certificados de Recebíveis)

### 3. Taxonomia Brasileira (TSB)
- 58 empresas classificadas (Verde/Transição)
- Mapeamento de setores sustentáveis
- KPIs por empresa

### 4. Análise de Risco
- Score integrado (ESG + Crédito + Concentração)
- Sistema de Early Warning
- Análise de dívida detalhada
- Calendário de vencimentos

### 5. IA Integrada
- Consultas em linguagem natural
- Powered by Groq AI
- Respostas instantâneas sobre portfólio

---

## SLIDE 5 - MÉTRICAS E COBERTURA

| Métrica | Volume |
|---------|--------|
| Fundos Catalogados | +100.000 |
| Fundos Pesquisáveis | 4.253+ |
| Debêntures | 250+ |
| Empresas TSB | 58 |
| Gestoras | 500+ |
| Frameworks ESG | 7 |

**Frameworks Suportados:**
- Green Bond Principles
- Social Bond Principles
- Sustainability-Linked Bonds (SLB)
- Sustainability-Linked Loans (SLL)
- Taxonomia Sustentável Brasileira
- FEBRABAN Verde
- Blue Economy

---

## SLIDE 6 - ARQUITETURA TECNOLÓGICA

### Stack Moderno e Escalável

**Backend:**
- Python/Flask (API RESTful)
- PostgreSQL (Banco de dados)
- Gunicorn (Servidor WSGI)

**Frontend:**
- HTML5/JavaScript
- Plotly.js (Visualizações)
- Design Responsivo

**Infraestrutura:**
- Deploy na Render.com
- Escalabilidade automática
- 99.9% uptime

**Integrações:**
- API ANBIMA
- Dados CVM
- Groq AI (LLM)

---

## SLIDE 7 - MODELO DE DADOS

### Arquitetura Star Schema

**Dimensões:**
- DimEmpresa (Empresas)
- DimSetor (Setores da economia)
- DimCategoria (Categorias de fundos)
- DimODS (Objetivos de Desenvolvimento Sustentável)
- DimTipoKPI (Tipos de indicadores)

**Fatos:**
- FatoCarteira (Composição de carteiras)
- FatoKPI (Indicadores de performance)
- FatoIndicadorSaneamento/Saude/Energia
- FatoMeta2030 (Metas ODS)

**Benefício:** Análises multidimensionais rápidas e flexíveis

---

## SLIDE 8 - DIFERENCIAIS COMPETITIVOS

| Diferencial | Descrição |
|-------------|-----------|
| **Dados em Tempo Real** | Integração direta com ANBIMA e CVM |
| **TSB Nativo** | Única plataforma com Taxonomia BR integrada |
| **IA Conversacional** | Pergunte em português, receba insights |
| **Multi-Asset** | Fundos, debêntures, títulos, CRI/CRA |
| **Risk Scoring** | Análise de risco proprietária |
| **Open Architecture** | API para integrações customizadas |

---

## SLIDE 9 - CASOS DE USO

### Para Gestores de Ativos
- Screening de fundos por critérios ESG
- Comparativo de performance
- Due diligence automatizada

### Para Analistas de Crédito
- Análise de emissores
- Early warning de riscos
- Calendário de vencimentos

### Para Compliance
- Validação de conformidade TSB
- Relatórios de sustentabilidade
- Audit trail completo

### Para Investidores Institucionais
- Portfólio screening
- Benchmark ESG
- Relatórios customizados

---

## SLIDE 10 - ROADMAP

### Fase Atual (Concluída)
- Catálogo completo de fundos ANBIMA
- Integração TSB
- Sistema de análise de risco
- IA para consultas

### Próximos Passos
- Integração com Bloomberg/Refinitiv
- Score ESG proprietário
- Mobile app
- API pública para fintechs
- Expansão para LATAM

---

## SLIDE 11 - MERCADO POTENCIAL

### Oportunidade de Mercado

**Brasil:**
- R$ 7,5 trilhões em ativos sob gestão
- 30% crescimento em fundos ESG (2023-2024)
- Regulação CVM incentivando ESG

**Global:**
- US$ 35 trilhões em investimentos ESG
- Crescimento de 15% ao ano
- Pressão regulatória crescente

**Target:**
- Bancos e asset managers
- Family offices
- Fundos de pensão
- Investidores institucionais

---

## SLIDE 12 - TIME E PARCEIROS

### Expertise Combinada

**Banco Votorantim:**
- Tradição em crédito e investimentos
- Base sólida de clientes institucionais
- Infraestrutura tecnológica robusta

**Parceiros de Dados:**
- ANBIMA (Associação Brasileira das Entidades dos Mercados Financeiro e de Capitais)
- CVM (Comissão de Valores Mobiliários)
- B3 (Bolsa de Valores)

---

## SLIDE 13 - CONTATO E PRÓXIMOS PASSOS

### Vamos Conversar

**Próximos Passos:**
1. Demo personalizada da plataforma
2. POC com dados do seu portfólio
3. Proposta comercial customizada

**Contato:**
- Website: [URL do Dashboard]
- Email: esg@bancovotorantim.com.br

---

## NOTAS DE DESIGN

### Paleta de Cores
- **Laranja Principal:** #FF6B00 ou #F57C00
- **Laranja Claro:** #FFB74D
- **Preto:** #1A1A1A ou #212121
- **Cinza Escuro:** #424242
- **Branco:** #FFFFFF (para contraste)

### Tipografia
- Títulos: Bold, sem serifa (Montserrat, Poppins)
- Corpo: Regular, sem serifa (Open Sans, Roboto)

### Elementos Visuais
- Ícones flat/outline
- Gráficos com gradiente laranja
- Cards com sombra suave
- Linhas de separação em laranja

### Estilo Geral
- Minimalista e profissional
- Alto contraste para legibilidade
- Espaço em branco generoso
- Foco em dados e métricas
