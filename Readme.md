# Global Superstore: Diagnóstico de Rentabilidade e Eficiência Logística

Projeto de BI end-to-end que audita e diagnostica a rentabilidade e a eficiência logística de uma operação de varejo global, da extração em Python até a visualização final em Power BI.

![Demonstração do Dashboard](assets/Gif%20-%20Superstore.gif)(https://app.powerbi.com/view?r=eyJrIjoiMjRiY2I5ZGQtOTA4MS00NzA5LWEyZmMtODI1NDBiZDM2ODc5IiwidCI6IjkwNzg5MzgzLTExYjMtNGQ0My05YjI4LWNlNDM1M2IyZDg1NSJ9)

---

## 🔗 Acesso ao Projeto

| Recurso | Link |
|---|---|
| 📊 Dashboard | https://app.powerbi.com/view?r=eyJrIjoiMjRiY2I5ZGQtOTA4MS00NzA5LWEyZmMtODI1NDBiZDM2ODc5IiwidCI6IjkwNzg5MzgzLTExYjMtNGQ0My05YjI4LWNlNDM1M2IyZDg1NSJ9 |
| 🗂️ Dataset original | [Global Superstore — Kaggle](https://www.kaggle.com/datasets/anandaramg/global-superstore) |

> **Nota sobre a fonte dos dados:** apesar da descrição do dataset no Kaggle mencionar a Walmart, o schema de colunas corresponde ao clássico "Sample/Global Superstore", um dataset fictício originado da Tableau e amplamente reutilizado em portfólios de BI. Tratado aqui como estudo de caso de varejo global, sem vínculo real com nenhuma empresa específica.

---

## 📋 Sobre o Projeto

O objetivo foi construir uma solução de BI capaz de responder três perguntas de negócio distintas sobre uma operação de varejo com presença em 147 países:

1. **A operação está saudável financeiramente, e a tendência é de melhora ou piora?**
2. **Onde a margem está sendo destruída, e por quê?**
3. **Onde a logística é ineficiente, em termos de custo e prazo?**

Cada página do dashboard foi desenhada para responder a uma dessas perguntas especificamente, evitando sobreposição de informação entre páginas.

### Funcionalidades / Escopo

- Pipeline de ETL em Python com tradução completa de conteúdo (147 países) e conversão cambial via API do Banco Central
- Modelagem dimensional (fato + dimensões) em PostgreSQL na nuvem (Supabase)
- Dashboard de 4 páginas com filtro de materialidade estatística aplicado a rankings e segmentações geográficas
- Visual customizado em Deneb com alternância dinâmica entre 3 métricas de ranking

---

## 🏗️ Arquitetura

```
Global Superstore.txt (51k+ linhas, bruto)
        │
        ▼
   etl_dataset.py  ──►  traducoes.py (colunas + 147 países)
        │
        ▼
   Tratamento: nulos, métricas derivadas, conversão cambial (API Banco Central)
        │
        ▼
   Carga: fVendas (fato) + views_sql.py (dimensões) no Supabase (PostgreSQL)
        │
        ▼
   Exportação: vendas_Processadas.csv (camada de consumo do Power BI)
```

### ⚠️ Nota de arquitetura

O modelo dimensional foi projetado para viver como Views SQL no banco em nuvem (`src/views_sql.py`) — essa é a modelagem de origem do projeto. Na prática, o conector nativo do Power BI apresentou duas barreiras de conectividade com o Supabase: incompatibilidade IPv4 (Power BI) vs IPv6 (conexão direta), e falha de validação de certificado SSL mesmo via Connection Pooler. Como o dataset é estático, a solução adotada foi publicar o CSV processado no GitHub e replicar a mesma lógica dimensional em Power Query — incluindo a correção de fan-out da dimensão de localidade (ver seção de bugs). A modelagem em SQL permanece documentada e funcional no pipeline.

### Estrutura de Dados

| Entidade | Papel | Observação |
|---|---|---|
| `fVendas` | Fato | Transações, métricas e chaves |
| `dProduto` | Dimensão | Chave composta (ID_Produto + Nome_Produto) |
| `dLocalidade` | Dimensão | País/Estado/Cidade/Região |
| `dCliente` | Dimensão | Segmento e identificação de clientes |
| `fVendas_Final` | View | Fato + dimensões unidas |

---

## 📊 Estrutura do Dashboard

### Visão Executiva
KPIs headline, os 4 principais achados de negócio em texto, e navegação para as páginas de detalhe.

### Performance Global — Saúde Financeira e Tendência
Evolução mensal de Receita vs Despesas vs Lucro Líquido, ranking dinâmico de países (Deneb), decomposição do lucro via waterfall.

### Diagnóstico de Margem — Onde a Rentabilidade é Perdida
Correlação entre desconto e performance de vendas, tabela de categorias com margem bruta/líquida e impacto do frete.

### Eficiência Logística — Custo e Prazo por Região
Mapa mundial (Comercial/Logística), quadrante de eficiência com zonas calibradas por mediana real, tabela de prazo/frete por região.

---

## 🔍 Achados de Negócio

**1. Existe um "penhasco" de desconto.** Pedidos com desconto de até 10% geram margem líquida de +12,9% — a partir de ~15% de desconto a margem já vira prejuízo, chegando a -121,9% acima de 50%.

**2. Só a Classe Econômica dá lucro de verdade.** Modos de envio rápidos operam no prejuízo (-5,5% a -6,0%) — por isso, pedidos de prioridade Alta e Crítica também fecham no vermelho (-2,3% e -11,2%).

**3. Móveis é a única categoria estruturalmente deficitária** (-3,8% de margem líquida), puxada pela subcategoria Mesas.

**4. Existem mercados com volume real, mas estruturalmente deficitários.** Turquia (632 pedidos) e Holanda (204 pedidos) operam com margem líquida de -101% e -65%.

---

## 🐛 Bugs Encontrados e Corrigidos

| Bug | Causa | Impacto | Correção |
|---|---|---|---|
| Lucro Bruto inflado (~2x) | Medida DAX de Custo de Vendas subtraindo o Custo de Envio indevidamente | Margem bruta reportada ~22% quando a real era ~12% | Fórmula corrigida para `Vendas - Lucro` |
| Fan-out na dimensão de Localidade | 7 combinações Pais/Estado/Cidade com Região inconsistente no dado bruto | Vendas infladas em ~$91k, Quantidade em 1.152 unidades | View reconstruída com a Região mais frequente por cidade |
| Tradução de países incompleta | Dicionário cobria 9 de 147 países | Países como "Spain" sem traduzir | Dicionário completo via `babel`/`pycountry` |
| Corrupção de locale no Power Query | Separador decimal interpretado como separador de milhar | Valores como 0,47 viravam 47 | Recriação do tipo via "Alterar Tipo → Usando Local" |
| Ranking dominado por ruído estatístico | Rankings percentuais sem piso de materialidade | Países com 1-2 pedidos dominavam o topo | Filtro de materialidade (Receita > R$150.000) |
| Comparação circular no Quadrante | Linha de referência usando `VALUES()`, respeitando o contexto do próprio ponto avaliado | Todo país comparado consigo mesmo | Troca para `ALLSELECTED()` |

---

## 🛠️ Tecnologias Utilizadas

| Tecnologia | Finalidade |
|---|---|
| Python (Pandas, NumPy) | ETL — extração, tratamento, métricas |
| SQLAlchemy + psycopg2 | Conectividade Python → Supabase |
| PostgreSQL (Supabase) | Data Warehouse, modelagem dimensional |
| API do Banco Central do Brasil | Cotação oficial USD/BRL |
| GitHub | Hospedagem do CSV (camada de consumo) |
| Power BI + Power Query | Modelagem final, DAX, visualização |
| Deneb (Vega-Lite) | Visual customizado de ranking dinâmico |
| babel / pycountry | Tradução completa de países |

---

## 🏗️ Estrutura do Repositório

```
bi-global-superstore/
├── assets/
├── bi/
│   └── global_superstore.pbix 
├── data/
│   ├── Global Superstore.txt
│   └── vendas_Processadas.csv
├── src/
│   ├── download_dataset.py
│   ├── etl_dataset.py
│   ├── traducoes.py
│   ├── views_sql.py
│   └── cotacao_api.py
├── .env
├── .gitignore
├── requirements.txt
└── README.md
```

---

## Como reproduzir o pipeline de dados
 
> Os passos abaixo reproduzem a camada de dados (ETL → banco → CSV). O dashboard em si (medidas DAX, visuais e páginas) não é distribuído como arquivo `.pbix` neste repositório — para explorá-lo, acesse o link publicado na seção "Acesso ao Projeto".
 
### Pré-requisitos
- Python 3.11+
- Conta no Supabase (PostgreSQL)
### 1. Clone o repositório
```bash
git clone https://github.com/Beattriz-Oliveira/bi-global-superstore.git
```
 
### 2. Instale as dependências
```bash
pip install -r requirements.txt --break-system-packages
```
 
### 3. Configure o `.env`
Copie `.env.example` para `.env` e preencha as credenciais do Supabase.
 
### 4. Rode o pipeline
```bash
python src/etl_dataset.py
```
Isso processa o dataset bruto, carrega o modelo dimensional no Supabase e gera `data/vendas_Processadas.csv` — o arquivo que alimenta o dashboard.
 
### 5. Visualize ou reproduza o dashboard
 
O arquivo `.pbix` não é versionado neste repositório (arquivo binário, sem diff útil no Git). Duas formas de acessar o resultado:
 
- **Só quer ver o resultado?** Acesse o dashboard publicado pelo link na seção "Acesso ao Projeto" acima.
- **Quer reproduzir/explorar o `.pbix` você mesma?** Abra o Power BI Desktop → Obter Dados → Web → cole a URL raw do `vendas_Processadas.csv` deste repositório no GitHub, e monte o modelo a partir daí (a lógica de tradução e tratamento já vem pronta no CSV; a modelagem dimensional das dimensões é replicada em Power Query, como descrito na Nota de Arquitetura acima).
---

## Erros comuns e soluções

<details>
<summary><strong>"No module named 'psycopg2'"</strong></summary>

Driver do PostgreSQL não instalado. Rode:
```bash
pip install psycopg2-binary --break-system-packages
```
</details>

<details>
<summary><strong>Power BI: "O certificado remoto é inválido"</strong></summary>

Problema conhecido do conector nativo do Power BI com o Supabase (cadeia de certificado SSL). A solução adotada foi consumir os dados via CSV publicado no GitHub, em vez de conexão direta — ver Nota de Arquitetura.
</details>

<details>
<summary><strong>Valores decimais aparecem multiplicados por 100 no Power Query</strong></summary>

Conflito de locale (separador decimal). Delete o passo automático "Changed Type" e reaplique o tipo via "Alterar Tipo → Usando Local → Inglês (Estados Unidos)".
</details>

---

## 👩‍💻 Autoria

* **Analista:** Beattriz Oliveira Satn'ana
* **Segmento:** Análista de Business Intelligence
* **Focos:** Data Architecture · Business Intelligence
* **Ano:** 2026