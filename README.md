# Pipeline de Processamento ANVISA e NFe

Este projeto contém um conjunto de pipelines automatizados para baixar dados da ANVISA, processar a base de medicamentos (CMED) e cruzar com notas fiscais eletrônicas (NFe) para enriquecimento e análise.

## Estrutura do Projeto

O projeto é dividido em 3 pipelines principais:

1.  **Download ANVISA**: Baixa o histórico de preços de medicamentos.
2.  **Processamento ANVISA**: Limpa, padroniza e gera a `baseANVISA` consolidada.
3.  **Pipeline NFe**: Processa notas fiscais, cruza com a base ANVISA e gera dados para QlikView.

### Estrutura de Pastas

```
Pipeline_Anvisa/
├── main.py                        # Executa o Pipeline 2 (Processamento ANVISA)
├── download.py                    # Executa o Pipeline 1 (Download ANVISA)
├── main_nfe.py                    # Executa o Pipeline 3 (NFe + Matching)
│
├── nfe/                           # [INPUT] Coloque seus arquivos de NFe aqui
│   └── INSIRA_AS_NFE_AQUI.txt
│
├── pipelines/                     # Código fonte dos pipelines
│   ├── anvisa_base/               # Pipelines 1 e 2
│   └── nfe/                       # Pipeline 3
│
├── data/                          # Armazenamento de dados intermediários
│   ├── raw/                       # Dados brutos
│   └── processed/                 # Dados processados entre etapas
│
├── output/                        # [OUTPUT] Saída final da base ANVISA
│   └── anvisa/
│       └── baseANVISA.csv         # Base Mestra processada
│
└── QlikView/                      # [OUTPUT] Saída final do Pipeline NFe
    ├── df_central.csv             # Tabela fato principal
    └── (tabelas dimensão)
```

## Instalação

1.  Clone o repositório.
2.  Instale as dependências:
    ```bash
    pip install -r requirements.txt
    ```

---

## 🚀 Como Executar

### 1. Pipeline de Download (ANVISA)
Baixa os arquivos históricos e atuais de preços de medicamentos (PMVG) do portal da ANVISA.

**Comando:**
```bash
python download.py
```
*   **Configuração:** `pipelines/anvisa_base/config_anvisa.py` (ajuste anos/meses se necessário).
*   **Saída:** Gera `data/processed/anvisa/base_anvisa_precos_vigencias.csv`.

### 2. Pipeline de Processamento (ANVISA)
Lê os arquivos baixados, padroniza nomes, limpa dados e unifica vigências para criar a Base Mestra.

**Comando:**
```bash
python main.py
```
*   **Entrada:** `data/processed/anvisa/base_anvisa_precos_vigencias.csv`
*   **Saída:** `output/anvisa/baseANVISA.csv`

### 3. Pipeline de NFe (Matching e Enriquecimento)
Processa suas notas fiscais, cruza com a Base Mestra da ANVISA e enriquece com IA os itens não identificados.

**Pré-requisitos:**
*   Ter executado os passos 1 e 2.
*   Colocar seu arquivo de notas fiscais em `nfe/nfe.csv`.

**Comando:**
```bash
python main_nfe.py
```
*   **Funcionalidades:**
    *   Limpeza e normalização de descrições.
    *   Matching por EAN e Fuzzy Matching (descrição).
    *   Integração com base manual (`support/base_manual.xlsx`).
    *   Extração de atributos via IA para itens sem match.
    *   Particionamento de dados para QlikView.
*   **Saída:** Arquivos finais na pasta `QlikView/`.

## Detalhes Técnicos

### Pipeline ANVISA
*   **Módulos:** Limpeza, Unificação de Vigências, Classificação Terapêutica, Princípio Ativo, etc.
*   **Localização:** `pipelines/anvisa_base/src/modules/`

### Pipeline NFe
*   **Etapas:** O pipeline é dividido em 22 etapas sequenciais (limpeza, matching, validação, IA, consolidação).
*   **Localização:** `pipelines/nfe/src/`

## Contribuição
Para contribuir, certifique-se de seguir a estrutura de pastas e atualizar os testes correspondentes.


### unificacao_vigencias.py
**Função principal:** `unificar_vigencias_consecutivas(df)`

Funcionalidades:
- Identifica registros consecutivos com valores idênticos
- Unifica vigências que se sobrepõem
- Reduz significativamente o número de registros
- Mantém a integridade dos dados

### classificacao_terapeutica.py
**Função principal:** `processar_classificacao_terapeutica(df)`

Funcionalidades:
- Padroniza códigos ATC na coluna 'CLASSE TERAPÊUTICA'
- Cria backup para permitir re-execuções
- Gera coluna 'GRUPO ANATOMICO' baseada nos códigos ATC
- Categoriza medicamentos por sistema anatômico

### principio_ativo.py
**Função principal:** `processar_principio_ativo(df, executar_fuzzy_matching=False)`

Funcionalidades:
- **Etapa 1:** Normalização inicial e criação de backup
- **Etapa 2:** Remoção de acentos de colunas de texto
- **Etapa 3:** Correções usando dicionário principal (300+ regras)
- **Etapa 4:** Preenchimento inteligente de valores "Não Especificado"
- **Etapa 5:** Correções direcionadas com regex
- **Etapa 6:** Consolidação final usando fuzzy matching
- **Etapa 7:** Análise de similaridade (opcional)
- Renomeia coluna 'TIPO DE PRODUTO (STATUS DO PRODUTO)' para 'STATUS'
- Exporta lista de princípios ativos únicos

### dicionarios_correcao.py
Contém todos os dicionários de correção:
- `DICIONARIO_DE_CORRECAO` - Regras principais de padronização
- `DIC_SUGERIDO_ATIVO` - Correções baseadas em fuzzy matching
- `CORRECOES_CONTAINS` - Correções direcionadas
- `COLUNAS_PARA_NORMALIZAR` - Colunas para remoção de acentos

### produto.py
**Função principal:** `processar_produto(df)`

Funcionalidades:
- **Etapa 1:** Remove produtos de teste
- **Etapa 2:** Normaliza coluna 'STATUS' (renomeia 'TIPO DE PRODUTO (STATUS DO PRODUTO)')
- **Etapa 3:** Segmenta descrições genéricas de PRODUTO
- **Etapa 4:** Aplica dicionários de correção ortográfica
- **Etapa 5:** Correções direcionadas com regras específicas
- Exporta lista de produtos únicos

### dicionarios_produto.py
Contém todos os dicionários de correção para produtos:
- `NAO_SEPARA` - Termos que não devem ser separados (ex: "meia vida")
- `SAL_NOMES` - Nomes de sais químicos (cloridrato, sulfato, etc.)
- `SAL_FORMAS` - Formas farmacêuticas (comprimido, cápsula, etc.)
- `DICIONARIO_CORRECAO_PRODUTO` - Regras de correção ortográfica
- `PRE_REPLACERS` - Substituições antes do processamento principal
- `POST_FIX_RULES` - Substituições após processamento
- `DIC_SUGERIDO_PRODUTO` - Correções baseadas em fuzzy matching
- `CORRECOES_CONTAINS_PRODUTO` - Correções direcionadas por substring

### apresentacao.py
**Função principal:** `processar_apresentacao(df)`, `criar_flag_substancia_composta(df)`

Funcionalidades:
- **Normalização de apresentação farmacêutica:**
  - Ajuste de espaçamento ao redor de '+'
  - Aplicação de 100+ regras de padronização (PADRONIZACOES)
  - Remoção de termos irrelevantes (materiais de embalagem, sabores, etc.)
  - Formatação inteligente de dosagens com detecção de contexto (BOLSA, PO)
  - Parsing de valores numéricos compostos (ex: "(50 + 12.5) MG")
  - Mesclagem de blocos adjacentes com mesma unidade
  - Limpeza final com 40+ regras específicas
  - Expansão de quantidades (ex: "CX 250 BL X 4" → "BL X 1000")

- **Criação de flag de substância composta:**
  - Identifica medicamentos com múltiplos princípios ativos (contém '+')
  - Usado para lógica condicional na normalização

### tipo_produto.py
**Função principal:** `processar_tipo_produto(df)`

Funcionalidades:
- **Categorização de tipo de produto:**
  - Identifica forma farmacêutica baseada em palavras-chave
  - Categorias: FRASCO, AMPOLA/FRASCO-AMPOLA, DISPOSITIVOS, COMPRIMIDO/CAPSULA, BISNAGA, BOLSA, SACHE/PO, OUTROS
  - Hierarquia de prioridade nas regras de detecção
  - Primeira correspondência é retornada
  - Exibe distribuição de categorias após processamento

### dosagem.py
**Função principal:** `processar_dosagem(df, debug=False)`

Funcionalidades:
- **Extração de quantidades e dosagens:**
  - **QUANTIDADE UNIDADES**: Número de itens primários (frascos, ampolas, blisters)
  - **QUANTIDADE MG**: Soma de todas as dosagens em miligramas (converte G e MCG)
  - **QUANTIDADE ML**: Soma de todos os volumes em mililitros
  - **QUANTIDADE UI**: Soma de todas as Unidades Internacionais
  - Hierarquia de regras regex para detecção confiável:
    1. CX_NUM_ITEM: "CX 10 FA"
    2. NUM_ITEM: "50 FA"
    3. CX_SIMPLES: "CX 50"
    4. X_GENERICO: "BL X 30"
    5. FALLBACK_1_ITEM: Assume 1 quando detecta palavra de item
  - Tratamento especial para BISNAGA (extração de G)
  - Preenche NaN com 1 quando não detectado
  - Exibe estatísticas de cobertura por tipo de dosagem

## Arquivos Gerados

### Entrada
- `base_anvisa_precos_vigencias.csv` - Gerado pelo `baixar.py`

### Saída
- `produtos_cmed.csv` - Arquivo final processado
- `principios_ativos_unicos.txt` - Lista ordenada de princípios ativos únicos
- `produtos_unicos.txt` - Lista ordenada de produtos únicos

### Colunas Adicionadas pelo Pipeline

1. **CLASSE_TERAPEUTICA_ORIGINAL** - Backup da coluna original
2. **PRINCIPIO_ATIVO_ORIGINAL** - Backup do princípio ativo original
3. **PRODUTO_ORIGINAL** - Backup do produto original
4. **STATUS** - Renomeação e normalização de 'TIPO DE PRODUTO (STATUS DO PRODUTO)'
5. **GRUPO ANATOMICO** - Categorização por sistema anatômico:
   - ANTINEOPLÁSICOS E IMUNOMODULADORES
   - ANTI-INFECCIOSOS DE USO SISTÊMICO
   - TRATO ALIMENTAR E METABOLISMO
   - SANGUE E ÓRGÃOS HEMATOPOÉTICOS
   - SOLUÇÕES INTRAVENOSAS
   - SISTEMA CARDIOVASCULAR
   - SISTEMA MÚSCULO-ESQUELÉTICO
   - HORMÔNIOS SISTÊMICOS, EXCETO SEXUAIS E INSULINAS
   - SISTEMA RESPIRATÓRIO
   - DERMATOLÓGICOS
   - SISTEMA GENITURINÁRIO E HORMÔNIOS SEXUAIS
   - ÓRGÃOS SENSORIAIS
   - ANTIPARASITÁRIOS
   - SISTEMA NERVOSO-PSICONEUROLÓGICOS
   - SISTEMA NERVOSO-ANESTÉSICOS E ANALGÉSICOS
   - VÁRIOS (para códigos não categorizados)
6. **SUBSTANCIA_COMPOSTA** - Flag booleana indicando medicamentos com múltiplos princípios ativos
7. **APRESENTACAO_NORMALIZADA** - Apresentação farmacêutica padronizada e limpa
8. **TIPO DE PRODUTO** - Categoria da forma farmacêutica (FRASCO, COMPRIMIDO/CAPSULA, etc.)
9. **QUANTIDADE UNIDADES** - Número de itens primários na embalagem
10. **QUANTIDADE MG** - Dosagem total em miligramas
11. **QUANTIDADE ML** - Volume total em mililitros
12. **QUANTIDADE UI** - Total de Unidades Internacionais

## Uso Individual dos Módulos

Cada módulo pode ser usado independentemente:

```python
import pandas as pd
from config import configurar_pandas
from limpeza_dados import limpar_padronizar_dados
from unificacao_vigencias import unificar_vigencias_consecutivas
from classificacao_terapeutica import processar_classificacao_terapeutica
from principio_ativo import processar_principio_ativo, exportar_principios_ativos_unicos
from produto import processar_produto, exportar_produtos_unicos

# Configurar pandas
configurar_pandas()

# Carregar dados
df = pd.read_csv('base_anvisa_precos_vigencias.csv')

# Aplicar apenas limpeza
df_limpo = limpar_padronizar_dados(df)

# Aplicar apenas unificação
df_unificado = unificar_vigencias_consecutivas(df_limpo)

# Aplicar apenas classificação
df_classificado = processar_classificacao_terapeutica(df_unificado)

# Aplicar processamento de princípio ativo
df_com_pa = processar_principio_ativo(df_classificado)

# Aplicar processamento de produto
df_final = processar_produto(df_com_pa)

# Exportar listas únicas
exportar_principios_ativos_unicos(df_final)
exportar_produtos_unicos(df_final)
```

## Requisitos

Instale as dependências usando:

```bash
pip install -r requirements.txt
```

Pacotes necessários:
- pandas>=2.0.0
- numpy>=1.24.0
- requests>=2.31.0
- beautifulsoup4>=4.12.0
- tqdm>=4.65.0
- rapidfuzz>=3.0.0

## Benefícios da Modularização

1. **Manutenibilidade** - Cada função tem responsabilidade específica
2. **Reutilização** - Módulos podem ser usados independentemente
3. **Testabilidade** - Cada módulo pode ser testado separadamente
4. **Legibilidade** - Código mais organizado e fácil de entender
5. **Flexibilidade** - Possibilidade de executar apenas partes do pipeline
6. **Escalabilidade** - Fácil adicionar novas etapas de processamento

## Detalhes do Processamento de Princípio Ativo

O módulo `principio_ativo.py` implementa um pipeline sofisticado em 7 etapas:

### Etapa 1: Normalização Inicial
- Cria backup da coluna original (`PRINCIPIO_ATIVO_ORIGINAL`)
- Converte para maiúsculas
- Substitui `;` por ` + ` em associações
- Remove valores nulos

### Etapa 2: Remoção de Acentos
Remove acentos das colunas:
- PRINCÍPIO ATIVO
- LABORATÓRIO
- PRODUTO
- APRESENTAÇÃO

### Etapa 3: Correções com Dicionário Principal
Aplica **300+ regras** de correção usando regex com limites de palavra (`\b`):
- Padroniza formas hidratadas (trihidratado, dihidratado, etc.)
- Corrige erros ortográficos comuns
- Remove informações redundantes
- Padroniza nomes de sais
- Remove abreviações científicas (L., LAM., etc.)

### Etapa 4: Preenchimento Inteligente
- Identifica registros com "Não Especificado"
- Cria mapa de imputação baseado em PRODUTO + APRESENTAÇÃO
- Preenche com o princípio ativo mais comum para aquela combinação

### Etapa 5: Correções Direcionadas
Aplica regras específicas de `str.replace`:
- Remove pontos e caracteres especiais
- Limpa sufixos hidratados remanescentes
- Remove referências a legislação (PORT 344/98)

### Etapa 6: Consolidação Final
Aplica dicionário de fuzzy matching com **60+ regras** adicionais:
- Unifica variações de nomes
- Padroniza associações de medicamentos
- Corrige nomes científicos

### Etapa 7: Análise de Similaridade (Opcional)
- Usa biblioteca `rapidfuzz`
- Encontra pares de nomes similares (>85% de similaridade)
- Gera sugestões de correção para revisão manual

## Detalhes do Processamento de Produto

O módulo `produto.py` implementa um pipeline especializado em 5 etapas:

### Etapa 1: Remoção de Produtos de Teste
- Remove registros onde PRODUTO contém "TESTE"
- Limpa base de dados de registros não comerciais

### Etapa 2: Normalização do STATUS
- Renomeia coluna 'TIPO DE PRODUTO (STATUS DO PRODUTO)' para 'STATUS'
- Simplifica nome da coluna para uso posterior

### Etapa 3: Segmentação Inteligente de Descrições Genéricas
Identifica e separa produtos genéricos que contêm múltiplos componentes:
- Detecta produtos com '/' que indicam múltiplos princípios ativos
- Respeita exceções que não devem ser separadas (ex: "MEIA VIDA")
- Preserva nomes de sais químicos intactos
- Mantém formas farmacêuticas juntas
- Regras especiais para associações:
  - Associações sem apresentação (ex: "PARACETAMOL/CODEINA") → separa
  - Associações com apresentação completa → preserva como está
- Aplica regex inteligente para detectar padrões de separação

### Etapa 4: Correções com Dicionários
Aplica múltiplos dicionários de correção:
- **PRE_REPLACERS**: Substituições antes do processamento principal
- **DICIONARIO_CORRECAO_PRODUTO**: Regras de padronização ortográfica
- **POST_FIX_RULES**: Substituições após processamento
- **DIC_SUGERIDO_PRODUTO**: Correções baseadas em fuzzy matching

### Etapa 5: Correções Direcionadas
Aplica correções específicas baseadas em substrings:
- Usa dicionário `CORRECOES_CONTAINS_PRODUTO`
- Identifica e corrige padrões específicos em nomes de produtos
- Padroniza variações de grafia

## Log de Execução

O pipeline fornece logs detalhados de cada etapa:
- Contagem de registros antes e depois de cada transformação
- Estatísticas de redução de dados
- Verificação de colunas criadas
- Tempo de execução
- Tratamento de erros com detalhes
- Barras de progresso com `tqdm` para operações longas

## Arquivos Gerados

Além do arquivo principal `produtos_cmed.csv`, o pipeline gera:
- `principios_ativos_unicos.txt` - Lista ordenada de todos os princípios ativos únicos após processamento