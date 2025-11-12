# Pipeline de Processamento da Anvisa

Este projeto contém um pipeline automatizado para baixar e processar dados de preços de medicamentos da Anvisa (PMVG).

## Estrutura do Projeto

```
├── baixar.py                    # Script para baixar dados da Anvisa
├── processar_dados.py          # Script principal de processamento
├── config.py                   # Configurações e constantes
├── limpeza_dados.py            # Módulo de limpeza e padronização
├── unificacao_vigencias.py     # Módulo de unificação de vigências
├── classificacao_terapeutica.py # Módulo de classificação terapêutica
├── principio_ativo.py          # Módulo de processamento de princípio ativo
├── dicionarios_correcao.py     # Dicionários de correção (princípio ativo)
├── produto.py                  # Módulo de processamento de produto
├── dicionarios_produto.py      # Dicionários de correção (produto)
├── apresentacao.py             # Módulo de normalização de apresentação
├── tipo_produto.py             # Módulo de categorização de tipo de produto
├── dosagem.py                  # Módulo de extração de dosagens
├── requirements.txt            # Dependências do projeto
└── README.md                   # Este arquivo
```

## Sequência de Execução

### 1. Baixar Dados da Anvisa
```bash
python baixar.py
```
- Baixa os arquivos de preços da Anvisa
- Gera o arquivo `base_anvisa_precos_vigencias.csv`

### 2. Processar Dados
```bash
python processar_dados.py
```
- Processa o arquivo `base_anvisa_precos_vigencias.csv`
- Gera o arquivo final `produtos_cmed.csv`

## Módulos de Processamento

### config.py
- Configurações do pandas e numpy
- Constantes do pipeline
- Mapeamento de grupos anatômicos
- Colunas para verificação de mudanças

### limpeza_dados.py
**Função principal:** `limpar_padronizar_dados(df)`

Funcionalidades:
- Padronização da coluna 'CÓDIGO GGREM'
- Padronização das colunas EAN (EAN 1, EAN 2, EAN 3)
- Remove caracteres não numéricos
- Trata valores nulos e vazios

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