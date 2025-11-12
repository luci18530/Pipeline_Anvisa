# Pipeline ANVISA - Estrutura Completa

## 📋 Visão Geral
Pipeline completo de processamento de dados da ANVISA, dividido em **10 etapas** sequenciais.

## 🗂️ Estrutura de Arquivos

### 📌 Scripts Principais
1. **`baixar.py`** - Download e consolidação de dados da ANVISA
2. **`processar_dados.py`** - Orquestrador principal do pipeline (10 etapas)

### 🔧 Módulos de Processamento

#### Etapa 1-2: Preparação
- **`config.py`** - Configurações globais e constantes
- **`limpeza_dados.py`** - Limpeza e padronização inicial
- **`unificacao_vigencias.py`** - Unificação de vigências consecutivas

#### Etapa 3-4: Classificação
- **`classificacao_terapeutica.py`** - Padronização de códigos ATC e grupo anatômico
- **`dicionarios_correcao.py`** - Dicionários de correção para princípio ativo (300+ regras)
- **`principio_ativo.py`** - Processamento de princípio ativo (7 estágios)

#### Etapa 5: Produto
- **`dicionarios_produto.py`** - Dicionários de correção para produto
- **`produto.py`** - Processamento e segmentação de produto (5 estágios)

#### Etapa 6-7: Apresentação e Dosagem
- **`apresentacao.py`** - Normalização de apresentações farmacêuticas (100+ regras)
- **`tipo_produto.py`** - Categorização de formas farmacêuticas
- **`dosagem.py`** - Extração de quantidades e dosagens (5 níveis hierárquicos)

#### Etapa 8-9: Laboratório e Grupo Terapêutico
- **`laboratorio.py`** - Normalização de nomes de laboratórios
- **`grupo_terapeutico.py`** - Mapeamento de grupos terapêuticos (download externo + joins)

#### Etapa 10: Finalização
- **`correcoes_ortograficas.py`** - Correções ortográficas e químicas (47 regras)
- **`finalizacao.py`** - Padronização final e exportações

## 🚀 Pipeline Completo (10 Etapas)

### ETAPA 1: Limpeza e Padronização
```python
from limpeza_dados import limpar_padronizar_dados
df = limpar_padronizar_dados(df)
```
- Remove duplicatas
- Padroniza tipos de dados
- Limpa espaços e caracteres especiais

### ETAPA 2: Unificação de Vigências
```python
from unificacao_vigencias import unificar_vigencias_consecutivas
df = unificar_vigencias_consecutivas(df)
```
- Consolida períodos consecutivos
- Otimiza registros temporais

### ETAPA 3: Classificação Terapêutica
```python
from classificacao_terapeutica import processar_classificacao_terapeutica
df = processar_classificacao_terapeutica(df)
```
- Padroniza códigos ATC
- Cria grupo anatômico
- Normaliza nomenclatura terapêutica

### ETAPA 4: Princípio Ativo
```python
from principio_ativo import processar_principio_ativo
df = processar_principio_ativo(df, executar_fuzzy_matching=False)
```
**7 Estágios:**
1. Normalização inicial e backup
2. Remoção de acentos
3. Correções via dicionário (300+ regras)
4. Preenchimento de não especificados
5. Correções direcionadas (contains)
6. Consolidação final
7. Correções ortográficas e químicas

### ETAPA 5: Produto
```python
from produto import processar_produto
df = processar_produto(df)
```
**5 Estágios:**
1. Remoção de produtos teste/tabelado
2. Normalização de STATUS
3. Segmentação inteligente
4. Aplicação de dicionário sugerido
5. Correções direcionadas + ortográficas

### ETAPA 6: Apresentação
```python
from apresentacao import criar_flag_substancia_composta, processar_apresentacao
df = criar_flag_substancia_composta(df)
df = processar_apresentacao(df)
```
- Cria flag de substância composta
- Aplica 100+ regras de padronização
- Expande abreviações (CX, BL, etc.)
- Normalização inteligente com contexto

### ETAPA 7: Tipo de Produto e Dosagem
```python
from tipo_produto import processar_tipo_produto
from dosagem import processar_dosagem
df = processar_tipo_produto(df)
df = processar_dosagem(df, debug=False)
```
**Categorias:** FRASCO, AMPOLA, DISPOSITIVOS, COMPRIMIDO/CÁPSULA, BISNAGA, BOLSA, SACHÊ/PÓ, OUTROS

**Dosagens extraídas:**
- QUANTIDADE UNIDADES
- QUANTIDADE MG
- QUANTIDADE ML
- QUANTIDADE UI

### ETAPA 8: Laboratório
```python
from laboratorio import processar_laboratorio
df = processar_laboratorio(df)
```
- Remove sufixos empresariais (LTDA, SA, EIRELI, EPP)
- Normaliza espaços
- Cria backup LABORATORIO_ORIGINAL

### ETAPA 9: Grupo Terapêutico
```python
from grupo_terapeutico import processar_grupo_terapeutico
df = processar_grupo_terapeutico(df, criar_debug=True)
```
- Baixa base externa (Google Sheets)
- Normaliza códigos ATC
- Faz mapeamento via dicionário (performance)
- Gera 3 arquivos de debug (Excel)

### ETAPA 10: Finalização e Exportação
```python
from finalizacao import processar_finalizacao
df = processar_finalizacao(df)
```

**Padronização:**
- Renomeia colunas originais para histórico
- Remove colunas intermediárias
- Renomeia colunas consolidadas
- Padroniza nomes (uppercase)

**Exportações:**
1. **`baseANVISA.csv`** - Para uso em pipeline (TSV)
2. **`baseANVISA_dtypes.json`** - Tipos de dados
3. **`dfprodutos.csv`** - Dataset completo
4. **`dfpro_correcao_manual.xlsx`** - Para análise manual (sem duplicatas)

## 📊 Arquivos de Saída

### Principais
| Arquivo | Formato | Propósito | Duplicatas |
|---------|---------|-----------|------------|
| `baseANVISA.csv` | TSV | Pipeline downstream | Mantém |
| `baseANVISA_dtypes.json` | JSON | Metadados de tipos | - |
| `dfprodutos.csv` | CSV | Dataset completo | Mantém |
| `dfpro_correcao_manual.xlsx` | Excel | Análise manual | Remove |

### Referência
| Arquivo | Conteúdo |
|---------|----------|
| `principios_ativos_unicos.txt` | Lista única de princípios ativos |
| `produtos_unicos.txt` | Lista única de produtos |

### Debug (Grupo Terapêutico)
| Arquivo | Conteúdo |
|---------|----------|
| `df_grupos_com_principio_ativo.xlsx` | Join completo (debug) |
| `df_grupos_sem_match.xlsx` | Classes não encontradas |
| `dfpro_sem_match_grupos.xlsx` | Registros sem correspondência |

## 🎯 Colunas Finais Exportadas

### Ordem de Exportação Completa
```python
[
    'ID_CMED_PRODUTO',      # Identificador único
    'GRUPO ANATOMICO',       # Classificação anatômica
    'PRINCIPIO ATIVO',       # Substância ativa
    'PRODUTO',               # Nome do medicamento
    'STATUS',                # Situação do registro
    'APRESENTACAO',          # Forma farmacêutica
    'TIPO DE PRODUTO',       # Categoria (FRASCO, AMPOLA, etc.)
    'QUANTIDADE UNIDADES',   # Qtd. em unidades
    'QUANTIDADE MG',         # Qtd. em miligramas
    'QUANTIDADE ML',         # Qtd. em mililitros
    'QUANTIDADE UI',         # Qtd. em unidades internacionais
    'LABORATORIO',           # Fabricante
    'CLASSE TERAPEUTICA',    # Código ATC
    'GRUPO TERAPEUTICO',     # Grupo terapêutico
    'GGREM',                 # Código GGREM
    'EAN_1',                 # Código de barras 1
    'EAN_2',                 # Código de barras 2
    'EAN_3',                 # Código de barras 3
    'REGISTRO'               # Número de registro
]
```

## 📈 Estatísticas do Pipeline

### Regras e Correções
- **300+ regras** de correção de princípio ativo
- **100+ regras** de normalização de apresentação
- **47 regras** de correção ortográfica e química
- **5 níveis hierárquicos** de extração de dosagem
- **8 categorias** de tipo de produto

### Módulos
- **18 arquivos Python** modulares
- **~200-600 linhas** por módulo (altamente modularizado)
- **10 etapas** de processamento sequencial

### Performance
- **Dicionários** para lookups rápidos (vs merges)
- **Backup automático** de colunas originais
- **Debug opcional** para fuzzy matching
- **Progress bars** com tqdm

## ✅ Validação

```bash
# Validar sintaxe de todos os módulos
python -m py_compile *.py

# Testar imports
python -c "import finalizacao; print('OK')"
python -c "import correcoes_ortograficas; print('OK')"
python -c "import grupo_terapeutico; print('OK')"
python -c "import laboratorio; print('OK')"

# Executar pipeline completo
python processar_dados.py
```

## 🔄 Fluxo de Execução

```
baixar.py
    ↓
base_anvisa_precos_vigencias.csv
    ↓
processar_dados.py
    ├─ ETAPA 1: Limpeza
    ├─ ETAPA 2: Vigências
    ├─ ETAPA 3: Classificação Terapêutica
    ├─ ETAPA 4: Princípio Ativo
    ├─ ETAPA 5: Produto
    ├─ ETAPA 6: Apresentação
    ├─ ETAPA 7: Tipo Produto + Dosagem
    ├─ ETAPA 8: Laboratório
    ├─ ETAPA 9: Grupo Terapêutico
    └─ ETAPA 10: Finalização
         ├─ baseANVISA.csv
         ├─ baseANVISA_dtypes.json
         ├─ dfprodutos.csv
         └─ dfpro_correcao_manual.xlsx
```

## 📝 Notas Técnicas

### Travas de Segurança
- Padronização alfabética **bloqueada** para: FURP, LQFEX, ISOFARMA, FRACAO
- Fuzzy matching **desabilitado por padrão** (performance)
- Backup automático de **todas as colunas originais**

### Configurações Importantes
- **Separador TSV** para baseANVISA.csv (compatibilidade)
- **UTF-8** encoding em todos os arquivos
- **openpyxl** engine para Excel exports
- **gdown** para download de base externa

### Dependências
```
pandas>=2.0.0
numpy>=1.24.0
tqdm>=4.65.0
rapidfuzz>=3.0.0
requests>=2.31.0
beautifulsoup4>=4.12.0
gdown
openpyxl
```

## 🎉 Status
✅ **Pipeline Completo e Funcional**
- Todos os módulos criados
- Sintaxe validada
- Imports testados
- Pronto para execução
