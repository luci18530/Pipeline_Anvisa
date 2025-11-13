# 📦 Módulo de Processamento de Notas Fiscais (NFe)

Pipeline completo de carregamento, processamento e análise de dados de Notas Fiscais Eletrônicas.

## 📋 Estrutura

```
Pipeline_Anvisa/
├── nfe/                          # Dados de entrada (não versionados)
│   └── nfe.csv                   # Arquivo CSV com dados brutos
├── src/
│   └── nfe_carregamento.py       # Módulo de carregamento e pré-processamento
├── scripts/
│   └── processar_nfe.py          # Script executável
└── data/
    └── processed/                # Dados processados (gerados)
```

## 🚀 Quick Start

### 1. Preparar Dados

Coloque seu arquivo CSV na pasta `nfe/`:
```
nfe/nfe.csv
```

### 2. Executar Processamento

```bash
python scripts/processar_nfe.py
```

### 3. Resultados

Os dados processados são salvos em:
- `data/processed/nfe_processado_YYYYMMDD_HHMMSS.parquet`
- `data/processed/nfe_processado_YYYYMMDD_HHMMSS.csv`

## 📊 Processamento Realizado

### 1. Carregamento Robusto
- Detecção automática de encoding (latin1, cp1252, utf-8, utf-16)
- Tratamento de erros de leitura
- Suporte a diferentes formatos de CSV

### 2. Normalização
- Remove caracteres especiais dos nomes de colunas
- Remove espaços e BOMs
- Padroniza nomes de colunas

### 3. Processamento de Datas
- Converte `data_emissao` para datetime
- Filtra registros anteriores a 2020-01-01
- Cria colunas `ano_emissao` e `mes_emissao`
- Mantém backup da data original

### 4. Filtragem de Qualidade
- Remove unidades inválidas:
  - BLOCO, TESTE, TES, T, TS, TST, KT, DZ, TBL
  - BOMB, BD, JG, FD18, CXA1, BD38

### 5. Conversão Numérica
- `valor_produtos` → float
- `valor_unitario` → float
- `quantidade` → float
- Tratamento de valores inválidos (NaN)

## 🔧 Uso Programático

### Exemplo Básico

```python
from src.nfe_carregamento import carregar_e_processar_nfe

# Carregar e processar
df = carregar_e_processar_nfe('nfe/nfe.csv')

# Usar DataFrame
print(df.head())
print(f"Total de registros: {len(df):,}")
```

### Exemplo Avançado

```python
from src.nfe_carregamento import (
    carregar_csv_nfe,
    preprocessar_nfe,
    salvar_dados_processados
)

# Carregar com encoding específico
df = carregar_csv_nfe('nfe/nfe.csv', encoding='latin1')

# Processar com data mínima customizada
df = preprocessar_nfe(df, data_minima='2022-01-01')

# Salvar em formato específico
salvar_dados_processados(df, formato='parquet')
```

## 📈 Colunas Esperadas

O arquivo CSV deve conter as seguintes colunas:

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `id_descricao` | string | ID da descrição |
| `descricao_produto` | string | Descrição do produto |
| `id_medicamento` | string | ID do medicamento |
| `cod_anvisa` | string | Código ANVISA |
| `codigo_municipio_destinatario` | string | Código do município |
| `data_emissao` | date | Data de emissão da NFe |
| `codigo_ncm` | string | Código NCM |
| `codigo_ean` | string | Código EAN |
| `valor_produtos` | float | Valor total dos produtos |
| `valor_unitario` | float | Valor unitário |
| `quantidade` | float | Quantidade |
| `unidade` | string | Unidade de medida |
| `cpf_cnpj_emitente` | string | CPF/CNPJ do emitente |
| `chave_codigo` | string | Chave de acesso da NFe |
| `cpf_cnpj` | string | CPF/CNPJ do destinatário |
| `razao_social_emitente` | string | Razão social do emitente |
| `nome_fantasia_emitente` | string | Nome fantasia do emitente |
| `razao_social_destinatario` | string | Razão social do destinatário |
| `nome_fantasia_destinatario` | string | Nome fantasia do destinatário |
| `id_data_fabricacao` | string | Data de fabricação |
| `id_data_validade` | string | Data de validade |

## 🔍 Funções Principais

### `carregar_csv_nfe(caminho_csv, encoding=None)`
Carrega arquivo CSV com detecção automática de encoding.

### `preprocessar_nfe(df, data_minima='2020-01-01')`
Pipeline completo de pré-processamento.

### `carregar_e_processar_nfe(caminho_csv, data_minima='2020-01-01', encoding=None)`
Função principal: carrega e processa em uma única chamada.

### `salvar_dados_processados(df, diretorio='data/processed', formato='parquet')`
Salva DataFrame processado em formato parquet ou csv.

## 📊 Estatísticas Geradas

O script exibe automaticamente:
- Total de registros processados
- Período de dados (data mínima e máxima)
- Distribuição por ano
- Valor total de produtos
- Quantidade total
- Número de emitentes únicos
- Número de produtos únicos

## ⚠️ Notas Importantes

### Encoding
- O módulo tenta múltiplos encodings automaticamente
- Se souber o encoding, especifique para melhor performance

### Memória
- Arquivos grandes podem consumir muita memória
- Use formato parquet para arquivos > 1GB
- Considere processar em chunks para arquivos muito grandes

### Performance
- Primeira execução pode ser lenta (conversões de tipo)
- Leituras subsequentes do parquet são muito mais rápidas
- Use `low_memory=False` para melhor consistência de tipos

## 🐛 Solução de Problemas

### Erro: "Arquivo não encontrado"
```bash
# Verifique se o arquivo está na pasta correta
ls nfe/nfe.csv
```

### Erro: "Encoding inválido"
```python
# Especifique o encoding manualmente
df = carregar_csv_nfe('nfe/nfe.csv', encoding='latin1')
```

### Erro: "Memória insuficiente"
```python
# Processe em chunks (implementação futura)
# Por enquanto, filtre os dados antes de carregar
```

## 📝 Próximos Passos

Após o carregamento, os próximos módulos do pipeline incluirão:
1. Limpeza e padronização de nomes de produtos
2. Matching com base ANVISA
3. Classificação terapêutica
4. Análise e agregação
5. Geração de relatórios

---

**Última atualização:** Nov 13, 2025  
**Versão:** 1.0
