# 📅 Módulo de Processamento de Vencimento de NFe

Pipeline completo para análise e categorização de vencimento de medicamentos em Notas Fiscais.

## 📋 Visão Geral

O módulo processa datas de fabricação, validade e emissão para:
- Calcular métricas de vida útil dos produtos
- Categorizar status de vencimento
- Particionar dados para análise dedicada

## 🚀 Execução Rápida

```bash
# 1. Processar vencimentos
python scripts/processar_vencimento.py

# 2. Validar resultados
python scripts/validar_vencimento.py
```

## 📊 Etapas do Pipeline

### Etapa 1: Limpeza de Datas
- Remove espaços em branco
- Converte valores inválidos para NaT
- Formata YYYYMMDD para YYYY-MM-DD
- Remove datas placeholder (2000-01-01, 2010-01-01, 2020-01-01)

### Etapa 2: Cálculo de Métricas
Calcula para cada produto:
- **vida_total**: dias entre fabricação e validade
- **vida_usada**: dias entre fabricação e emissão
- **dias_restantes**: dias entre emissão e validade
- **vida_usada_porcento**: percentual de vida utilizada

### Etapa 3: Categorização
Classifica em 5 categorias:

| Categoria | Critério | Ação |
|-----------|----------|------|
| **VENCIDO** | Emissão > Validade | ❌ Remover imediatamente |
| **MUITO PROXIMO AO VENCIMENTO** | ≥75% vida + <365d restantes | ⚠️ Verificar prioridade |
| **PROXIMO AO VENCIMENTO** | 25-75% vida + <365d restantes | ⏱️ Monitorar |
| **PRAZO ACEITAVEL** | <75% vida OU >365d restantes | ✅ Utilizar normalmente |
| **INDETERMINADO** | Dados insuficientes/inválidos | ❓ Investigar |

### Etapa 4: Particionamento
Separa dados em:
- **df_venc**: apenas métricas e categorias de vencimento
- **df_base**: dados originais sem métricas (para próxima etapa)

## 🔧 Funções Principais

### `limpar_datas(serie)`
Padroniza uma série de datas com diversos formatos.

```python
from src.nfe_vencimento import limpar_datas

datas_limpas = limpar_datas(df['id_data_validade'])
```

### `calcular_metricas_vencimento(df)`
Calcula vida útil e dias restantes.

```python
from src.nfe_vencimento import calcular_metricas_vencimento

df = calcular_metricas_vencimento(df)
```

### `categorizar_vencimento(df)`
Classifica em categorias de vencimento.

```python
from src.nfe_vencimento import categorizar_vencimento

df = categorizar_vencimento(df)
```

### `processar_vencimento_nfe(df)`
Pipeline completo (etapas 1-4).

```python
from src.nfe_vencimento import processar_vencimento_nfe

df_base, df_venc = processar_vencimento_nfe(df)
```

## 📈 Exemplo de Resultado

Dos 46.389 registros processados:

| Categoria | Registros | % |
|-----------|-----------|-----|
| PRAZO ACEITAVEL | 27.117 | 58,5% |
| INDETERMINADO | 13.028 | 28,1% |
| PROXIMO AO VENCIMENTO | 5.059 | 10,9% |
| MUITO PROXIMO AO VENCIMENTO | 1.129 | 2,4% |
| VENCIDO | 56 | 0,1% |

Dias restantes:
- **Mediana**: 519 dias
- **Média**: 507 dias
- **Mínimo**: -2.887 dias (vencidos)
- **Máximo**: 13.995 dias

## 📁 Arquivos de Saída

Os dados processados são salvos em:
- `data/processed/nfe_vencimento_YYYYMMDD_HHMMSS.parquet`
- `data/processed/nfe_vencimento_YYYYMMDD_HHMMSS.csv`

## 📊 Estrutura dos Dados de Saída

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `id_venc` | string | ID único (chave_codigo) |
| `dt_fabricacao` | datetime | Data de fabricação |
| `dt_validade` | datetime | Data de validade |
| `dt_emissao` | datetime | Data de emissão da NFe |
| `vida_total` | int | Dias entre fabricação e validade |
| `vida_usada` | int | Dias entre fabricação e emissão |
| `dias_restantes` | int | Dias entre emissão e validade |
| `vida_usada_porcento` | float | % de vida utilizada |
| `categoria_vencimento` | string | Classificação de vencimento |

## 🔍 Casos Especiais

### INDETERMINADO
Registros são classificados como INDETERMINADO quando:
- Faltam datas críticas (fabricação ou validade)
- Mais de 3.650 dias (10 anos) de diferença
- Vida total é zero

### Valores Negros
- **dias_restantes < 0**: Produto vencido
- **vida_usada_porcento > 100%**: Produto fabricado após emissão (erro de dados)

## ⚙️ Configuração

### Alterar Limites

Edit `src/nfe_vencimento.py` na função `categorizar_vencimento()`:

```python
# Modificar limites de categorização
cond_muito_prox = (df['vida_usada_porcento'] >= 0.75) & (df['dias_restantes'] < 365)
#                                                                              ↑
#                                                        Alterar limite (dias)
```

## 📝 Próximos Passos

Após processamento de vencimento:
1. Análise exploratória de vencimentos
2. Relatórios por categoria
3. Alertas para produtos vencidos
4. Visualizações de tendências

---

**Última atualização:** Nov 13, 2025  
**Versão:** 1.0
