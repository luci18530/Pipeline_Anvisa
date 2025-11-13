# Debug de EANs Sem Match

## 📋 Visão Geral

O pipeline NFe inclui uma seção **DEBUG** opcional que analisa EANs que não tiveram match com a base ANVISA. Isso é útil para investigar produtos que não foram encontrados e ajustar a estratégia de matching.

## 🔧 Como Ativar

### Opção 1: Toggle no `main_nfe.py`

Abra `main_nfe.py` e procure por:

```python
def main():
    """Função principal"""
    
    # ⚙️ TOGGLE DE DEBUG - Altere para True para executar análise
    DEBUG_ENABLED = False
```

Altere para:

```python
    DEBUG_ENABLED = True
```

Agora quando você executar `python main_nfe.py`, após o pipeline completo, a análise DEBUG será executada automaticamente.

### Opção 2: Executar Manualmente

Você pode analisar um arquivo `nfe_matched_*.csv` diretamente:

```python
from main_nfe import analisar_eans_sem_match

# Analisar o arquivo mais recente
analisar_eans_sem_match('data/processed/nfe_matched_20251113_112049.csv', exportar=True)
```

## 📊 O Que é Analisado

### 1. Filtro de Registros Sem Match
- Identifica linhas onde coluna `PRODUTO` é nula ou vazia
- Agrupa por `codigo_ean` e `descricao_produto`
- Mantém apenas a descrição mais frequente para cada EAN

### 2. Métricas Calculadas

Para cada EAN sem match:
- **Frequência**: Quantas vezes aparece nos dados
- **Valor Total**: Soma de valores de produtos (R$)
- **Valor Médio**: Média de valores (R$)

### 3. Saída do DEBUG

O debug exibe:

#### Tabela 1: Top 50 por Frequência
```
codigo_ean                  descricao_produto                           Frequencia
──────────────────────────────────────────────────────────────────────────────────
7896123456789              DIPIRONA 500 MG 20 COMPRIMIDOS                      45
7896987654321              AMOXICILINA 500 MG 20 COMPRIMIDOS                   38
7894567890123              IBUPROFENO 400 MG 30 COMPRIMIDOS                    32
...
```

#### Tabela 2: Top 50 com Métricas Financeiras
```
codigo_ean        Frequencia    Valor_Total    Valor_Medio
──────────────────────────────────────────────────────────
7896123456789            45    R$ 4.234,50      R$ 94,10
7896987654321            38    R$ 3.895,00      R$ 102,50
7894567890123            32    R$ 2.456,80      R$ 76,78
...
```

## 📁 Arquivos Gerados

Quando DEBUG está ativo, 2 arquivos são criados em `data/processed`:

### `debug_eans_sem_match_TIMESTAMP.csv`
- Todas as análises simples (EAN + descrição + frequência)
- **Uso:** Identificar quais EANs não têm match

### `debug_eans_metricas_TIMESTAMP.csv`
- Análise com métricas financeiras
- **Uso:** Priorizar correções por valor

## 💡 Exemplos de Uso

### Cenário 1: Encontrar Produtos de Alto Valor Sem Match

1. Ative DEBUG (`DEBUG_ENABLED = True`)
2. Execute: `python main_nfe.py`
3. Procure em `debug_eans_metricas_*.csv` pelos maiores valores em `Valor_Total`
4. Investigue esses EANs manualmente

### Cenário 2: Analisar Frequência de Falhas

1. Abra `debug_eans_sem_match_*.csv`
2. Procure por EANs que aparecem 10+ vezes
3. Considere adicionar esses EANs manualmente à base ANVISA

### Cenário 3: Validar Melhorias de Matching

1. Rode o pipeline com DEBUG antes de uma melhoria
2. Anote o número de EANs sem match
3. Implemente a melhoria
4. Rode novamente e compare os resultados

## 📈 Estatísticas

O debug exibe no console:

```
[INFO] Registros sem PRODUTO (sem match): 5.364 (11.56%)
```

Isso mostra:
- **Número total** de registros sem match
- **Percentual** em relação ao total

## ⚠️ Notas Importantes

### Performance
- DEBUG executa **após** o pipeline completo
- Adiciona tempo de processamento (~30-60 segundos para 46k registros)
- Não afeta o pipeline principal

### Dados Sensíveis
- Exporta apenas EAN, descrição do produto e métricas
- Não inclui dados de clientes ou valores exatos por registro
- Arquivos podem ser compartilhados com fornecedores para análise

### Arquivo de Backup
- Os CSVs exportados ficam em `data/processed`
- Acompanham os timestamps do matching
- Podem ser mantidos para auditoria ou análise histórica

## 🔍 Código Completo

```python
# Para executar diretamente em um Jupyter ou script:

from main_nfe import analisar_eans_sem_match
import pandas as pd

# Carregar e analisar
analisar_eans_sem_match(
    arquivo_matched='data/processed/nfe_matched_20251113_112049.csv',
    exportar=True  # Salva os CSVs
)
```

## 🚀 Próximos Passos

Com a análise DEBUG, você pode:

1. **Adicionar EANs Manualmente** na base ANVISA
2. **Melhorar Algoritmo de Matching** para casos comuns
3. **Investigar Duplicatas** ou EANs inválidos
4. **Priorizar Correções** por valor ou frequência

---

**Última atualização:** 13/11/2025
