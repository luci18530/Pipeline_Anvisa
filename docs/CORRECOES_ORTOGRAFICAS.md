# Módulo de Correções Ortográficas

## Descrição
Este documento descreve as correções ortográficas e químicas adicionadas aos módulos `produto.py` e `principio_ativo.py`.

## Arquivo Criado
- **`correcoes_ortograficas.py`**: Módulo compartilhado com funções de correção

## Funcionalidades Implementadas

### 1. Correções Ortográficas e Químicas
- **GETAMICINA** → GENTAMICINA
- **AZITRIMICINA** → AZITROMICINA
- **SIDENAFILA** → SILDENAFILA
- **PROPANOLOL** → PROPRANOLOL
- **DIPROPRIONATO** → DIPROPIONATO
- Remoção de "+ TRI HIDRATADA", "+ DI HIDRATADA"
- Normalização de "SOLUÇÃO RINGER COM LACTATO"
- Correção de "AC ACETILSALIC" → "ÁCIDO ACETILSALICÍLICO"

### 2. Adição de '+' em Combinações
Casos que DEVEM ter '+':
- CALCIO COLECALCIFEROL → CALCIO + COLECALCIFEROL
- BETAMETASONA SULFATO → BETAMETASONA + SULFATO
- AMOXICILINA CLAVULANATO → AMOXICILINA + CLAVULANATO
- ISONIAZIDA RIFAMPICINA → ISONIAZIDA + RIFAMPICINA
- TENOFOVIR DESOPROXILA LAMIVUDINA → TENOFOVIR DESOPROXILA + LAMIVUDINA

### 3. Remoção de '+' Desnecessário
- ALGESTONA + ACETOFENIDA → ALGESTONA ACETOFENIDA
- CANDESARTANA + CILEXETILA → CANDESARTANA CILEXETILA

### 4. Correção de Nomenclatura
Remoção de "SÓDICO" e conversão para "DE SÓDIO":
- TAZOBACTAM SODICO → TAZOBACTAM
- RABEPRAZOL SODICO → RABEPRAZOL
- ACICLOVIR SODICO → ACICLOVIR
- NAPROXENO SODICO → NAPROXENO
- PANTOPRAZOL SODICO → PANTOPRAZOL
- MONTELUCASTE SODICO → MONTELUCASTE DE SODIO

### 5. Padronização Alfabética de Combinações
- Remove duplicatas em combinações
- Ordena componentes alfabeticamente
- Exemplo: "PARACETAMOL + CARISOPRODOL + DICLOFENACO SODICO + CAFEINA"
  → "CAFEINA + CARISOPRODOL + DICLOFENACO SODICO + PARACETAMOL"

**TRAVAS**: Não altera linhas contendo:
- FURP
- LQFEX
- ISOFARMA
- FRACAO

### 6. Normalização Final
- Normaliza espaços ao redor de '+' (` + `)
- Remove '++' duplicado
- Remove espaços duplos
- Remove '+ G' ou '+G' no final

## Integração nos Módulos

### produto.py
```python
from correcoes_ortograficas import processar_correcoes_ortograficas

def processar_produto(df):
    # ... etapas anteriores ...
    
    # NOVA ETAPA: Correções ortográficas
    df_processado = processar_correcoes_ortograficas(
        df_processado, 
        colunas=['PRODUTO']
    )
    
    return df_processado
```

### principio_ativo.py
```python
from correcoes_ortograficas import processar_correcoes_ortograficas

def processar_principio_ativo(df, executar_fuzzy_matching=False):
    # ... etapas anteriores ...
    
    # NOVA ETAPA: Correções ortográficas
    df_processado = processar_correcoes_ortograficas(
        df_processado, 
        colunas=['PRINCÍPIO ATIVO']
    )
    
    return df_processado
```

## Funções Disponíveis

### `processar_correcoes_ortograficas(df, colunas=['PRODUTO', 'PRINCÍPIO ATIVO'])`
Função principal que aplica todas as correções.

### `corrigir_descricoes(df, col='PRODUTO')`
Aplica correções ortográficas e químicas específicas.

### `padronizar_combinacoes(texto)`
Ordena componentes de combinações alfabeticamente.

### `aplicar_padronizacao_combinacoes(df, coluna)`
Aplica padronização alfabética em uma coluna.

### `remover_procedimento_medico_tabelado(df, coluna='PRODUTO')`
Remove registros com "PROCEDIMENTO MÉDICO TABELADO PELO GOVERNO".

## Estatísticas
- **47 regras de correção** no dicionário CORRECOES_COMUNS
- **3 padrões** para adicionar '+'
- **1 padrão** para remover '+' desnecessário
- **4 palavras** de bloqueio para ordenação

## Validação
✅ Sintaxe validada com `py_compile`
✅ Import testado com sucesso
✅ Integrado em `produto.py` e `principio_ativo.py`

## Próximos Passos
- Testar com dados reais
- Monitorar casos não cobertos
- Adicionar novas regras conforme necessário
