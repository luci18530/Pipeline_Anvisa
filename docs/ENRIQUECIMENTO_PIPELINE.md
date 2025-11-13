# Etapa 4: Enriquecimento com Dados de Município

## Visão Geral

A Etapa 4 enriquece os dados de NFe com informações de município baseadas no código IBGE (`codigo_municipio_destinatario`).

## Entrada

- Arquivo CSV: `data/processed/nfe_limpo_*.csv`
  - Contém descrições já limpas e padronizadas
  - Coluna-chave: `codigo_municipio_destinatario`

- Arquivo de Suporte: `support/codigomunicipio.xlsx`
  - Lookup table com códigos IBGE e nomes de municípios
  - Deve estar no repositório (não é baixado)

## Processamento

### 1. Verificação de Arquivo
```
[VALIDANDO] Arquivo de códigos de município...
[OK] Arquivo encontrado!
```

### 2. Carregamento de Dados
```
[INFO] Carregando dados de NFe...
[OK] 46,389 registros carregados

[INFO] Carregando códigos de município de: support/codigomunicipio.xlsx
[OK] X,XXX códigos de município carregados
```

### 3. Conversão de Tipo
```python
df['codigo_municipio_destinatario'] = pd.to_numeric(
    df['codigo_municipio_destinatario'],
    errors='coerce'
).astype('Int64')
```

### 4. Merge com Lookup Table
```python
df = df.merge(
    df_codigos,
    left_on='codigo_municipio_destinatario',
    right_on='codigo_municipio_destinatario',
    how='left'
)
```

### 5. Normalização
- Remover espaços vazios
- Converter para MAIÚSCULAS
- Validar preenchimento (meta: ≥95%)

### 6. Reorganização de Colunas
- Coluna `municipio` é posicionada logo após `codigo_municipio_destinatario`
- Facilita visualização e análise

## Saída

- Arquivo CSV: `data/processed/nfe_enriquecido_YYYYMMDD_HHMMSS.csv`
  - Contém todas as colunas anteriores
  - **Nova coluna**: `municipio` (nome do município em MAIÚSCULAS)

## Validações

A validação verifica:

1. ✅ Colunas críticas presentes
   - `municipio`, `codigo_municipio_destinatario`, `descricao_produto`

2. ✅ Taxa de preenchimento
   - Meta: ≥95% de municípios preenchidos
   - Alerta se <80%

3. ✅ Formatação
   - Municípios em MAIÚSCULAS
   - Códigos com 5 dígitos

4. ✅ Unicidade
   - Conta municipios únicos
   - Sem duplicação desnecessária

5. ✅ Integridade
   - Total de registros preservado
   - Sem registros duplicados

## Estatísticas

Exemplo de saída:

```
Total de registros: 46,389
Municípios únicos: 1,547

Top 5 Municípios:
    892 ( 1.92%) - SAO PAULO
    456 ( 0.98%) - CAMPINAS
    389 ( 0.84%) - GUARULHOS
    287 ( 0.62%) - BELO HORIZONTE
    234 ( 0.50%) - CURITIBA
```

## Execução

### Standalone
```bash
python scripts/processar_enriquecimento.py
```

### Via Pipeline
```bash
python main_nfe.py
```

## Troubleshooting

**Erro: "Arquivo de códigos de município não encontrado"**
```
[ERRO] Arquivo de códigos de município não encontrado!
[INFO] Coloque o arquivo em: support/codigomunicipio.xlsx
```
→ Verifique se o arquivo está na pasta `support/`

**Aviso: "Menos de 95% dos registros foram enriquecidos"**
```
[AVISO] Menos de 95% dos registros foram enriquecidos!
[AVISO] 2,340 registros sem município
```
→ Verifique se os códigos no arquivo de suporte correspondem aos dados
→ Verifique se há valores nulos/inválidos nos códigos

## Arquivos Relacionados

- `src/nfe_enriquecimento.py` - Módulo principal
- `scripts/processar_enriquecimento.py` - Script de execução
- `scripts/validar_enriquecimento.py` - Script de validação
- `support/codigomunicipio.xlsx` - Lookup table (deve ser fornecida)
- `support/SETUP.md` - Guia de setup do arquivo

## Próximas Etapas

Após o enriquecimento, os dados podem ser utilizados para:
- Análise geográfica de distribuição de medicamentos
- Identificação de padrões por região
- Matching com bases de dados da ANVISA (Etapa 5)
- Classificação terapêutica (Etapa 6)
