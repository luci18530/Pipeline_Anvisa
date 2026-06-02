# Pipeline ANVISA + NFe

Projeto pessoal de engenharia de dados para baixar a base CMED/ANVISA,
processar notas fiscais de medicamentos, cruzar produtos/precos e gerar saidas
para analise de sobrepreco no QlikView.

## Comece por aqui

Execute sempre a partir da raiz do projeto.

```powershell
python scripts/run_anvisa_completo.py
python scripts/run_nfe_pipeline_completo.py
```

Opcionalmente, abra o painel grafico:

```powershell
python apps/painel_mestre.py
```

## Mapa rapido

| Caminho | Para que serve |
| --- | --- |
| `scripts/` | Comandos operacionais para rodar ANVISA, NFe e rotinas auxiliares da raiz. |
| `apps/` | Aplicacoes interativas, hoje o painel grafico do projeto. |
| `notebooks/` | Notebooks exploratorios ou historicos. Nao sao o fluxo canonico. |
| `pipelines/anvisa_base/` | Codigo do pipeline que baixa/processa a base CMED/ANVISA. |
| `pipelines/nfe/` | Codigo do pipeline NFe, etapas 01 a 23. |
| `pipelines/nfe/src/` | Implementacao real das etapas do pipeline NFe. |
| `pipelines/nfe/scripts/` | Wrappers especificos de etapas NFe. |
| `pipelines/nfe/docs/` | Backlogs tecnicos, IA/ML e documentacao de etapas. |
| `pipelines/nfe/support/` | Dicionarios, bases auxiliares e datasets supervisionados da NFe. |
| `data/` | Dados brutos, externos e intermediarios gerados durante execucao. |
| `output/anvisa/` | Saida principal da base ANVISA/CMED processada. |
| `QlikView/` | Saidas finais para BI. |
| `tests/` | Testes unitarios, smoke e manuais. |

## Comandos principais

### Base ANVISA completa

```powershell
python scripts/run_anvisa_completo.py
```

Gera principalmente:

- `output/anvisa/baseANVISA.csv`
- `output/anvisa/baseANVISA_dtypes.json`

### Reprocessar ANVISA sem baixar de novo

```powershell
python scripts/run_anvisa_reprocessar_sem_download.py
```

### Rodar apenas processamento avancado ANVISA

```powershell
python scripts/run_anvisa_apenas_processamento_avancado.py
```

### Pipeline NFe completo

```powershell
python scripts/run_nfe_pipeline_completo.py
```

Entrada esperada:

- `nfe/nfe.csv`

Saidas principais:

- `data/processed/df_etapa*.zip`
- `data/external/nfe_vencimento.csv`
- `QlikView/df_central.csv`
- `QlikView/df_entidades.csv`
- `QlikView/df_valores_ajustados.csv`
- `QlikView/nfe_vencimento.csv`

## Etapas NFe

1. carregamento e pre-processamento
2. vencimento
3. limpeza textual
4. municipio
5. base ANVISA
6. otimizacao de memoria
7. matching ANVISA
8. matching manual
9. separacao/filtragem
10. extracao de nomes
11. refinamento de nomes
12. unificacao/matching final
13. matching de apresentacao unica
14. extracao de atributos com IA/modelo local
15. matching hibrido
16. finalizacao
17. consolidacao final
17.5. conversao unidade/caixa antes do sobrepreco
18. sobrepreco
19. ajuste inflacionario
20. classificacao por esfera
21. padronizacao de unidades
22. particionamento QlikView
23. diagnostico final

## Configuracao

Arquivo principal:

- `pipeline_config.json`

Toggles mais importantes:

- `pipeline.modo_rapido`
- `pipeline.debug_mode`
- `pipeline.cleanup_processed`
- `etapa14.usar_modelo_local`
- `etapa14.usar_gemini_api`
- `anvisa.usar_mes_anterior`

## Testes

```powershell
pytest tests/unit -q
pytest tests/smoke/test_imports.py -q
```

## Observacoes operacionais

- `data/processed/` e `QlikView/` sao artefatos de execucao. Se rodar uma amostra,
  essas pastas passam a refletir a amostra.
- `notebooks/` fica como historico/exploracao; o fluxo confiavel esta nos scripts
  e nos modulos em `pipelines/`.
- Para uma visao mais detalhada da organizacao, veja
  `docs/mapa_do_projeto.md`.
