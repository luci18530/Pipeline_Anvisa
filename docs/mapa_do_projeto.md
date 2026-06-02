# Mapa do Projeto

Este arquivo existe para reduzir a confusao de navegacao. A regra mental do
repositorio agora e:

- `scripts/`: comandos para executar coisas.
- `apps/`: interfaces interativas.
- `notebooks/`: exploracao/historico.
- `pipelines/`: codigo de producao dos pipelines.
- `data/`, `output/`, `QlikView/`: dados e artefatos gerados.

## Comandos da raiz

| Arquivo | Uso |
| --- | --- |
| `scripts/run_anvisa_completo.py` | Baixa/processa a base ANVISA inteira. |
| `scripts/run_anvisa_reprocessar_sem_download.py` | Reprocessa ANVISA usando arquivos ja baixados. |
| `scripts/run_anvisa_apenas_processamento_avancado.py` | Roda somente a etapa avancada que gera/refina `baseANVISA`. |
| `scripts/run_anvisa_apenas_download.py` | Executa apenas download/consolidacao bruta ANVISA. |
| `scripts/run_nfe_pipeline_completo.py` | Roda o pipeline NFe completo. |
| `apps/painel_mestre.py` | Painel grafico para escolher CSV e rodar os fluxos. |

## Codigo ativo

| Caminho | Conteudo |
| --- | --- |
| `pipelines/anvisa_base/main.py` | Orquestrador da base ANVISA. |
| `pipelines/anvisa_base/src/` | Tratamentos da base CMED/ANVISA. |
| `pipelines/anvisa_base/workflows/` | Fluxos internos de download/processamento. |
| `pipelines/nfe/main.py` | Orquestrador das etapas NFe. |
| `pipelines/nfe/src/nfe_etapaXX_*.py` | Implementacao das etapas NFe. |
| `pipelines/nfe/src/nfe_etapa17_5_conversao_unidade_caixa.py` | Conversao economica unidade/caixa antes do sobrepreco. |
| `pipelines/nfe/src/nfe_etapa14_modelo_local.py` | Modelo local de extracao de atributos da Etapa 14. |
| `pipelines/nfe/scripts/` | Wrappers para rodar etapas NFe isoladamente. |

## Documentacao e backlog

| Caminho | Conteudo |
| --- | --- |
| `README.md` | Guia principal e comandos canonicos. |
| `docs/mapa_do_projeto.md` | Este mapa de navegacao. |
| `pipelines/nfe/docs/backlog_ia_oportunidades_pipeline_nfe.md` | Oportunidades de IA/ML no pipeline NFe. |
| `pipelines/nfe/docs/backlog_mestrado_etapa14_ml.txt` | Ideias e escopo de mestrado/Etapa 14 ML. |
| `pipelines/nfe/docs/ETAPA_17_CONSOLIDACAO.md` | Documentacao especifica da Etapa 17. |

## Dados e artefatos

| Caminho | Observacao |
| --- | --- |
| `nfe/nfe.csv` | Entrada principal do pipeline NFe. |
| `pipelines/nfe/support/` | Dicionarios, bases auxiliares e dataset supervisionado da Etapa 14. |
| `data/raw/` | Downloads brutos. |
| `data/processed/` | Intermediarios por etapa. Pode ser limpo/regerado. |
| `data/external/` | Artefatos auxiliares usados entre etapas. |
| `output/anvisa/` | Base ANVISA processada. |
| `QlikView/` | Saidas finais para BI. |

## Arquivos que nao sao fluxo canonico

- `notebooks/`: mantidos para consulta, mas prefira scripts/modulos para rodar.
- `tests/manual/`: diagnosticos e scripts manuais.
- `pipelines/nfe/tools/`: ferramentas pontuais de investigacao.
