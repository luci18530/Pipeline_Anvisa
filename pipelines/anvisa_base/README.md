# Pipeline ANVISA: organização

Este diretório foi reorganizado para facilitar navegação e manutenção.

## Estrutura

- `workflows/`: implementação canônica das etapas do pipeline.
- `scripts/`: wrappers de compatibilidade (mantém comandos/caminhos antigos).
- `legacy/`: scripts antigos preservados por dependências externas.
- `src/`: módulos de transformação e utilitários da base ANVISA usada no matching.
- `tools/`: utilitários operacionais.
- `config_anvisa.py`: configuração central das etapas.
- `main.py`: entrypoint principal (pipeline completo).
- `download.py`: entrypoint do Pipeline 1.0.

## Fluxo recomendado

1. `python 1_download_anvisa.py`
2. `python 2_processar_base_anvisa.py`
3. `python 2b_processar_dados_anvisa.py`

## Entrypoints internos

- Pipeline completo: `python -m pipelines.anvisa_base.main`
- Só download/consolidação: `python -m pipelines.anvisa_base.download`

## Compatibilidade

Arquivos em `pipelines/anvisa_base/scripts/*.py` continuam existindo como wrappers para evitar quebra em automações antigas.

