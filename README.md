# Pipeline ANVISA + NFe

Projeto de engenharia de dados para:

1. baixar e consolidar publicações da ANVISA (CMED),
2. construir uma base mestra de medicamentos (`baseANVISA`),
3. processar NFe e cruzar com a base ANVISA,
4. gerar tabelas finais para consumo no QlikView.

## Visão Geral da Arquitetura

O repositório é organizado em dois pipelines principais e uma camada de orquestração:

- `pipelines/anvisa_base/`: download, consolidação e engenharia da base CMED.
- `pipelines/nfe/`: pipeline de 22 etapas para limpeza, matching, enriquecimento e consolidação de NFe.
- Scripts na raiz (`1_*`, `2_*`, `2b_*`, `3_*`): wrappers de execução do fluxo recomendado.

Pastas de dados:

- `data/raw/`: downloads brutos (ANVISA e outros insumos).
- `data/processed/`: artefatos intermediários das etapas.
- `data/external/`: artefatos auxiliares de processamento (ex.: vencimento da NFe).
- `output/anvisa/`: saídas da base mestra ANVISA.
- `QlikView/`: saídas finais do pipeline NFe para BI.
- `nfe/`: entrada principal de NFe (`nfe.csv`).

## Pré-requisitos

- Python 3.10+.
- Execução a partir da raiz do repositório.
- Dependências instaladas:

```bash
pip install -r requirements.txt
```

- Para execução de ponta a ponta do NFe, os seguintes arquivos precisam existir:
  - `output/anvisa/baseANVISA.csv`
  - `output/anvisa/baseANVISA_dtypes.json`

## Fluxo Recomendado de Execução (CLI)

### 1) Fase 1 unificada da base bruta ANVISA

```bash
python 1_download_anvisa.py
```

Modos disponíveis:

```bash
# Download + consolidação bruta (padrão)
python 1_download_anvisa.py --modo download

# Reconsolidar ANVISA_LIMPO_*.csv existentes (sem re-download)
python 1_download_anvisa.py --modo reconsolidar

# Auto: reconsolida se houver ANVISA_LIMPO, senão faz download
python 1_download_anvisa.py --modo auto
```

Gera, entre outros:

- `data/raw/anvisa_ano_fiscal_*/...`
- `data/processed/ANVISA_LIMPO_*.csv`
- `data/processed/anvisa/anvisa_pmvg_consolidado_temp.csv`

### 2) Processamento e engenharia de vigências/preços

```bash
python 2_processar_base_anvisa.py
```

Entrada principal:

- `data/processed/anvisa/anvisa_pmvg_consolidado_temp.csv`

Saídas principais:

- `data/processed/anvisa/base_anvisa_precos_vigencias.csv`
- `output/anvisa/baseANVISA.csv`

### 3) Processamento avançado da base ANVISA

```bash
python 2b_processar_dados_anvisa.py
```

Refina e padroniza atributos de produto (princípio ativo, apresentação, dosagem, laboratório etc.).

Saídas principais:

- `output/anvisa/baseANVISA.csv` (CSV com separador `;`)
- `output/anvisa/baseANVISA_dtypes.json`
- `output/anvisa/dfprodutos.csv`
- `output/anvisa/dfpro_correcao_manual.xlsx`
- `output/anvisa/principios_ativos_unicos.txt`
- `output/anvisa/produtos_unicos.txt`

### 4) Pipeline completo NFe (22 etapas)

```bash
python 3_pipeline_nfe.py
```

Entrada principal:

- `nfe/nfe.csv`

Saídas principais:

- Intermediários em `data/processed/*` (CSV/ZIP por etapa).
- `data/external/nfe_vencimento.csv`.
- Tabelas finais em `QlikView/` (ex.: `df_central.csv`, `df_dosagem.csv`, `df_registro_anvisa.csv`, `df_entidades.csv`, `df_valores_ajustados.csv`, `df_eans.csv`, `nfe_vencimento.csv`).

## Reconsolidação Sem Re-download da ANVISA

Se os arquivos `ANVISA_LIMPO_*.csv` já existirem e você quiser apenas reconsolidar:

```bash
python 1_download_anvisa.py --modo reconsolidar
python 2_processar_base_anvisa.py
python 2b_processar_dados_anvisa.py
```

Compatibilidade:

- `1b_reconsolidar_anvisa_limpo.py` segue funcionando como atalho legado e redireciona para `1_download_anvisa.py --modo reconsolidar`.

## Pipeline NFe: Etapas 01 a 22 (Resumo)

1. carregamento e pré-processamento da NFe,
2. cálculo de vencimento,
3. limpeza textual,
4. enriquecimento de município,
5. garantia/disponibilização da base ANVISA,
6. otimização de memória,
7. matching ANVISA (EAN + regras de negócio),
8. matching manual,
9. separação de itens não medicinais,
10. extração de nomes,
11. refinamento,
12. unificação de matching,
13. matching de apresentação única,
14. extração via IA/cache,
15. matching híbrido ponderado,
16. finalização de matching,
17. consolidação final,
18. cálculo de sobrepreço,
19. ajuste inflacionário,
20. classificação de esfera,
21. padronização de unidades,
22. particionamento final para QlikView.

## Execução Incremental vs Carga Limpa

- A etapa 22 pode acumular histórico em arquivos existentes de `QlikView/` e aplicar deduplicação.
- Se o input `nfe/nfe.csv` for alterado, o pipeline pode forçar limpeza de intermediários em `data/processed`.
- Para uma carga limpa, revise e limpe artefatos antigos antes da execução (principalmente `data/processed/`, `data/external/` e `QlikView/`), conforme sua política operacional.

## Configuração

### Toggle central do pipeline

Arquivo: `pipeline_config.json`

```json
{
  "pipeline": {
    "debug_mode": false,
    "cleanup_processed": false,
    "modo_rapido": false
  },
  "etapa14": {
    "usar_gemini_api": false
  },
  "anvisa": {
    "usar_mes_anterior": false
  }
}
```

Leitura desses toggles é feita por `pipeline_config.py`.

Observação:

- `debug_mode`, `cleanup_processed`, `modo_rapido` e `etapa14.usar_gemini_api` afetam o pipeline NFe.
- O toggle `anvisa.usar_mes_anterior` no `pipeline_config.json` alimenta `USAR_MES_ANTERIOR` em `pipelines/anvisa_base/config_anvisa.py`.
- O período também pode ser ajustado em `pipelines/anvisa_base/config_anvisa.py` (`ANO_INICIO`, `MES_INICIO`).

### Configuração da coleta ANVISA

Arquivo: `pipelines/anvisa_base/config_anvisa.py`

Parâmetros-chave:

- período de coleta,
- paralelismo de download/limpeza,
- caminhos de arquivos intermediários e finais do pipeline ANVISA.

## Dependências Externas e Integrações

Dependendo das etapas habilitadas, o projeto pode depender de:

- Google Sheets (matching manual em etapas do NFe),
- Google Drive (`gdown`) para baixar insumos ausentes,
- Gemini API na etapa 14 (`GOOGLE_API_KEY` + `google-generativeai`) quando `etapa14.usar_gemini_api=true`,
- serviços externos de dados públicos (ex.: ANVISA/IBGE).

## Execução via Painel Gráfico

Também é possível executar pelo painel:

```bash
python painel_mestre.py
```

O painel executa os mesmos wrappers da raiz e, no fluxo NFe, copia o CSV selecionado para `nfe/nfe.csv`.

## Troubleshooting

### Base ANVISA não encontrada para o NFe

- Verifique se existem `output/anvisa/baseANVISA.csv` e `output/anvisa/baseANVISA_dtypes.json`.
- Execute novamente: `python 2_processar_base_anvisa.py` e `python 2b_processar_dados_anvisa.py`.

### Falha em etapas com base manual/Google Sheets

- Verifique conectividade e permissões de acesso aos recursos externos.
- Se necessário, mantenha cópias locais de apoio em `support/`.

### Falha por insumo ausente em etapas 19/20

- Instale `gdown` (já listado em `requirements.txt`) ou adicione manualmente os arquivos esperados em `support/`.

### Problemas de parsing no `nfe/nfe.csv`

- Valide separador, encoding e presença de colunas esperadas.
- Gere um recorte pequeno para depuração antes de rodar carga completa.

### Saída acumulou dados antigos no QlikView

- Revise a estratégia incremental da etapa 22 e limpe arquivos anteriores quando desejar reprocessamento completo.

## Testes e Qualidade

- Scripts de diagnóstico manual ficam em `tests/manual/`.
- Suíte automatizada inicial:
  - `tests/unit/`
  - `tests/smoke/`
- Configuração de teste: `pytest.ini`.
- Dependências de desenvolvimento: `requirements-dev.txt`.
- CI para unit/smoke: `.github/workflows/ci.yml`.

Execução local:

```bash
pip install -r requirements-dev.txt
pytest -m "unit or smoke" -q
```

## Documentação e Scripts Legados

- Fluxo canônico atual: `1_download_anvisa.py` → `2_processar_base_anvisa.py` → `2b_processar_dados_anvisa.py` → `3_pipeline_nfe.py`.
- Scripts `_LEGADO_*` e alguns READMEs secundários podem refletir fluxos antigos.
- `pipelines/nfe/README.md` está desatualizado em relação ao pipeline atual e deve ser tratado como referência histórica até atualização.

## Estrutura de Pastas (Resumo)

```text
Pipeline_Anvisa/
|-- 1_download_anvisa.py
|-- 1b_reconsolidar_anvisa_limpo.py (compatibilidade)
|-- 2_processar_base_anvisa.py
|-- 2b_processar_dados_anvisa.py
|-- 3_pipeline_nfe.py
|-- pipeline_config.py
|-- pipeline_config.json
|-- painel_mestre.py
|-- pipelines/
|   |-- anvisa_base/
|   `-- nfe/
|-- data/
|   |-- raw/
|   |-- processed/
|   `-- external/
|-- output/
|   `-- anvisa/
|-- QlikView/
`-- nfe/
```

## Ordem Rápida Para Novos Leitores

```bash
pip install -r requirements.txt
python 1_download_anvisa.py
python 2_processar_base_anvisa.py
python 2b_processar_dados_anvisa.py
# colocar arquivo em nfe/nfe.csv
python 3_pipeline_nfe.py
```
