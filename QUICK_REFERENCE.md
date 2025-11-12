# 🗂️ Índice da Estrutura do Projeto

## Raiz do Projeto
- **`main.py`** - Script principal para executar o pipeline
- **`download.py`** - Script para baixar dados (alias para scripts/baixar.py)
- **`README.md`** - Documentação principal do projeto
- **`USAGE.md`** - Guia detalhado de uso
- **`QUICK_REFERENCE.md`** - Este arquivo

## `/src` - Código-Fonte
- **`config.py`** - Configurações centralizadas (caminhos, constantes)
- **`processar_dados.py`** - Orquestrador principal do pipeline

## `/src/modules` - Módulos de Processamento (14 arquivos)
1. **`limpeza_dados.py`** - Padronização inicial (GGREM, EAN)
2. **`unificacao_vigencias.py`** - Consolida períodos válidos
3. **`classificacao_terapeutica.py`** - Mapeia códigos ATC e grupos
4. **`principio_ativo.py`** - Processa princípios ativos (7 etapas)
5. **`produto.py`** - Processa descrições de produtos
6. **`apresentacao.py`** - Normaliza apresentações
7. **`tipo_produto.py`** - Categoriza tipos (comprimidos, ampolas, etc)
8. **`dosagem.py`** - Extrai doses e quantidades
9. **`laboratorio.py`** - Normaliza nomes de laboratórios
10. **`grupo_terapeutico.py`** - Mapeia grupos de uso
11. **`finalizacao.py`** - Padroniza colunas e exporta
12. **`correcoes_ortograficas.py`** - Regras de correção
13. **`dicionarios_correcao.py`** - Dicionários para princípios ativos
14. **`dicionarios_produto.py`** - Dicionários para produtos

## `/scripts` - Scripts Executáveis
- **`baixar.py`** - Download e limpeza de dados da ANVISA

## `/docs` - Documentação
- **`ESTRUTURA_PIPELINE.md`** - Arquitetura técnica detalhada
- **`CORRECOES_ORTOGRAFICAS.md`** - Regras de correção aplicadas

## `/data` - Dados do Projeto
```
data/
├── raw/                    # Arquivos Excel brutos (auto-gerado)
├── processed/              # CSVs processados e consolidados
│   ├── base_anvisa_precos_vigencias.csv   # Entrada do pipeline
│   └── anvisa_pmvg_consolidado_temp.csv   # Intermediário
└── external/               # Dados externos
    └── grupos_terapeuticos.xlsx           # Mapeamento de grupos
```

## `/output` - Saída do Pipeline (Auto-gerado)
```
output/
├── baseANVISA.csv                    # TSV para outros pipelines (15MB)
├── baseANVISA_dtypes.json            # Metadados de tipos
├── dfprodutos.csv                    # Dataset completo (5.86MB)
├── dfpro_correcao_manual.xlsx        # Para análise manual (1.88MB)
├── principios_ativos_unicos.txt      # Lista de 2.151 compostos
├── produtos_unicos.txt               # Lista de 5.973 produtos
├── df_grupos_com_principio_ativo.xlsx  # Debug: mapeamento
└── df_grupos_sem_match.xlsx          # Debug: sem correspondência
```

## Fluxo de Execução

```
main.py
  └─→ src/processar_dados.py
       ├─→ ETAPA 1: modules/limpeza_dados.py
       ├─→ ETAPA 2: modules/unificacao_vigencias.py
       ├─→ ETAPA 3: modules/classificacao_terapeutica.py
       ├─→ ETAPA 4: modules/principio_ativo.py
       ├─→ ETAPA 5: modules/produto.py
       ├─→ ETAPA 6: modules/apresentacao.py
       ├─→ ETAPA 7: modules/tipo_produto.py
       ├─→ ETAPA 8: modules/dosagem.py
       ├─→ ETAPA 9: modules/laboratorio.py
       ├─→ ETAPA 10: modules/grupo_terapeutico.py
       └─→ ETAPA 11: modules/finalizacao.py → output/
```

## Configuração Importante

**Arquivo:** `src/config.py`
- `ARQUIVO_ENTRADA` = `data/processed/base_anvisa_precos_vigencias.csv`
- `ARQUIVO_SAIDA` = `output/produtos_cmed.csv`

**Arquivo:** `scripts/baixar.py` (linhas 26-30)
- `TOGGLE_MES_ANTERIOR = 0` → Dados desde jan/2020
- `TOGGLE_MES_ANTERIOR = 1` → Apenas mês anterior ao atual

## Comandos Rápidos

```bash
# Primeira execução (obrigatório)
python download.py

# Processar dados
python main.py

# Apenas listar arquivos gerados
ls output/

# Limpar dados processados
rm -r data/processed/*.csv
```

## Checklist de Execução

- [ ] `pip install -r requirements.txt`
- [ ] `python download.py` (gera `data/processed/base_anvisa_precos_vigencias.csv`)
- [ ] `python main.py` (gera 8 arquivos em `output/`)
- [ ] Arquivos foram criados em `output/`

---

**Última atualização:** Nov 12, 2025  
**Versão:** 2.0 (Reorganizado)
