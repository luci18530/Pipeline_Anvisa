# Backlog de oportunidades de IA no pipeline NFe

Este documento lista pontos do pipeline onde IA/ML pode melhorar cobertura, confianca e calculo de sobrepreco, alem do modelo local de extracao de atributos da Etapa 14.

## Prioridade 1 - Conversao unidade/caixa antes do sobrepreco

Problema: a Etapa 18 calcula `RAZAO_VALOR_TETO = valor_unitario / PRECO_MAXIMO_REFINADO`, enquanto a padronizacao de unidades so ocorre na Etapa 21. Se uma nota compra por comprimido, ampola, frasco ou unidade avulsa, mas o teto CMED esta por apresentacao/caixa, o sobrepreco pode ficar artificialmente baixo ou alto.

Proposta:
- Criar uma Etapa 17.5 ou mover parte da Etapa 21 para antes da Etapa 18.
- Classificar cada linha como `CAIXA`, `UNIDADE_AVULSA`, `FRACAO_DE_CAIXA`, `MISTO` ou `INDETERMINADO`.
- Extrair `fator_unidades_por_caixa` a partir de `APRESENTACAO`, `QUANTIDADE UNIDADES`, `IA_QUANTIDADE UNIDADES`, descricao e unidade original.
- Calcular colunas novas: `quantidade_caixa_equivalente`, `valor_unitario_caixa_equivalente`, `teto_caixa_equivalente`, `confianca_conversao_unidade`.
- Usar `valor_unitario_caixa_equivalente` na Etapa 18 quando a confianca for alta; manter o valor original e marcar baixa confianca quando nao for possivel converter.

Modelo sugerido:
- Inicio com regras auditaveis e dataset rotulado por amostragem.
- Depois, classificador supervisionado leve: TF-IDF/LightGBM ou CatBoost usando `descricao_produto`, `unidade`, `quantidade`, `valor_produtos`, `valor_unitario`, `APRESENTACAO`, `QUANTIDADE UNIDADES`, `IA_TIPO DA UNIDADE`.
- Saida probabilistica, nunca binaria cega.

Validacao:
- Comparar distribuicao de `RAZAO_VALOR_TETO` antes/depois.
- Auditar amostras em classes extremas.
- Medir quantas linhas mudaram de classe de sobrepreco por causa da conversao.

Status implementado em 2026-06-01:
- Criada a Etapa 17.5 em `pipelines/nfe/src/nfe_etapa17_5_conversao_unidade_caixa.py`, com wrapper em `pipelines/nfe/scripts/processar_etapa17_5_conversao_unidade_caixa.py`.
- A Etapa 18 agora consome a saida `df_etapa17_5_unidade_caixa.zip` quando ela existe e, no pipeline completo, executa a conversao logo antes do calculo de sobrepreco.
- A comparacao economica passou a usar `VALOR_UNITARIO_ANALISE`: recebe `valor_unitario_caixa_equivalente` apenas quando `usar_valor_unitario_caixa_equivalente=True`; caso contrario preserva `valor_unitario`.
- A regra ficou conservadora: se a unidade parece avulsa, mas `valor_unitario / PRECO_MAXIMO_REFINADO >= 0.10`, a conversao fica bloqueada com `observacao_conversao_unidade=valor_unitario_ja_proximo_teto_caixa`, pois ha risco de `UN` significar apresentacao inteira.
- A Etapa 17.5 exporta resumo e amostras auditaveis: `df_etapa17_5_unidade_caixa_resumo.csv` e `df_etapa17_5_unidade_caixa_amostras.csv`.
- Testes unitarios cobrem caixa, unidade avulsa, frasco com fator 1, bloqueio por valor ja proximo ao teto e fallback por descricao.

Ponto de cuidado observado na base ANVISA:
- `QUANTIDADE ML` pode somar concentracao e volume da apresentacao em casos como `250 MG/5 ML ... 100 ML`; por isso a conversao economica usa `QUANTIDADE UNIDADES` como fator primario e nao usa ML como multiplicador de caixa.
- Amostra/distribuicao da NFe mostrou unidades muito frequentes alem do obvio: `UN`, `UND`, `CX`, `AMP`, `FR`, `CPR`, `CP`, `CX1`, `VD`, `BG`, `CMP`, `TB`, `COM`, `AM`, `FA`, `FRA`, `FRS`, `COMP`; a regra inicial ja inclui essas abreviacoes principais como caixa ou unidade/container.
- Rodagem em amostra estratificada de 2.500 linhas mostrou que a primeira trava `>= 0.60` ainda era permissiva demais: itens `UN/UND` com `CX`, `C/30`, `BL X 30` e sprays `200 DOSES` eram multiplicados indevidamente. A trava foi apertada para `valor_unitario / PRECO_MAXIMO_REFINADO >= 0.10` e fatores de `DOSE/ACIONAMENTO/JATO` foram bloqueados com `observacao_conversao_unidade=fator_representa_dose_nao_caixa`.
- A mesma amostra mostrou casos de match/apresentacao incompativeis com falso positivo anterior, por exemplo descricao injetavel caindo em apresentacao de comprimidos. A Etapa 17.5 agora bloqueia a amplificacao nesses casos com `observacao_conversao_unidade=forma_nfe_cmed_incompativel`.
- Resultado final da amostra apos ajustes: 1.952 linhas consolidadas, 1.833 com razao valida, 1.541 com conversao habilitada, 1.020 com mudanca efetiva de `valor_unitario` para `VALOR_UNITARIO_ANALISE`, e 1.009 mudaram de classe de sobrepreco. A matriz de classes da amostra foi salva em `data/processed/diagnostico_amostra_175_matriz_classes.csv`.

## Prioridade 2 - Modelo de fator de embalagem

Problema: a base ANVISA extrai `QUANTIDADE UNIDADES` por regras da apresentacao, mas casos como `CX 50 AMP`, `BL X 30`, `FR 10 ML`, `2 MG / 5 ML`, kits e apresentacoes compostas ainda podem ser ambiguos.

Proposta:
- Criar uma tabela supervisionada `descricao/apresentacao -> fator_unidades_por_caixa`.
- Aproveitar `UNIDADES_RULE` do modulo de dosagem em modo debug como feature e como explicacao.
- Guardar origem do fator: `ANVISA_REGRA`, `IA_DESCRICAO`, `MANUAL`, `INDETERMINADO`.

Modelo sugerido:
- Regressao/classificacao para prever fator discreto: 1, 2, 3, 5, 10, 12, 20, 30, 50, 60, 100, 200 etc.
- Top-k candidatos com confianca, para revisao humana quando incerto.

## Prioridade 3 - Reclassificacao de unidade original da NFe

Problema: `unidade` da NFe vem muito ruidosa (`CX`, `UN`, `AMP`, `FR`, numeros, abreviacoes estranhas). A Etapa 21 hoje usa um mapa grande e heuristicas baseadas em score, mas isso entra tarde e pode confundir casos raros.

Proposta:
- Treinar classificador local para padronizar `unidade_original -> unidade_semantica`.
- Classes: `CAIXA`, `UNIDADE`, `COMPRIMIDO_CAPSULA`, `AMPOLA`, `FRASCO`, `BISNAGA`, `ENVELOPE`, `SERINGA`, `TUBO`, `KIT`, `OUTRO`.
- Usar `descricao_produto`, `unidade`, `quantidade`, `valor_unitario`, `valor_produtos`, `IA_TIPO DA UNIDADE` e dados CMED.

Validacao:
- Relatorio de matriz de confusao por classe.
- Lista de abreviacoes com baixa confianca para virar regra manual revisada.

## Prioridade 4 - Matching inteligente NFe x CMED

Problema: a Etapa 15 ja usa matching hibrido ponderado com nome, laboratorio e atributos numericos. Pode melhorar com aprendizado de ranking e calibracao de confianca.

Proposta:
- Transformar pares candidato-linha em dataset supervisionado: `match_correto = 1/0`.
- Treinar ranker ou classificador de pares com features de similaridade textual, numericas, laboratorio, EAN/registro, apresentacao e fator de embalagem.
- Substituir pesos fixos (`W_NAME`, `W_LAB`, `W_NUM`) por pesos aprendidos.
- Gerar `match_confidence`, `match_reason`, `top_2_margin` e fila de revisao para margem pequena.

Validacao:
- Precisao@1, recall de matching, taxa de falsos positivos e impacto no sobrepreco.
- Comparar contra Etapas 7, 13 e 15 atuais.

## Prioridade 5 - Filtro de itens nao medicinais

Problema: Etapa 9 remove itens nao medicinais por listas de palavras/termos. Isso pode deixar passar tintas, taxas, alimentos, materiais gerais, e tambem pode remover produtos validos se a regra for ampla.

Proposta:
- Classificador `medicamento/produto_saude/nao_medicamento`.
- Treinar com amostras do `df_trabalhando`, itens removidos por regra e casos revisados.
- Manter classe intermediaria `produto_saude` para nao descartar fraldas, seringas, materiais hospitalares etc. sem decisao consciente.

Validacao:
- Amostra estratificada de removidos e mantidos.
- Medir quanto o filtro melhora a pureza das etapas 10-15.

## Prioridade 6 - Extracao de nome de produto e laboratorio

Problema: Etapas 10 e 11 dependem de dicionarios, termos de parada, abreviacoes e fuzzy manual. Isso funciona, mas exige manutencao constante.

Proposta:
- Criar modelo de NER/sequence labeling simples para identificar spans de `produto`, `laboratorio`, `dosagem`, `forma`, `embalagem`.
- Alternativa leve: classificador token-a-token com CRF ou spaCy customizado; alternativa robusta: transformer pequeno ajustado com exemplos revisados.
- Usar saida como sugestao, nao como substituto total das regras.

Validacao:
- Precisao/recall por entidade.
- Comparar `NOME_PRODUTO_LIMPO` contra base mestre antes/depois.

## Prioridade 7 - Active learning para revisar os casos que mais importam

Problema: revisar aleatoriamente nao maximiza ganho. O pipeline ja gera scores, classes de sobrepreco e nulos, mas ainda nao prioriza revisao.

Proposta:
- Criar fila de revisao com score de prioridade:
  - alto valor financeiro,
  - classe `MUITO ACIMA` ou `EXTREMAMENTE ACIMA`,
  - baixa confianca de match,
  - baixa confianca de conversao unidade/caixa,
  - divergencia entre regra e modelo,
  - item sem match mas parecido com produto CMED.
- Salvar revisoes em CSV versionado e retroalimentar modelos/regras.

Validacao:
- Medir ganho de cobertura/matching por 100 linhas revisadas.

## Prioridade 8 - Deteccao de anomalias de preco

Problema: a Etapa 18 usa faixas fixas da razao valor/teto. Isso e bom para regra de negocio, mas anomalias tambem podem aparecer por municipio, fornecedor, produto, periodo ou unidade.

Proposta:
- Modelo de anomalia por produto/apresentacao/esfera/municipio/fornecedor.
- Features: razao valor/teto, valor ajustado, quantidade, unidade semantica, esfera, mes, fornecedor, municipio.
- Saida: `anomalia_preco_score`, `anomalia_preco_motivo`.

Validacao:
- Auditar top 100 anomalias.
- Separar suspeita real de erro de unidade/matching.

## Prioridade 9 - Classificacao de esfera e entidade

Problema: Etapa 20 cruza CNPJ com base de classificacao e aplica regras manuais. Entidades novas ou nomes ruidosos podem cair como municipal por default.

Proposta:
- Classificador de entidade/esfera usando CNPJ, razao social, nome fantasia, municipio, termos como prefeitura, fundo municipal, secretaria estadual, hospital, instituto.
- Usar modelo apenas para preencher `ID_ESFERA_SUGERIDO` quando a base externa nao cobre.

Validacao:
- Nao sobrescrever base oficial sem confianca/revisao.
- Relatorio de divergencias entre regra manual, base externa e modelo.

## Prioridade 10 - Qualidade automatica e diagnostico inteligente

Problema: Etapa 23 diagnostica nulos, duplicatas e colunas ausentes. Pode virar um monitor inteligente do pipeline.

Proposta:
- Detectar drift entre execucoes: aumento de nulos, queda de match, aumento de linhas sem preco, mudanca na distribuicao de unidades.
- Criar resumo textual automatico por run: o que piorou, onde investigar, quais top descricoes quebraram.
- Gerar alertas por etapa com severidade.

Validacao:
- Comparar diagnosticos historicos.
- Definir limites esperados por coluna critica.

## Ordem recomendada de implementacao

1. Etapa 17.5 de unidade/caixa antes da Etapa 18, inicialmente por regras e auditoria.
2. Dataset rotulado para `tipo_compra` e `fator_unidades_por_caixa`.
3. Modelo local para classificar unidade original e fator de embalagem.
4. Etapa 18 usando preco/quantidade em caixa equivalente.
5. Active learning para revisar os casos de maior impacto financeiro.
6. Ranker aprendido para melhorar matching hibrido.
7. Filtro ML de nao medicamentos.
8. NER para produto/laboratorio/dosagem.
9. Anomalia de preco.
10. Diagnostico inteligente por run.

## Colunas novas sugeridas

- `unidade_original`
- `unidade_semantica`
- `tipo_compra`
- `fator_unidades_por_caixa`
- `quantidade_caixa_equivalente`
- `valor_unitario_caixa_equivalente`
- `teto_caixa_equivalente`
- `confianca_unidade_semantica`
- `confianca_fator_caixa`
- `origem_conversao_unidade`
- `match_confidence`
- `match_top2_margin`
- `anomalia_preco_score`
- `prioridade_revisao`

## Observacao importante

Para sobrepreco, a unidade/caixa nao deve ser tratada como ajuste cosmetico de BI. Ela precisa entrar antes do calculo de `RAZAO_VALOR_TETO`, porque muda o denominador economico da comparacao. A Etapa 21 atual pode continuar existindo para padronizacao final, mas a conversao economica deveria acontecer antes da Etapa 18.
