# -*- coding: utf-8 -*-
"""ETAPA 23: DIAGNÓSTICO FINAL DA BASE QLIKVIEW (RAM-SAFE, CHUNKED).

Objetivos:
1. Validar integridade básica dos CSVs finais em QlikView.
2. Mapear nulos, vazios e colunas críticas ausentes.
3. Identificar duplicidade por chave de negócio (streaming).
4. Gerar artefatos auditáveis para análise posterior.
"""

from __future__ import annotations

import json
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List

import pandas as pd

from pipelines.nfe.src.paths import PROJECT_ROOT


QLIKVIEW_DIR = PROJECT_ROOT / "QlikView"
CHUNK_SIZE = 150_000

ARQUIVOS_ALVO = [
    "df_central.csv",
    "df_dosagem.csv",
    "df_registro_anvisa.csv",
    "df_entidades.csv",
    "df_valores_ajustados.csv",
    "df_eans.csv",
    "nfe_vencimento.csv",
]

COLUNAS_CRITICAS_CENTRAL = [
    "id",
    "chave_codigo",
    "id_descricao",
    "codigo_ean",
    "valor_produtos_ajustado",
]

COLUNAS_NUMERICAS_RELEVANTES = [
    "valor_produtos",
    "valor_unitario",
    "valor_produtos_ajustado",
    "valor_unitario_ajustado",
    "valor_teto_unitario",
    "razao_valor_teto",
    "quantidade",
]

# Whitelist de nulidade esperada por tabela/coluna.
# O alerta só dispara se ultrapassar o limite definido.
NULIDADE_ESPERADA: Dict[str, Dict[str, Dict[str, float | str]]] = {
    "df_eans.csv": {
        "EAN_2": {
            "max_null_pct": 99.9,
            "motivo": "EAN secundário é opcional em grande parte dos registros.",
        },
        "EAN_3": {
            "max_null_pct": 99.95,
            "motivo": "EAN terciário é opcional e raramente preenchido.",
        },
    },
    "df_dosagem.csv": {
        "QUANTIDADE UI": {
            "max_null_pct": 99.9,
            "motivo": "UI só se aplica a subconjunto específico de apresentações.",
        },
        "QUANTIDADE ML": {
            "max_null_pct": 90.0,
            "motivo": "ML não se aplica a medicamentos sólidos/sem volume.",
        },
    },
}

# Colunas críticas que podem ser consideradas cobertas por tabela particionada.
COLUNAS_CRITICAS_COBERTURA = {
    "df_central.csv": {
        "valor_produtos_ajustado": {
            "arquivo_cobertura": "df_valores_ajustados.csv",
            "coluna_cobertura": "valor_produtos_ajustado",
            "chave": "id",
        }
    }
}


@dataclass
class NumericAgg:
    count_valid: int = 0
    sum_value: float = 0.0
    min_value: float | None = None
    max_value: float | None = None
    zeros: int = 0
    negatives: int = 0

    def update(self, values: pd.Series) -> None:
        vals = pd.to_numeric(values, errors="coerce").dropna()
        if vals.empty:
            return
        self.count_valid += int(vals.shape[0])
        self.sum_value += float(vals.sum())
        local_min = float(vals.min())
        local_max = float(vals.max())
        self.min_value = local_min if self.min_value is None else min(self.min_value, local_min)
        self.max_value = local_max if self.max_value is None else max(self.max_value, local_max)
        self.zeros += int((vals == 0).sum())
        self.negatives += int((vals < 0).sum())

    def to_dict(self) -> dict:
        mean_value = (self.sum_value / self.count_valid) if self.count_valid else None
        return {
            "count_valid": self.count_valid,
            "mean": mean_value,
            "min": self.min_value,
            "max": self.max_value,
            "zeros": self.zeros,
            "negatives": self.negatives,
        }


def _iter_chunks(caminho: Path) -> Iterable[pd.DataFrame]:
    return pd.read_csv(
        caminho,
        sep=";",
        low_memory=False,
        chunksize=CHUNK_SIZE,
    )


def _hash_keys(df: pd.DataFrame, key_cols: List[str]) -> pd.Series:
    base = df[key_cols].copy()
    for col in key_cols:
        base[col] = base[col].astype("string").fillna("")
    return pd.util.hash_pandas_object(base, index=False).astype("uint64")


def _escrever_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _gerar_alertas(arquivo_info: dict, colunas_rows: List[dict]) -> List[dict]:
    alertas: List[dict] = []
    arquivo = arquivo_info["arquivo"]
    total_rows = arquivo_info["linhas"]

    if not arquivo_info["existe"]:
        alertas.append(
            {
                "severidade": "ALTA",
                "arquivo": arquivo,
                "regra": "arquivo_ausente",
                "mensagem": "Arquivo esperado não encontrado na pasta QlikView.",
            }
        )
        return alertas

    if total_rows == 0:
        alertas.append(
            {
                "severidade": "ALTA",
                "arquivo": arquivo,
                "regra": "arquivo_vazio",
                "mensagem": "Arquivo existe, mas possui zero linhas.",
            }
        )

    if arquivo_info.get("duplicatas_chave", 0) > 0:
        alertas.append(
            {
                "severidade": "ALTA",
                "arquivo": arquivo,
                "regra": "duplicidade_chave",
                "mensagem": f"{arquivo_info['duplicatas_chave']:,} duplicatas detectadas por chave.",
            }
        )

    for row in colunas_rows:
        null_pct = row["null_pct"]
        empty_pct = row["empty_pct"]
        regra_nulidade = NULIDADE_ESPERADA.get(arquivo, {}).get(row["coluna"])
        if regra_nulidade and null_pct <= float(regra_nulidade["max_null_pct"]):
            continue

        if null_pct >= 90:
            alertas.append(
                {
                    "severidade": "ALTA",
                    "arquivo": arquivo,
                    "regra": "coluna_quase_toda_nula",
                    "mensagem": f"{row['coluna']}: {null_pct:.2f}% de nulos.",
                }
            )
        elif null_pct >= 50:
            alertas.append(
                {
                    "severidade": "MEDIA",
                    "arquivo": arquivo,
                    "regra": "coluna_muita_nulidade",
                    "mensagem": f"{row['coluna']}: {null_pct:.2f}% de nulos.",
                }
            )

        if empty_pct >= 50:
            alertas.append(
                {
                    "severidade": "MEDIA",
                    "arquivo": arquivo,
                    "regra": "coluna_muitos_vazios",
                    "mensagem": f"{row['coluna']}: {empty_pct:.2f}% de vazios.",
                }
            )

    return alertas


def diagnosticar_arquivo(caminho: Path) -> tuple[dict, List[dict], List[dict]]:
    arquivo_info = {
        "arquivo": caminho.name,
        "existe": caminho.exists(),
        "linhas": 0,
        "colunas": 0,
        "tamanho_mb": round((caminho.stat().st_size / (1024 * 1024)), 2) if caminho.exists() else 0.0,
        "colunas_chave_usadas": [],
        "duplicatas_chave": 0,
        "colunas_criticas_ausentes": [],
        "colunas_criticas_cobertas": [],
        "colunas_presentes": [],
    }
    colunas_rows: List[dict] = []
    alertas: List[dict] = []

    if not caminho.exists():
        return arquivo_info, colunas_rows, _gerar_alertas(arquivo_info, colunas_rows)

    null_counts = defaultdict(int)
    empty_counts = defaultdict(int)
    numeric_aggs: Dict[str, NumericAgg] = {}
    cols_ordem: List[str] = []

    seen_hashes: set[int] = set()
    duplicatas_chave = 0
    key_cols: List[str] = []

    for idx, chunk in enumerate(_iter_chunks(caminho), start=1):
        if not cols_ordem:
            cols_ordem = chunk.columns.tolist()
            arquivo_info["colunas"] = len(cols_ordem)
            arquivo_info["colunas_presentes"] = cols_ordem
            if caminho.name == "df_central.csv":
                arquivo_info["colunas_criticas_ausentes"] = [
                    c for c in COLUNAS_CRITICAS_CENTRAL if c not in cols_ordem
                ]
            if {"chave_codigo", "id_descricao"}.issubset(cols_ordem):
                key_cols = ["chave_codigo", "id_descricao"]
            elif "id" in cols_ordem:
                key_cols = ["id"]
            arquivo_info["colunas_chave_usadas"] = key_cols

            for col in COLUNAS_NUMERICAS_RELEVANTES:
                if col in cols_ordem:
                    numeric_aggs[col] = NumericAgg()

        arquivo_info["linhas"] += len(chunk)

        ns = chunk.isna().sum()
        for col, val in ns.items():
            null_counts[col] += int(val)

        obj_cols = chunk.select_dtypes(include=["object", "string"]).columns.tolist()
        for col in obj_cols:
            empty_counts[col] += int(chunk[col].astype("string").str.strip().eq("").sum())

        for col, agg in numeric_aggs.items():
            agg.update(chunk[col])

        if key_cols:
            hashes = _hash_keys(chunk, key_cols).tolist()
            for h in hashes:
                h_int = int(h)
                if h_int in seen_hashes:
                    duplicatas_chave += 1
                else:
                    seen_hashes.add(h_int)

        if idx % 10 == 0:
            print(f"[INFO] {caminho.name}: processados ~{arquivo_info['linhas']:,} registros")

    arquivo_info["duplicatas_chave"] = duplicatas_chave
    arquivo_info["numeric_stats"] = {col: agg.to_dict() for col, agg in numeric_aggs.items()}

    total_rows = max(arquivo_info["linhas"], 1)
    for col in cols_ordem:
        n_null = null_counts.get(col, 0)
        n_empty = empty_counts.get(col, 0)
        colunas_rows.append(
            {
                "arquivo": caminho.name,
                "coluna": col,
                "null_count": n_null,
                "null_pct": round((n_null / total_rows) * 100, 4),
                "empty_count": n_empty,
                "empty_pct": round((n_empty / total_rows) * 100, 4),
                "nulidade_esperada": bool(NULIDADE_ESPERADA.get(caminho.name, {}).get(col)),
                "limite_nulidade_esperada_pct": (
                    NULIDADE_ESPERADA.get(caminho.name, {}).get(col, {}).get("max_null_pct", "")
                ),
                "motivo_nulidade_esperada": (
                    NULIDADE_ESPERADA.get(caminho.name, {}).get(col, {}).get("motivo", "")
                ),
            }
        )

    alertas.extend(_gerar_alertas(arquivo_info, colunas_rows))
    return arquivo_info, colunas_rows, alertas


def _aplicar_cobertura_colunas_criticas(arquivos_info: List[dict], alertas_rows: List[dict]) -> None:
    """Reclassifica ausência de coluna crítica quando houver cobertura por tabela particionada."""
    info_por_arquivo = {info["arquivo"]: info for info in arquivos_info}

    for arquivo, regras_cols in COLUNAS_CRITICAS_COBERTURA.items():
        info_base = info_por_arquivo.get(arquivo)
        if not info_base or not info_base.get("existe"):
            continue

        ausentes = list(info_base.get("colunas_criticas_ausentes", []))
        if not ausentes:
            continue

        resolvidas: List[str] = []
        for col in ausentes:
            regra = regras_cols.get(col)
            if not regra:
                continue
            info_cob = info_por_arquivo.get(str(regra["arquivo_cobertura"]))
            if not info_cob or not info_cob.get("existe"):
                continue

            colunas_base = set(info_base.get("colunas_presentes", []))
            colunas_cob = set(info_cob.get("colunas_presentes", []))
            coluna_cob = str(regra["coluna_cobertura"])
            chave = str(regra["chave"])
            if coluna_cob in colunas_cob and chave in colunas_cob and chave in colunas_base:
                resolvidas.append(col)
                info_base.setdefault("colunas_criticas_cobertas", []).append(col)
                alertas_rows.append(
                    {
                        "severidade": "BAIXA",
                        "arquivo": arquivo,
                        "regra": "coluna_critica_coberta_por_particionamento",
                        "mensagem": (
                            f"{col} ausente em {arquivo}, mas coberta por "
                            f"{regra['arquivo_cobertura']} via chave {chave}."
                        ),
                    }
                )

        info_base["colunas_criticas_ausentes"] = [c for c in ausentes if c not in resolvidas]

    for info in arquivos_info:
        if info.get("colunas_criticas_ausentes"):
            alertas_rows.append(
                {
                    "severidade": "ALTA",
                    "arquivo": info["arquivo"],
                    "regra": "colunas_criticas_ausentes",
                    "mensagem": "Ausentes: " + ", ".join(info["colunas_criticas_ausentes"]),
                }
            )


def executar_diagnostico_final() -> bool:
    print("\n" + "=" * 80)
    print("ETAPA 23: DIAGNÓSTICO FINAL DA BASE QLIKVIEW")
    print("=" * 80)

    QLIKVIEW_DIR.mkdir(parents=True, exist_ok=True)
    resumo_path = QLIKVIEW_DIR / "etapa23_diagnostico_resumo.json"
    colunas_path = QLIKVIEW_DIR / "etapa23_diagnostico_colunas.csv"
    alertas_path = QLIKVIEW_DIR / "etapa23_diagnostico_alertas.csv"
    log_path = QLIKVIEW_DIR / "etapa23_diagnostico_log.txt"

    arquivos_info: List[dict] = []
    colunas_rows: List[dict] = []
    alertas_rows: List[dict] = []

    for nome in ARQUIVOS_ALVO:
        caminho = QLIKVIEW_DIR / nome
        print(f"[INFO] Diagnosticando {nome}...")
        info, cols, alerts = diagnosticar_arquivo(caminho)
        arquivos_info.append(info)
        colunas_rows.extend(cols)
        alertas_rows.extend(alerts)

    _aplicar_cobertura_colunas_criticas(arquivos_info, alertas_rows)

    resumo = {
        "gerado_em": datetime.now().isoformat(),
        "arquivos_total": len(ARQUIVOS_ALVO),
        "arquivos_encontrados": sum(1 for a in arquivos_info if a["existe"]),
        "alertas_total": len(alertas_rows),
        "alertas_alta": sum(1 for a in alertas_rows if a["severidade"] == "ALTA"),
        "alertas_media": sum(1 for a in alertas_rows if a["severidade"] == "MEDIA"),
        "alertas_baixa": sum(1 for a in alertas_rows if a["severidade"] == "BAIXA"),
        "arquivos": arquivos_info,
    }

    _escrever_json(resumo_path, resumo)
    pd.DataFrame(colunas_rows).to_csv(colunas_path, sep=";", index=False, encoding="utf-8")
    pd.DataFrame(alertas_rows).to_csv(alertas_path, sep=";", index=False, encoding="utf-8")

    linhas_log = []
    linhas_log.append("ETAPA 23 - DIAGNOSTICO FINAL QLIKVIEW")
    linhas_log.append(f"Gerado em: {resumo['gerado_em']}")
    linhas_log.append("")
    linhas_log.append(
        f"Arquivos encontrados: {resumo['arquivos_encontrados']}/{resumo['arquivos_total']} | "
        f"Alertas: {resumo['alertas_total']} (ALTA={resumo['alertas_alta']}, "
        f"MEDIA={resumo['alertas_media']}, BAIXA={resumo['alertas_baixa']})"
    )
    linhas_log.append("")

    for info in arquivos_info:
        linhas_log.append(
            f"- {info['arquivo']}: existe={info['existe']}, linhas={info['linhas']:,}, "
            f"colunas={info['colunas']}, tamanho_mb={info['tamanho_mb']}"
        )
        if info["colunas_criticas_ausentes"]:
            linhas_log.append(f"  colunas_criticas_ausentes={','.join(info['colunas_criticas_ausentes'])}")
        if info["duplicatas_chave"] > 0:
            linhas_log.append(f"  duplicatas_chave={info['duplicatas_chave']:,}")
    linhas_log.append("")

    if alertas_rows:
        linhas_log.append("TOP ALERTAS (ate 30):")
        for alerta in alertas_rows[:30]:
            linhas_log.append(f"  [{alerta['severidade']}] {alerta['arquivo']} | {alerta['regra']} | {alerta['mensagem']}")
    else:
        linhas_log.append("Nenhum alerta encontrado.")

    log_path.write_text("\n".join(linhas_log), encoding="utf-8")

    print("\n[OK] Diagnóstico exportado:")
    print(f"  - {resumo_path.relative_to(PROJECT_ROOT)}")
    print(f"  - {colunas_path.relative_to(PROJECT_ROOT)}")
    print(f"  - {alertas_path.relative_to(PROJECT_ROOT)}")
    print(f"  - {log_path.relative_to(PROJECT_ROOT)}")
    print("[SUCESSO] Etapa 23 concluída.")
    return True


if __name__ == "__main__":
    raise SystemExit(0 if executar_diagnostico_final() else 1)
