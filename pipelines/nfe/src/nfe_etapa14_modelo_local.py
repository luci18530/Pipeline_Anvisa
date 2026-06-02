# -*- coding: utf-8 -*-
"""Modelo local para extracao de atributos farmaceuticos da Etapa 14.

Este modulo transforma o cache historico gerado por IA
(`extracao_ia_medicamentos.csv`) em um modelo supervisionado simples,
auditavel e reutilizavel no pipeline NFe.
"""

from __future__ import annotations

import json
import re
import unicodedata
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import joblib
import numpy as np
import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics import f1_score
from sklearn.model_selection import train_test_split
from sklearn.neighbors import NearestNeighbors

from pipelines.nfe.src.paths import PIPELINE_ROOT, SUPPORT_DIR

COLUNAS_IA = [
    "IA_PRODUTO",
    "IA_LABORATORIO",
    "IA_TIPO DA UNIDADE",
    "IA_QUANTIDADE MG (POR UNIDADE/ML)",
    "IA_QUANTIDADE ML",
    "IA_QUANTIDADE UI",
    "IA_QUANTIDADE UNIDADES",
]

COLUNAS_CATEGORICAS = [
    "IA_PRODUTO",
    "IA_LABORATORIO",
    "IA_TIPO DA UNIDADE",
]

COLUNAS_NUMERICAS = [
    "IA_QUANTIDADE MG (POR UNIDADE/ML)",
    "IA_QUANTIDADE ML",
    "IA_QUANTIDADE UI",
    "IA_QUANTIDADE UNIDADES",
]

DEFAULT_TRAINING_CSV = SUPPORT_DIR / "extracao_ia_medicamentos.csv"
DEFAULT_MODEL_PATH = PIPELINE_ROOT / "models" / "etapa14_atributos.joblib"
DEFAULT_METRICS_PATH = PIPELINE_ROOT / "models" / "etapa14_atributos_metricas.json"
DEFAULT_MIN_CONFIDENCE = 0.55


@dataclass
class ModeloAtributosEtapa14:
    """Artefato em memoria usado para inferencia local."""

    vectorizer: TfidfVectorizer
    nearest_neighbors: NearestNeighbors
    referencia: pd.DataFrame
    min_confidence: float
    created_at: str
    training_rows: int
    model_type: str = "tfidf_char_nearest_neighbor"


def normalizar_descricao(valor: Any) -> str:
    """Normaliza descricoes para treino e inferencia."""
    if pd.isna(valor):
        return ""
    texto = str(valor).upper().strip()
    texto = unicodedata.normalize("NFKD", texto)
    texto = "".join(ch for ch in texto if not unicodedata.combining(ch))
    texto = re.sub(r"\s+", " ", texto)
    return texto.strip()


def _normalizar_label(valor: Any) -> Any:
    if pd.isna(valor):
        return pd.NA
    texto = str(valor).strip()
    if not texto or texto.lower() in {"nan", "none", "<na>"}:
        return pd.NA
    return re.sub(r"\s+", " ", texto)


def _parse_numero(valor: Any) -> float:
    if pd.isna(valor):
        return np.nan
    texto = str(valor).strip().replace(",", ".")
    if not texto or texto.lower() in {"nan", "none", "<na>"}:
        return np.nan
    match = re.search(r"-?\d+(?:\.\d+)?", texto)
    if not match:
        return np.nan
    try:
        return float(match.group(0))
    except ValueError:
        return np.nan


def _formatar_numero(valor: float) -> str:
    if float(valor).is_integer():
        return str(int(valor))
    return f"{valor:.6f}".rstrip("0").rstrip(".")


def _somar_expressao_numerica(expressao: str) -> float:
    partes = re.findall(r"\d+(?:[,.]\d+)?", expressao)
    return sum(float(parte.replace(",", ".")) for parte in partes)


def extrair_numericos_por_regras(descricao: Any) -> dict[str, Any]:
    """Extrai atributos numericos diretamente da descricao quando possivel."""
    texto = normalizar_descricao(descricao)
    resultado = {coluna: pd.NA for coluna in COLUNAS_NUMERICAS}
    if not texto:
        return resultado

    # Dosagem em MG: suporta "875+125MG", "25 MG" e conversoes simples.
    mg_matches = re.findall(r"(\d+(?:[,.]\d+)?(?:\s*\+\s*\d+(?:[,.]\d+)?)*)\s*MG\b", texto)
    if mg_matches:
        resultado["IA_QUANTIDADE MG (POR UNIDADE/ML)"] = _formatar_numero(
            sum(_somar_expressao_numerica(match) for match in mg_matches[:1])
        )
    else:
        mcg_match = re.search(r"(\d+(?:[,.]\d+)?)\s*(?:MCG|MICROGRAMAS?)\b", texto)
        if mcg_match:
            resultado["IA_QUANTIDADE MG (POR UNIDADE/ML)"] = _formatar_numero(
                float(mcg_match.group(1).replace(",", ".")) / 1000.0
            )
        else:
            g_match = re.search(r"(\d+(?:[,.]\d+)?)\s*G\b", texto)
            if g_match:
                resultado["IA_QUANTIDADE MG (POR UNIDADE/ML)"] = _formatar_numero(
                    float(g_match.group(1).replace(",", ".")) * 1000.0
                )

    ml_match = re.search(r"(\d+(?:[,.]\d+)?)\s*ML\b", texto)
    if ml_match:
        resultado["IA_QUANTIDADE ML"] = _formatar_numero(float(ml_match.group(1).replace(",", ".")))

    ui_match = re.search(r"(\d+(?:[,.]\d+)?)\s*(?:UI|U I|IU)\b", texto)
    if ui_match:
        resultado["IA_QUANTIDADE UI"] = _formatar_numero(float(ui_match.group(1).replace(",", ".")))

    padroes_unidades = [
        r"(?:C/|COM|CX|CAIXA\s+COM)\s*(\d+(?:[,.]\d+)?)\b",
        r"(\d+(?:[,.]\d+)?)\s*(?:COMPRIMIDOS?|CAPSULAS?|C[ÁA]PSULAS?|CPR|COMP|AMPOLAS?|FRASCOS?|UNIDADES?)\b",
    ]
    for padrao in padroes_unidades:
        matches = re.findall(padrao, texto)
        if matches:
            resultado["IA_QUANTIDADE UNIDADES"] = _formatar_numero(float(matches[-1].replace(",", ".")))
            break

    return resultado


def carregar_dataset_supervisionado(caminho_csv: str | Path = DEFAULT_TRAINING_CSV) -> pd.DataFrame:
    """Carrega e valida o dataset supervisionado da Etapa 14."""
    caminho = Path(caminho_csv)
    if not caminho.exists():
        raise FileNotFoundError(f"Dataset de treino nao encontrado: {caminho}")

    df = pd.read_csv(caminho, sep=",", encoding="utf-8", dtype="string", low_memory=False)
    colunas_necessarias = ["descricao_produto"] + COLUNAS_IA
    faltantes = [col for col in colunas_necessarias if col not in df.columns]
    if faltantes:
        raise ValueError(f"Colunas ausentes no dataset de treino: {faltantes}")

    df = df[colunas_necessarias].copy()
    df["descricao_produto"] = df["descricao_produto"].map(_normalizar_label)
    df = df.dropna(subset=["descricao_produto"])
    df = df[df["descricao_produto"].astype(str).str.strip() != ""]

    for coluna in COLUNAS_IA:
        df[coluna] = df[coluna].map(_normalizar_label)

    df["_descricao_norm"] = df["descricao_produto"].map(normalizar_descricao)
    df = df[df["_descricao_norm"] != ""]

    # Quando ha duplicatas, manter a linha mais completa reduz perda de atributos.
    df["_completude"] = df[COLUNAS_IA].notna().sum(axis=1)
    df = (
        df.sort_values(["_descricao_norm", "_completude"])
        .drop_duplicates(subset=["_descricao_norm"], keep="last")
        .drop(columns=["_completude"])
        .reset_index(drop=True)
    )
    return df


def _ajustar_modelo(
    df_treino: pd.DataFrame,
    min_confidence: float = DEFAULT_MIN_CONFIDENCE,
) -> ModeloAtributosEtapa14:
    vectorizer = TfidfVectorizer(
        analyzer="char_wb",
        ngram_range=(3, 5),
        min_df=1,
        max_features=120_000,
        sublinear_tf=True,
        dtype=np.float32,
    )
    matriz = vectorizer.fit_transform(df_treino["_descricao_norm"])
    nearest_neighbors = NearestNeighbors(
        n_neighbors=1,
        metric="cosine",
        algorithm="brute",
        n_jobs=-1,
    )
    nearest_neighbors.fit(matriz)

    referencia = df_treino[["descricao_produto", "_descricao_norm"] + COLUNAS_IA].reset_index(drop=True)
    return ModeloAtributosEtapa14(
        vectorizer=vectorizer,
        nearest_neighbors=nearest_neighbors,
        referencia=referencia,
        min_confidence=float(min_confidence),
        created_at=datetime.now().isoformat(timespec="seconds"),
        training_rows=len(df_treino),
    )


def prever_atributos_dataframe(
    df_descricoes: pd.DataFrame,
    model_path: str | Path = DEFAULT_MODEL_PATH,
    modelo: ModeloAtributosEtapa14 | None = None,
    min_confidence: float | None = None,
    incluir_confianca: bool = False,
) -> pd.DataFrame:
    """Prediz colunas IA para um DataFrame com `descricao_produto`."""
    if "descricao_produto" not in df_descricoes.columns:
        raise ValueError("DataFrame de inferencia precisa conter a coluna 'descricao_produto'.")

    modelo = modelo if modelo is not None else carregar_modelo(model_path)
    limiar = modelo.min_confidence if min_confidence is None else float(min_confidence)

    saida = df_descricoes[["descricao_produto"]].copy()
    if saida.empty:
        for coluna in COLUNAS_IA:
            saida[coluna] = pd.NA
        if incluir_confianca:
            saida["IA_MODELO_CONFIANCA"] = pd.Series(dtype="float64")
        return saida

    descricoes_norm = saida["descricao_produto"].map(normalizar_descricao)
    matriz = modelo.vectorizer.transform(descricoes_norm)
    distancias, indices = modelo.nearest_neighbors.kneighbors(matriz, n_neighbors=1)
    confianca = np.clip(1.0 - distancias[:, 0], 0.0, 1.0)

    predicoes = modelo.referencia.iloc[indices[:, 0]][COLUNAS_IA].reset_index(drop=True).copy()
    predicoes.loc[confianca < limiar, COLUNAS_IA] = pd.NA

    numericos_regras = pd.DataFrame(saida["descricao_produto"].map(extrair_numericos_por_regras).tolist())
    for coluna in COLUNAS_NUMERICAS:
        mask_regra = numericos_regras[coluna].notna().to_numpy()
        predicoes.loc[mask_regra, coluna] = numericos_regras.loc[mask_regra, coluna].to_numpy()

    saida = pd.concat([saida.reset_index(drop=True), predicoes], axis=1)
    if incluir_confianca:
        saida["IA_MODELO_CONFIANCA"] = confianca
    return saida


def carregar_modelo(model_path: str | Path = DEFAULT_MODEL_PATH) -> ModeloAtributosEtapa14:
    """Carrega o artefato local salvo em disco."""
    caminho = Path(model_path)
    if not caminho.exists():
        raise FileNotFoundError(f"Modelo local da Etapa 14 nao encontrado: {caminho}")
    return joblib.load(caminho)


def modelo_disponivel(model_path: str | Path = DEFAULT_MODEL_PATH) -> bool:
    return Path(model_path).exists()


def _metricas_categoricas(df_real: pd.DataFrame, df_pred: pd.DataFrame) -> dict[str, dict[str, float]]:
    metricas: dict[str, dict[str, float]] = {}
    for coluna in COLUNAS_CATEGORICAS:
        y_true = df_real[coluna].fillna("").astype(str)
        y_pred = df_pred[coluna].fillna("").astype(str)
        mask = y_true != ""
        if not mask.any():
            metricas[coluna] = {"suporte": 0, "f1_micro": 0.0, "f1_macro": 0.0}
            continue
        metricas[coluna] = {
            "suporte": int(mask.sum()),
            "f1_micro": float(f1_score(y_true[mask], y_pred[mask], average="micro", zero_division=0)),
            "f1_macro": float(f1_score(y_true[mask], y_pred[mask], average="macro", zero_division=0)),
        }
    return metricas


def _metricas_numericas(df_real: pd.DataFrame, df_pred: pd.DataFrame) -> dict[str, dict[str, float]]:
    metricas: dict[str, dict[str, float]] = {}
    for coluna in COLUNAS_NUMERICAS:
        real = df_real[coluna].map(_parse_numero)
        pred = df_pred[coluna].map(_parse_numero)
        mask_real = real.notna()
        mask_validos = mask_real & pred.notna()
        if not mask_real.any():
            metricas[coluna] = {"suporte": 0, "cobertura_predicao": 0.0, "mae": None}
            continue
        mae = float((real[mask_validos] - pred[mask_validos]).abs().mean()) if mask_validos.any() else None
        metricas[coluna] = {
            "suporte": int(mask_real.sum()),
            "cobertura_predicao": float(mask_validos.sum() / mask_real.sum()),
            "mae": mae,
        }
    return metricas


def avaliar_modelo_holdout(
    df: pd.DataFrame,
    test_size: float = 0.2,
    random_state: int = 42,
    min_confidence: float = DEFAULT_MIN_CONFIDENCE,
) -> dict[str, Any]:
    """Treina em uma particao e avalia em holdout."""
    if len(df) < 10:
        return {"avaliacao": "ignorada", "motivo": "dataset pequeno", "linhas": len(df)}

    treino, teste = train_test_split(df, test_size=test_size, random_state=random_state, shuffle=True)
    modelo = _ajustar_modelo(treino.reset_index(drop=True), min_confidence=min_confidence)
    pred = prever_atributos_dataframe(
        teste[["descricao_produto"]],
        modelo=modelo,
        min_confidence=min_confidence,
        incluir_confianca=True,
    )
    return {
        "avaliacao": "holdout",
        "linhas_treino": int(len(treino)),
        "linhas_teste": int(len(teste)),
        "min_confidence": float(min_confidence),
        "confianca_media": float(pred["IA_MODELO_CONFIANCA"].mean()) if len(pred) else 0.0,
        "cobertura_limiar": float((pred["IA_MODELO_CONFIANCA"] >= min_confidence).mean()) if len(pred) else 0.0,
        "categoricas": _metricas_categoricas(teste.reset_index(drop=True), pred),
        "numericas": _metricas_numericas(teste.reset_index(drop=True), pred),
    }


def treinar_e_salvar_modelo(
    training_csv: str | Path = DEFAULT_TRAINING_CSV,
    model_path: str | Path = DEFAULT_MODEL_PATH,
    metrics_path: str | Path = DEFAULT_METRICS_PATH,
    min_confidence: float = DEFAULT_MIN_CONFIDENCE,
    test_size: float = 0.2,
    random_state: int = 42,
) -> dict[str, Any]:
    """Treina, avalia, salva o modelo final e grava metricas em JSON."""
    df = carregar_dataset_supervisionado(training_csv)
    metricas = avaliar_modelo_holdout(
        df,
        test_size=test_size,
        random_state=random_state,
        min_confidence=min_confidence,
    )

    modelo = _ajustar_modelo(df, min_confidence=min_confidence)
    model_path = Path(model_path)
    model_path.parent.mkdir(parents=True, exist_ok=True)
    joblib.dump(modelo, model_path, compress=3)

    payload = {
        "gerado_em": datetime.now().isoformat(timespec="seconds"),
        "training_csv": str(Path(training_csv)),
        "model_path": str(model_path),
        "linhas_dataset": int(len(df)),
        "descricoes_unicas": int(df["_descricao_norm"].nunique()),
        "tipo_modelo": modelo.model_type,
        "colunas_ia": COLUNAS_IA,
        "metricas": metricas,
    }

    metrics_path = Path(metrics_path)
    metrics_path.parent.mkdir(parents=True, exist_ok=True)
    metrics_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return payload
