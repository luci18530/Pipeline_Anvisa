"""
Guardas defensivas contra corrupcao de encoding em fontes do pipeline NFe.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import re


# Mira apenas palavras MAIUSCULAS com '?' no meio (ex.: PRINC[?]PIO, AN[?]LISE),
# evitando falso positivo em URLs (ex.: export?format=...).
WORD_WITH_QUESTION_RE = re.compile(r"\b[A-ZÀ-Ý]{2,}\?[A-ZÀ-Ý]{2,}\b")
UTF8_MOJIBAKE_PAIR_RE = re.compile(r"(?:\u00C2[\u0080-\u00BF]|\u00C3[\u0080-\u00BF]|\u00E2[\u0080-\u00BF])")
C1_CONTROL_RE = re.compile(r"[\u0080-\u009F]")


@dataclass(frozen=True)
class EncodingIssue:
    path: Path
    line: int
    rule: str
    snippet: str


def _iter_py_files(root: Path):
    for path in root.rglob("*.py"):
        if "__pycache__" not in path.parts:
            yield path


def scan_python_encoding_issues(root: Path) -> list[EncodingIssue]:
    issues: list[EncodingIssue] = []
    for path in _iter_py_files(root):
        try:
            text = path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            issues.append(
                EncodingIssue(
                    path=path,
                    line=1,
                    rule="decode_error",
                    snippet="arquivo nao pode ser lido como UTF-8",
                )
            )
            continue

        for lineno, line in enumerate(text.splitlines(), start=1):
            if "\uFFFD" in line:
                issues.append(EncodingIssue(path=path, line=lineno, rule="replacement_char", snippet=line.strip()))
            if WORD_WITH_QUESTION_RE.search(line):
                issues.append(EncodingIssue(path=path, line=lineno, rule="word_with_question", snippet=line.strip()))
            if UTF8_MOJIBAKE_PAIR_RE.search(line):
                issues.append(EncodingIssue(path=path, line=lineno, rule="mojibake_pair", snippet=line.strip()))
            if C1_CONTROL_RE.search(line):
                issues.append(EncodingIssue(path=path, line=lineno, rule="c1_control_char", snippet=line.strip()))

    return issues


def assert_no_encoding_corruption(root: Path, max_items: int = 60) -> None:
    issues = scan_python_encoding_issues(root)
    if not issues:
        return

    header = (
        "[ERRO] Corrupcao de encoding detectada em fontes Python do NFe. "
        "Corrija antes de executar o pipeline."
    )
    lines = [header, f"Total de ocorrencias: {len(issues)}"]
    for issue in issues[:max_items]:
        rel_path = issue.path.as_posix()
        lines.append(f"- {rel_path}:{issue.line} [{issue.rule}] {issue.snippet}")
    if len(issues) > max_items:
        lines.append(f"- ... e mais {len(issues) - max_items} ocorrencias")
    raise RuntimeError("\n".join(lines))
