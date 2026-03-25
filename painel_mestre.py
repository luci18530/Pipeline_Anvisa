#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
PAINEL MESTRE - PIPELINES ANVISA/NFE
Interface gráfica para executar pipelines sem linha de comando.
"""
from __future__ import annotations

import os
import sys
import threading
import queue
import subprocess
import shutil
from pathlib import Path
import tkinter as tk
from tkinter import ttk, messagebox, filedialog

PROJECT_ROOT = Path(__file__).resolve().parent
PYTHON_EXE = sys.executable

SCRIPTS = [
    ("1) Pipeline ANVISA (Execucao Unica)", "1_download_anvisa.py"),
    ("2) Pipeline NFe Completo", "3_pipeline_nfe.py"),
]

class PainelMestre(tk.Tk):
    def __init__(self) -> None:
        super().__init__()
        self.title("Painel Mestre - Pipelines ANVISA/NFe")
        self.geometry("980x650")
        self.minsize(880, 560)
        self.configure(bg="#0f172a")

        self._queue: queue.Queue[str] = queue.Queue()
        self._process: subprocess.Popen | None = None
        self._stop_requested = False

        self._build_ui()
        self._poll_logs()

    def _build_ui(self) -> None:
        # Header
        header = tk.Frame(self, bg="#0f172a")
        header.pack(fill=tk.X, padx=20, pady=(20, 8))
        
        title = tk.Label(
            header,
            text="Painel Mestre",
            font=("Segoe UI", 20, "bold"),
            fg="#f8fafc",
            bg="#0f172a",
        )
        title.pack(anchor="w")

        subtitle = tk.Label(
            header,
            text="Execute os pipelines com um clique — sem linha de comando.",
            font=("Segoe UI", 11),
            fg="#cbd5f5",
            bg="#0f172a",
        )
        subtitle.pack(anchor="w", pady=(2, 0))

        # Main content
        content = tk.Frame(self, bg="#0f172a")
        content.pack(fill=tk.BOTH, expand=True, padx=20, pady=12)

        # Left panel (actions)
        left = tk.Frame(content, bg="#111827")
        left.pack(side=tk.LEFT, fill=tk.Y, padx=(0, 12), pady=0)

        left_title = tk.Label(
            left,
            text="Executar Pipelines",
            font=("Segoe UI", 12, "bold"),
            fg="#e2e8f0",
            bg="#111827",
        )
        left_title.pack(anchor="w", padx=14, pady=(14, 10))

        for label, script in SCRIPTS:
            btn = ttk.Button(
                left,
                text=label,
                command=lambda s=script: self._run_script(s),
                style="Primary.TButton",
            )
            btn.pack(fill=tk.X, padx=14, pady=6)

        # NFe source file picker
        nfe_frame = tk.Frame(left, bg="#111827")
        nfe_frame.pack(fill=tk.X, padx=14, pady=(8, 6))

        nfe_label = tk.Label(
            nfe_frame,
            text="Arquivo NFe (CSV)",
            font=("Segoe UI", 10, "bold"),
            fg="#e2e8f0",
            bg="#111827",
        )
        nfe_label.pack(anchor="w", pady=(0, 4))

        self.nfe_source_var = tk.StringVar(value="")
        nfe_entry = tk.Entry(
            nfe_frame,
            textvariable=self.nfe_source_var,
            bg="#0b1220",
            fg="#e2e8f0",
            insertbackground="#e2e8f0",
            relief="flat",
        )
        nfe_entry.pack(fill=tk.X, pady=(0, 6))

        ttk.Button(
            nfe_frame,
            text="Selecionar arquivo...",
            command=self._pick_nfe_source,
            style="Primary.TButton",
        ).pack(fill=tk.X)

        # Control buttons
        controls = tk.Frame(left, bg="#111827")
        controls.pack(fill=tk.X, padx=14, pady=(12, 14))

        ttk.Button(
            controls,
            text="Parar Execução",
            command=self._stop_process,
            style="Danger.TButton",
        ).pack(fill=tk.X)

        # Right panel (log)
        right = tk.Frame(content, bg="#0f172a")
        right.pack(side=tk.RIGHT, fill=tk.BOTH, expand=True)

        log_title = tk.Label(
            right,
            text="Saída / Log",
            font=("Segoe UI", 12, "bold"),
            fg="#e2e8f0",
            bg="#0f172a",
        )
        log_title.pack(anchor="w", pady=(0, 8))

        self.log_text = tk.Text(
            right,
            height=24,
            bg="#0b1220",
            fg="#e2e8f0",
            insertbackground="#e2e8f0",
            font=("Cascadia Mono", 10),
            wrap="word",
        )
        self.log_text.pack(fill=tk.BOTH, expand=True)

        # Status bar
        self.status_var = tk.StringVar(value="Pronto")
        status = tk.Label(
            self,
            textvariable=self.status_var,
            font=("Segoe UI", 10),
            fg="#94a3b8",
            bg="#0f172a",
            anchor="w",
        )
        status.pack(fill=tk.X, padx=20, pady=(4, 12))

        self._setup_styles()

    def _setup_styles(self) -> None:
        style = ttk.Style()
        try:
            style.theme_use("clam")
        except Exception:
            pass

        style.configure(
            "Primary.TButton",
            font=("Segoe UI", 10, "bold"),
            foreground="#0f172a",
            background="#38bdf8",
            padding=8,
        )
        style.map(
            "Primary.TButton",
            background=[("active", "#7dd3fc"), ("disabled", "#94a3b8")],
        )

        style.configure(
            "Danger.TButton",
            font=("Segoe UI", 10, "bold"),
            foreground="#ffffff",
            background="#ef4444",
            padding=8,
        )
        style.map(
            "Danger.TButton",
            background=[("active", "#f87171"), ("disabled", "#94a3b8")],
        )

    def _append_log(self, text: str) -> None:
        self.log_text.insert(tk.END, text)
        self.log_text.see(tk.END)

    def _run_script(self, script_rel: str | list[str]) -> None:
        if self._process is not None and self._process.poll() is None:
            messagebox.showwarning("Processo em execução", "Já existe um processo em execução.")
            return

        script_list = [script_rel] if isinstance(script_rel, str) else script_rel
        script_paths = [PROJECT_ROOT / s for s in script_list]
        missing = [str(p) for p in script_paths if not p.exists()]
        if missing:
            messagebox.showerror("Arquivo não encontrado", "Não encontrado:\n" + "\n".join(missing))
            return

        if "3_pipeline_nfe.py" in script_list:
            if not self._prepare_nfe_input():
                return

        self.log_text.delete("1.0", tk.END)
        label = ", ".join(script_list)
        self._append_log(f"==> Executando: {label}\n\n")
        self.status_var.set(f"Executando: {label}")
        self._stop_requested = False

        def _worker() -> None:
            try:
                for script_path in script_paths:
                    if self._stop_requested:
                        break
                    self._queue.put(f"\n==> Iniciando: {script_path.name}\n")
                    self._process = subprocess.Popen(
                        [PYTHON_EXE, str(script_path)],
                        cwd=str(PROJECT_ROOT),
                        stdout=subprocess.PIPE,
                        stderr=subprocess.STDOUT,
                        text=True,
                        bufsize=1,
                    )
                    assert self._process.stdout is not None
                    for line in self._process.stdout:
                        self._queue.put(line)
                        if self._stop_requested:
                            break
            except Exception as exc:
                self._queue.put(f"[ERRO] {exc}\n")
            finally:
                self._queue.put("\n==> Processo finalizado.\n")
                self.status_var.set("Pronto")

        threading.Thread(target=_worker, daemon=True).start()

    def _stop_process(self) -> None:
        if self._process is None or self._process.poll() is not None:
            messagebox.showinfo("Nenhum processo", "Nenhum processo em execução.")
            return
        self._stop_requested = True
        try:
            self._process.terminate()
            self.status_var.set("Processo interrompido")
            self._append_log("\n[INFO] Processo interrompido pelo usuário.\n")
        except Exception as exc:
            messagebox.showerror("Erro ao interromper", str(exc))

    def _poll_logs(self) -> None:
        try:
            while True:
                line = self._queue.get_nowait()
                self._append_log(line)
        except queue.Empty:
            pass
        self.after(100, self._poll_logs)

    def _pick_nfe_source(self) -> None:
        arquivo = filedialog.askopenfilename(
            title="Selecione o arquivo de notas fiscais (CSV)",
            filetypes=[("CSV", "*.csv"), ("Todos os arquivos", "*.*")],
            initialdir=str(PROJECT_ROOT / "nfe"),
        )
        if arquivo:
            self.nfe_source_var.set(arquivo)

    def _prepare_nfe_input(self) -> bool:
        destino = PROJECT_ROOT / "nfe" / "nfe.csv"
        origem = self.nfe_source_var.get().strip()

        if origem:
            origem_path = Path(origem)
            if not origem_path.exists():
                messagebox.showerror("Arquivo não encontrado", f"Não encontrado: {origem_path}")
                return False
            if origem_path.resolve() != destino.resolve():
                destino.parent.mkdir(parents=True, exist_ok=True)
                if destino.exists():
                    overwrite = messagebox.askyesno(
                        "Sobrescrever nfe.csv",
                        "Já existe nfe.csv em nfe/. Deseja sobrescrever?",
                    )
                    if not overwrite:
                        return False
                try:
                    shutil.copy2(origem_path, destino)
                    self._append_log(f"[INFO] Arquivo NFe copiado para {destino}\n")
                except Exception as exc:
                    messagebox.showerror("Erro ao copiar", str(exc))
                    return False
            return True

        if destino.exists():
            return True

        messagebox.showerror(
            "Arquivo NFe ausente",
            "Selecione um arquivo CSV de NFe antes de executar o pipeline.",
        )
        return False


def main() -> None:
    app = PainelMestre()
    app.mainloop()


if __name__ == "__main__":
    main()
