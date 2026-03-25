#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
PAINEL MESTRE - PIPELINES ANVISA/NFE
Interface grafica para executar pipelines sem linha de comando.
"""
from __future__ import annotations

import os
import sys
import threading
import queue
import subprocess
import shutil
from datetime import datetime
from pathlib import Path
import tkinter as tk
from tkinter import ttk, messagebox, filedialog

PROJECT_ROOT = Path(__file__).resolve().parent
PYTHON_EXE = sys.executable

COLORS = {
    "bg": "#0b1220",
    "panel": "#111827",
    "panel_alt": "#0f172a",
    "text": "#e2e8f0",
    "text_muted": "#93a4bf",
    "accent": "#38bdf8",
    "accent_hover": "#7dd3fc",
    "warning": "#f59e0b",
    "warning_hover": "#fbbf24",
    "danger": "#ef4444",
    "danger_hover": "#f87171",
    "border": "#253145",
    "log_bg": "#081126",
}

SCRIPTS = [
    ("1) Pipeline ANVISA (Execucao Unica)", "1_download_anvisa.py"),
    ("2) Pipeline NFe Completo", "3_pipeline_nfe.py"),
]

SCRIPT_ANVISA_PROCESSAR_SEM_DOWNLOAD = "2_processar_base_anvisa.py"
SCRIPT_ANVISA_APENAS_2B = "2b_processar_dados_anvisa.py"


class PainelMestre(tk.Tk):
    def __init__(self) -> None:
        super().__init__()
        self.title("Painel Mestre - Pipelines ANVISA/NFe")
        self.configure(bg=COLORS["bg"])
        self._configure_window()

        self._queue: queue.Queue[str] = queue.Queue()
        self._process: subprocess.Popen | None = None
        self._stop_requested = False

        self._auto_scroll = tk.BooleanVar(value=True)
        self.nfe_source_var = tk.StringVar(value="")
        self.status_var = tk.StringVar(value="Pronto")
        self.readiness_var = tk.StringVar(value="")

        self._build_ui()
        self._bind_shortcuts()
        self._refresh_file_status()
        self._poll_logs()

    def _configure_window(self) -> None:
        self.update_idletasks()
        screen_w = self.winfo_screenwidth()
        screen_h = self.winfo_screenheight()

        width = max(1024, int(screen_w * 0.82))
        height = max(680, int(screen_h * 0.82))
        self.minsize(960, 620)
        self.geometry(f"{width}x{height}")
        self._center_window(width, height)

    def _center_window(self, width: int, height: int) -> None:
        screen_w = self.winfo_screenwidth()
        screen_h = self.winfo_screenheight()
        x_pos = max((screen_w - width) // 2, 0)
        y_pos = max((screen_h - height) // 2, 0)
        self.geometry(f"{width}x{height}+{x_pos}+{y_pos}")

    def _build_ui(self) -> None:
        # Header
        header = tk.Frame(self, bg=COLORS["bg"])
        header.pack(fill=tk.X, padx=20, pady=(20, 8))

        title = tk.Label(
            header,
            text="Painel Mestre",
            font=("Segoe UI", 22, "bold"),
            fg="#f8fafc",
            bg=COLORS["bg"],
        )
        title.pack(anchor="w")

        subtitle = tk.Label(
            header,
            text="----",
            font=("Segoe UI", 11),
            fg="#cbd5f5",
            bg=COLORS["bg"],
        )
        subtitle.pack(anchor="w", pady=(2, 0))

        # Main content
        content = tk.Frame(self, bg=COLORS["bg"])
        content.pack(fill=tk.BOTH, expand=True, padx=20, pady=12)

        paned = tk.PanedWindow(
            content,
            orient=tk.HORIZONTAL,
            bg=COLORS["bg"],
            sashwidth=8,
            sashpad=2,
            relief="flat",
            bd=0,
        )
        paned.pack(fill=tk.BOTH, expand=True)

        # Left panel
        left = tk.Frame(
            paned,
            bg=COLORS["panel"],
            highlightthickness=1,
            highlightbackground=COLORS["border"],
        )
        paned.add(left, minsize=320)

        left_title = tk.Label(
            left,
            text="Executar Pipelines",
            font=("Segoe UI", 12, "bold"),
            fg=COLORS["text"],
            bg=COLORS["panel"],
        )
        left_title.pack(anchor="w", padx=14, pady=(14, 10))

        for label, script in SCRIPTS:
            ttk.Button(
                left,
                text=label,
                command=lambda s=script: self._run_script(s),
                style="Primary.TButton",
            ).pack(fill=tk.X, padx=14, pady=6)

        ttk.Button(
            left,
            text="1B) Processar Base ANVISA (sem baixar)",
            command=lambda: self._run_script(SCRIPT_ANVISA_PROCESSAR_SEM_DOWNLOAD),
            style="Rare.TButton",
        ).pack(fill=tk.X, padx=14, pady=(10, 4))

        rare_hint = tk.Label(
            left,
            text="Usa os dados ja baixados: executa etapa 1.5 + 2B.",
            font=("Segoe UI", 9),
            fg="#fbbf24",
            bg=COLORS["panel"],
            wraplength=280,
            justify="left",
        )
        rare_hint.pack(anchor="w", padx=14, pady=(0, 8))

        ttk.Button(
            left,
            text="1C) Apenas 2B",
            command=lambda: self._run_script(SCRIPT_ANVISA_APENAS_2B),
            style="Secondary.TButton",
        ).pack(fill=tk.X, padx=14, pady=(0, 8))

        nfe_frame = tk.Frame(left, bg=COLORS["panel"])
        nfe_frame.pack(fill=tk.X, padx=14, pady=(8, 6))

        nfe_label = tk.Label(
            nfe_frame,
            text="Arquivo NFe (CSV)",
            font=("Segoe UI", 10, "bold"),
            fg=COLORS["text"],
            bg=COLORS["panel"],
        )
        nfe_label.pack(anchor="w", pady=(0, 4))

        nfe_entry = tk.Entry(
            nfe_frame,
            textvariable=self.nfe_source_var,
            bg=COLORS["log_bg"],
            fg=COLORS["text"],
            insertbackground=COLORS["text"],
            relief="flat",
            highlightthickness=1,
            highlightbackground=COLORS["border"],
            bd=0,
        )
        nfe_entry.pack(fill=tk.X, pady=(0, 6))

        ttk.Button(
            nfe_frame,
            text="Selecionar arquivo...",
            command=self._pick_nfe_source,
            style="Secondary.TButton",
        ).pack(fill=tk.X)

        ttk.Button(
            nfe_frame,
            text="Abrir pasta nfe/",
            command=lambda: self._open_in_explorer(PROJECT_ROOT / "nfe"),
            style="Ghost.TButton",
        ).pack(fill=tk.X, pady=(6, 0))

        controls = tk.Frame(left, bg=COLORS["panel"])
        controls.pack(fill=tk.X, padx=14, pady=(12, 8))

        ttk.Button(
            controls,
            text="Limpar Log",
            command=self._clear_log,
            style="Ghost.TButton",
        ).pack(fill=tk.X, pady=(0, 6))

        ttk.Button(
            controls,
            text="Parar Execução",
            command=self._stop_process,
            style="Danger.TButton",
        ).pack(fill=tk.X)

        quick = tk.Frame(left, bg=COLORS["panel"])
        quick.pack(fill=tk.X, padx=14, pady=(0, 12))

        ttk.Button(
            quick,
            text="Abrir output/anvisa",
            command=lambda: self._open_in_explorer(PROJECT_ROOT / "output" / "anvisa"),
            style="Ghost.TButton",
        ).pack(fill=tk.X, pady=(0, 6))

        ttk.Button(
            quick,
            text="Salvar log em arquivo",
            command=self._save_log_to_file,
            style="Ghost.TButton",
        ).pack(fill=tk.X)

        readiness = tk.Label(
            left,
            textvariable=self.readiness_var,
            font=("Segoe UI", 9),
            fg=COLORS["text_muted"],
            bg=COLORS["panel"],
            justify="left",
            anchor="w",
            wraplength=280,
        )
        readiness.pack(fill=tk.X, padx=14, pady=(0, 14))

        # Right panel
        right = tk.Frame(
            paned,
            bg=COLORS["panel_alt"],
            highlightthickness=1,
            highlightbackground=COLORS["border"],
        )
        paned.add(right, minsize=500)

        log_title = tk.Label(
            right,
            text="Saida / Log",
            font=("Segoe UI", 12, "bold"),
            fg=COLORS["text"],
            bg=COLORS["panel_alt"],
        )
        log_title.pack(anchor="w", padx=12, pady=(10, 8))

        log_tools = tk.Frame(right, bg=COLORS["panel_alt"])
        log_tools.pack(fill=tk.X, padx=12, pady=(0, 8))

        ttk.Checkbutton(
            log_tools,
            text="Auto-scroll",
            variable=self._auto_scroll,
            style="Panel.TCheckbutton",
        ).pack(side=tk.LEFT)

        ttk.Button(
            log_tools,
            text="Copiar log",
            command=self._copy_log_to_clipboard,
            style="Ghost.TButton",
        ).pack(side=tk.RIGHT)

        log_wrap = tk.Frame(right, bg=COLORS["panel_alt"])
        log_wrap.pack(fill=tk.BOTH, expand=True, padx=12, pady=(0, 12))

        self.log_text = tk.Text(
            log_wrap,
            height=24,
            bg=COLORS["log_bg"],
            fg=COLORS["text"],
            insertbackground=COLORS["text"],
            font=("Cascadia Mono", 10),
            wrap="none",
            relief="flat",
            bd=0,
            highlightthickness=1,
            highlightbackground=COLORS["border"],
            padx=10,
            pady=8,
        )

        log_scroll = ttk.Scrollbar(log_wrap, orient="vertical", command=self.log_text.yview)
        log_scroll_x = ttk.Scrollbar(log_wrap, orient="horizontal", command=self.log_text.xview)
        self.log_text.configure(yscrollcommand=log_scroll.set, xscrollcommand=log_scroll_x.set)
        self.log_text.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        log_scroll.pack(side=tk.RIGHT, fill=tk.Y)
        log_scroll_x.pack(side=tk.BOTTOM, fill=tk.X)

        # Status bar
        status_wrap = tk.Frame(
            self,
            bg=COLORS["panel"],
            highlightthickness=1,
            highlightbackground=COLORS["border"],
        )
        status_wrap.pack(fill=tk.X, padx=20, pady=(4, 12))

        status = tk.Label(
            status_wrap,
            textvariable=self.status_var,
            font=("Segoe UI", 10),
            fg=COLORS["text_muted"],
            bg=COLORS["panel"],
            anchor="w",
        )
        status.pack(fill=tk.X, padx=10, pady=6, side=tk.LEFT, expand=True)

        hints = tk.Label(
            status_wrap,
            text="Atalhos: Ctrl+L limpar log | Ctrl+S salvar log",
            font=("Segoe UI", 9),
            fg=COLORS["text_muted"],
            bg=COLORS["panel"],
            anchor="e",
        )
        hints.pack(side=tk.RIGHT, padx=10)

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
            background=COLORS["accent"],
            padding=8,
        )
        style.map(
            "Primary.TButton",
            background=[("active", COLORS["accent_hover"]), ("disabled", COLORS["text_muted"])],
        )

        style.configure(
            "Secondary.TButton",
            font=("Segoe UI", 10, "bold"),
            foreground=COLORS["text"],
            background="#334155",
            padding=8,
        )
        style.map(
            "Secondary.TButton",
            background=[("active", "#475569"), ("disabled", COLORS["text_muted"])],
        )

        style.configure(
            "Danger.TButton",
            font=("Segoe UI", 10, "bold"),
            foreground="#ffffff",
            background=COLORS["danger"],
            padding=8,
        )
        style.map(
            "Danger.TButton",
            background=[("active", COLORS["danger_hover"]), ("disabled", COLORS["text_muted"])],
        )

        style.configure(
            "Rare.TButton",
            font=("Segoe UI", 10, "bold"),
            foreground="#111827",
            background=COLORS["warning"],
            padding=8,
        )
        style.map(
            "Rare.TButton",
            background=[("active", COLORS["warning_hover"]), ("disabled", COLORS["text_muted"])],
        )

        style.configure(
            "Ghost.TButton",
            font=("Segoe UI", 9),
            foreground=COLORS["text"],
            background="#1e293b",
            padding=6,
        )
        style.map(
            "Ghost.TButton",
            background=[("active", "#334155"), ("disabled", COLORS["text_muted"])],
        )

        style.configure(
            "Panel.TCheckbutton",
            background=COLORS["panel_alt"],
            foreground=COLORS["text_muted"],
            font=("Segoe UI", 9),
        )

    def _bind_shortcuts(self) -> None:
        self.bind_all("<Control-l>", lambda _e: self._clear_log())
        self.bind_all("<Control-s>", lambda _e: self._save_log_to_file())

    def _append_log(self, text: str) -> None:
        self.log_text.insert(tk.END, text)
        if self._auto_scroll.get():
            self.log_text.see(tk.END)

    def _clear_log(self) -> None:
        self.log_text.delete("1.0", tk.END)
        self.status_var.set("Log limpo")

    def _copy_log_to_clipboard(self) -> None:
        texto = self.log_text.get("1.0", tk.END).strip()
        if not texto:
            messagebox.showinfo("Copiar log", "Nao ha conteudo no log.")
            return
        self.clipboard_clear()
        self.clipboard_append(texto)
        self.status_var.set("Log copiado para a area de transferencia")

    def _save_log_to_file(self) -> None:
        texto = self.log_text.get("1.0", tk.END).strip()
        if not texto:
            messagebox.showinfo("Salvar log", "Não ha conteudo no log.")
            return

        default_name = f"log_pipeline_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        arquivo = filedialog.asksaveasfilename(
            title="Salvar log",
            defaultextension=".txt",
            initialfile=default_name,
            filetypes=[("TXT", "*.txt"), ("Todos os arquivos", "*.*")],
            initialdir=str(PROJECT_ROOT),
        )
        if not arquivo:
            return

        try:
            Path(arquivo).write_text(texto + "\n", encoding="utf-8")
            self.status_var.set(f"Log salvo em: {arquivo}")
        except Exception as exc:
            messagebox.showerror("Erro ao salvar log", str(exc))

    def _open_in_explorer(self, path: Path) -> None:
        path.mkdir(parents=True, exist_ok=True)
        try:
            if sys.platform.startswith("win"):
                os.startfile(str(path))  # type: ignore[attr-defined]
            elif sys.platform == "darwin":
                subprocess.Popen(["open", str(path)])
            else:
                subprocess.Popen(["xdg-open", str(path)])
        except Exception as exc:
            messagebox.showerror("Erro ao abrir pasta", str(exc))

    def _refresh_file_status(self) -> None:
        base_anvisa = PROJECT_ROOT / "output" / "anvisa" / "baseANVISA.csv"
        nfe_csv = PROJECT_ROOT / "nfe" / "nfe.csv"

        anvisa_txt = "OK" if base_anvisa.exists() else "Ausente"
        nfe_txt = "OK" if nfe_csv.exists() else "Ausente"

        self.readiness_var.set(
            "Prontidao rapida:\n"
            f"- Base ANVISA (output/anvisa/baseANVISA.csv): {anvisa_txt}\n"
            f"- NFe (nfe/nfe.csv): {nfe_txt}"
        )
        self.after(2500, self._refresh_file_status)

    def _run_script(self, script_rel: str | list[str]) -> None:
        if self._process is not None and self._process.poll() is None:
            messagebox.showwarning("Processo em execucao", "Ja existe um processo em execucao.")
            return

        script_list = [script_rel] if isinstance(script_rel, str) else script_rel
        script_paths = [PROJECT_ROOT / s for s in script_list]
        missing = [str(p) for p in script_paths if not p.exists()]
        if missing:
            messagebox.showerror("Arquivo nao encontrado", "Nao encontrado:\n" + "\n".join(missing))
            return

        if "3_pipeline_nfe.py" in script_list and not self._prepare_nfe_input():
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
            messagebox.showinfo("Nenhum processo", "Nenhum processo em execucao.")
            return

        self._stop_requested = True
        try:
            self._process.terminate()
            self.status_var.set("Processo interrompido")
            self._append_log("\n[INFO] Processo interrompido pelo usuario.\n")
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
                messagebox.showerror("Arquivo nao encontrado", f"Nao encontrado: {origem_path}")
                return False

            if origem_path.resolve() != destino.resolve():
                destino.parent.mkdir(parents=True, exist_ok=True)
                if destino.exists():
                    overwrite = messagebox.askyesno(
                        "Sobrescrever nfe.csv",
                        "Ja existe nfe.csv em nfe/. Deseja sobrescrever?",
                    )
                    if not overwrite:
                        return False
                try:
                    shutil.copy2(origem_path, destino)
                    self._append_log(f"[INFO] Arquivo NFe copiado para {destino}\n")
                except Exception as exc:
                    messagebox.showerror("Erro ao copiar", str(exc))
                    return False
            self._refresh_file_status()
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
