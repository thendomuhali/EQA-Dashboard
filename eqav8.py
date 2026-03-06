"""
EQA Dashboard (Standalone) - Modules + Multi-analyte LJ export + Stats flagging
-------------------------------------------------------------------------------
Adds:
1) Separate data store per MODULE: Module 1 / Module 2
2) Append month paste/import merges rows and sorts alphabetically (Analyte, Unit)
3) Export/Print LJ report:
   - choose multiple analytes
   - choose months
   - outputs a single multi-page PDF (recommended) OR multiple PNGs
4) Overall stats:
   - failure rate (FAIL defined as |Z|>3) and max |Z|
   - flag colour if failure rate > 20% OR max |Z| > 4.0
   - still shows OK/WARN/FAIL breakdown

Requirements:
    pip install pandas openpyxl matplotlib

Run:
    python eqa_dashboard.py
"""

import os
import json
import re
import sys
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, List, Tuple, Optional, Set

import numpy as np
import pandas as pd

import matplotlib
matplotlib.use("TkAgg")
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages

import tkinter as tk
from tkinter import ttk, filedialog, messagebox


MONTHS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
MONTH_TO_NUM = {m: i + 1 for i, m in enumerate(MONTHS)}
NUM_TO_MONTH = {i + 1: m for i, m in enumerate(MONTHS)}

MODULES = ["Module 1", "Module 2"]

STATUS_SYMBOLS = {"✔", "✘", "✍", "🖋", "âœ”", "âœ˜", "âœ", "ô€€€"}
UNICODE_MINUS = "−"


# ----------------- helpers -----------------

def _safe_float(x) -> float:
    try:
        if x is None:
            return np.nan
        if isinstance(x, str):
            s = x.strip()
            if not s:
                return np.nan
            s = s.replace(",", ".").replace(UNICODE_MINUS, "-").replace("âˆ’", "-")
            m = re.search(r"[-+]?\d*\.?\d+(?:[eE][-+]?\d+)?", s)
            if not m:
                return np.nan
            return float(m.group(0))
        return float(x)
    except Exception:
        return np.nan


def normalize_month(v) -> str:
    if v is None or (isinstance(v, float) and np.isnan(v)):
        return ""
    s = str(v).strip()
    if not s:
        return ""
    if s.isdigit():
        n = int(s)
        return NUM_TO_MONTH.get(n, "")
    s3 = s[:3].title()
    return s3 if s3 in MONTH_TO_NUM else ""


def clean_analyte_name(v: Any) -> str:
    s = "" if v is None else str(v).strip()
    s = s.replace("_", " ")
    # Remove symbols/punctuation so equivalent analytes aggregate together.
    s = re.sub(r"[^\w\s]", " ", s, flags=re.UNICODE)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def normalize_analyte_key(v: Any) -> str:
    return clean_analyte_name(v).casefold()


def z_status(z: float) -> Tuple[str, str]:
    """
    Tag categories (based on TRUE Z):
      ok   : |Z| <= 2
      warn : 2 < |Z| <= 3
      fail : |Z| > 3
    """
    if z is None or (isinstance(z, float) and np.isnan(z)):
        return ("", "")
    az = abs(float(z))
    if az > 3:
        return ("FAIL (|Z|>3)", "fail")
    if az > 2:
        return ("WARN (|Z|>2)", "warn")
    return ("OK", "ok")


def _is_number_token(tok: str) -> bool:
    if not tok:
        return False
    t = tok.strip().replace(",", ".").replace(UNICODE_MINUS, "-").replace("âˆ’", "-")
    return re.fullmatch(r"[-+]?\d*\.?\d+(?:[eE][-+]?\d+)?\*?", t) is not None


def parse_biorad_report_text(text: str) -> Tuple[List[Dict[str, Any]], Dict[str, str]]:
    """
    Parses lines like:
        [status] AFP ug/L 34.8 33.8 0.62 -0.07 Peer

    Assumed columns:
        [status] Analyte Unit Result Mean Z-score RMZ [Comparator]

    Stores row dict with keys:
        Analyte, Unit, Result, Mean, Z, RMZ, Notes
    """
    rows: List[Dict[str, Any]] = []
    meta: Dict[str, str] = {"provider": "Bio-Rad"}

    if not text or not text.strip():
        return rows, meta

    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]

    for ln in lines:
        if ln.lower().startswith("instrument:"):
            meta["instrument"] = ln.split(":", 1)[1].strip()

    for ln in lines:
        low = ln.lower()
        if low.startswith("instrument:") or low.startswith("legend:") or low.startswith("* amended") or "non-robust" in low:
            continue

        tokens = ln.split()
        if not tokens:
            continue

        # drop leading icon
        if tokens[0] in STATUS_SYMBOLS:
            tokens = tokens[1:]
        if len(tokens) < 6:
            continue

        # drop trailing comparator (optional)
        if tokens and (not _is_number_token(tokens[-1])) and re.fullmatch(r"[A-Za-z]+", tokens[-1]):
            tokens = tokens[:-1]
        if len(tokens) < 6:
            continue

        # pull numeric tokens from right (need >=4)
        nums: List[str] = []
        while tokens and _is_number_token(tokens[-1]):
            nums.append(tokens.pop())
        nums = list(reversed(nums))
        if len(nums) < 4:
            continue

        nums4 = nums[-4:]  # Result, Mean, Z, RMZ
        result = _safe_float(nums4[0])
        mean = _safe_float(nums4[1])
        z = _safe_float(nums4[2])
        rmz = _safe_float(nums4[3])

        if len(tokens) < 2:
            continue

        unit = tokens[-1]
        analyte = clean_analyte_name(" ".join(tokens[:-1]).strip())
        if not analyte:
            continue

        rows.append({
            "Analyte": analyte,
            "Unit": unit,
            "Result": float(result) if not np.isnan(result) else np.nan,
            "Mean": float(mean) if not np.isnan(mean) else np.nan,
            "Z": float(z) if not np.isnan(z) else np.nan,
            "RMZ": float(rmz) if not np.isnan(rmz) else np.nan,
            "Notes": ""
        })

    return rows, meta


def sort_rows_alpha(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    def key(r):
        a = str(r.get("Analyte", "")).strip().lower()
        u = str(r.get("Unit", "")).strip().lower()
        return (a, u)
    return sorted(rows, key=key)


def merge_append_rows(existing: List[Dict[str, Any]], incoming: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Append without replacing; keep duplicates (labs often want historical repeats).
    Then sort alphabetically.
    """
    merged = (existing or []) + (incoming or [])
    return sort_rows_alpha(merged)


# ----------------- dialogs -----------------

class MonthSelectDialog(tk.Toplevel):
    def __init__(self, parent: tk.Tk, title: str = "Select months"):
        super().__init__(parent)
        self.title(title)
        self.resizable(False, False)
        self.transient(parent)
        self.grab_set()
        self.selected: List[str] = []

        ttk.Label(self, text="Select months to include:").grid(row=0, column=0, padx=10, pady=(10, 6), sticky="w")

        self.lb = tk.Listbox(self, selectmode=tk.MULTIPLE, height=12, exportselection=False)
        for m in MONTHS:
            self.lb.insert(tk.END, m)
        self.lb.grid(row=1, column=0, padx=10, pady=6, sticky="nsew")

        btns = ttk.Frame(self)
        btns.grid(row=2, column=0, padx=10, pady=(6, 10), sticky="ew")

        ttk.Button(btns, text="Select all", command=self._select_all).pack(side=tk.LEFT)
        ttk.Button(btns, text="Clear", command=self._clear).pack(side=tk.LEFT, padx=6)
        ttk.Button(btns, text="OK", command=self._ok).pack(side=tk.RIGHT)
        ttk.Button(btns, text="Cancel", command=self._cancel).pack(side=tk.RIGHT, padx=6)

        self._select_all()

    def _select_all(self):
        self.lb.selection_clear(0, tk.END)
        self.lb.selection_set(0, tk.END)

    def _clear(self):
        self.lb.selection_clear(0, tk.END)

    def _ok(self):
        idxs = list(self.lb.curselection())
        self.selected = [MONTHS[i] for i in idxs]
        self.destroy()

    def _cancel(self):
        self.selected = []
        self.destroy()


class AnalyteSelectDialog(tk.Toplevel):
    def __init__(self, parent: tk.Tk, analytes: List[str], title: str = "Select analytes"):
        super().__init__(parent)
        self.title(title)
        self.resizable(False, False)
        self.transient(parent)
        self.grab_set()
        self.selected: List[str] = []

        ttk.Label(self, text="Select analytes to print/export (each on its own page):").grid(
            row=0, column=0, padx=10, pady=(10, 6), sticky="w"
        )

        self.lb = tk.Listbox(self, selectmode=tk.MULTIPLE, height=16, width=50, exportselection=False)
        for a in analytes:
            self.lb.insert(tk.END, a)
        self.lb.grid(row=1, column=0, padx=10, pady=6, sticky="nsew")

        btns = ttk.Frame(self)
        btns.grid(row=2, column=0, padx=10, pady=(6, 10), sticky="ew")

        ttk.Button(btns, text="Select all", command=self._select_all).pack(side=tk.LEFT)
        ttk.Button(btns, text="Clear", command=self._clear).pack(side=tk.LEFT, padx=6)
        ttk.Button(btns, text="OK", command=self._ok).pack(side=tk.RIGHT)
        ttk.Button(btns, text="Cancel", command=self._cancel).pack(side=tk.RIGHT, padx=6)

        # default: all selected for quick multi-page PDF export
        self._select_all()

    def _select_all(self):
        self.lb.selection_clear(0, tk.END)
        self.lb.selection_set(0, tk.END)

    def _clear(self):
        self.lb.selection_clear(0, tk.END)

    def _ok(self):
        idxs = list(self.lb.curselection())
        self.selected = [self.lb.get(i) for i in idxs]
        self.destroy()

    def _cancel(self):
        self.selected = []
        self.destroy()


# ----------------- app -----------------

class EQADashboard(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("EQA Dashboard (Modules)")
        self.geometry("1400x980")

        style = ttk.Style(self)
        try:
            style.theme_use("clam")
        except Exception:
            pass

        # store schema:
        # data["store"][module][year][month] = {"meta": {...}, "rows":[...]}
        self.store_path = self._default_store_path()
        self.data: Dict[str, Any] = {"schema_version": 5, "store": {m: {} for m in MODULES}}
        self._load_store()

        self.current_module = tk.StringVar(value=MODULES[0])

        # For UI mapping per module
        self._month_trees: Dict[Tuple[str, str, str], ttk.Treeview] = {}  # (module, year, month) -> tree

        # Plot controls
        self.year_filter_var = tk.StringVar(value="(All)")
        self.analyte_var = tk.StringVar(value="")

        # Paste controls
        self.paste_year_var = tk.StringVar(value=str(datetime.now().year))
        self.paste_month_var = tk.StringVar(value=MONTHS[0])
        self.paste_append_var = tk.BooleanVar(value=True)  # default append ON now (per your requirement)
        self.paste_report_id_var = tk.StringVar(value="")

        # Stats controls
        self.stats_year_var = tk.StringVar(value="(All)")

        # Header
        header = ttk.Frame(self, padding=(10, 10))
        header.pack(fill=tk.X)
        ttk.Label(header, text="EQA Dashboard", font=("Segoe UI", 14, "bold")).pack(anchor="w")
        ttk.Label(
            header,
            text="Module 1 / Module 2 | Paste parsing | Excel import | Persistent JSON store | Multi-analyte LJ export | Overall stats",
            font=("Segoe UI", 10)
        ).pack(anchor="w")

        # Top controls
        top = ttk.Frame(self, padding=(10, 6))
        top.pack(fill=tk.X)

        top_row1 = ttk.Frame(top)
        top_row1.pack(fill=tk.X)

        ttk.Label(top_row1, text="Module:").pack(side=tk.LEFT, padx=(0, 4))
        self.module_cb = ttk.Combobox(top_row1, textvariable=self.current_module, values=MODULES, state="readonly", width=12)
        self.module_cb.pack(side=tk.LEFT, padx=4)
        self.module_cb.bind("<<ComboboxSelected>>", lambda e: self._refresh_all_ui())

        ttk.Button(top_row1, text="Create EQA Excel Template...", command=self.create_template).pack(side=tk.LEFT, padx=4)
        ttk.Button(top_row1, text="Import EQA Excel...", command=self.import_eqa_excel).pack(side=tk.LEFT, padx=4)

        ttk.Button(top_row1, text="Save store", command=self._save_store).pack(side=tk.LEFT, padx=(12, 4))
        ttk.Button(top_row1, text="Reload store", command=self._reload).pack(side=tk.LEFT, padx=4)
        ttk.Button(top_row1, text="Change store location...", command=self.change_store_location).pack(side=tk.LEFT, padx=(12, 4))

        ttk.Button(top_row1, text="Delete a month...", command=self.delete_month_dialog).pack(side=tk.LEFT, padx=(12, 4))
        ttk.Button(top_row1, text="Delete a year...", command=self.delete_year_dialog).pack(side=tk.LEFT, padx=4)

        top_row2 = ttk.Frame(top)
        top_row2.pack(fill=tk.X, pady=(6, 0))

        ttk.Label(top_row2, text="Year (for plot/export):").pack(side=tk.LEFT, padx=(0, 4))
        self.year_cb = ttk.Combobox(top_row2, textvariable=self.year_filter_var, state="readonly", width=10)
        self.year_cb.pack(side=tk.LEFT, padx=4)

        ttk.Label(top_row2, text="Analyte:").pack(side=tk.LEFT, padx=(12, 4))
        self.analyte_cb = ttk.Combobox(top_row2, textvariable=self.analyte_var, state="readonly", width=30)
        self.analyte_cb.pack(side=tk.LEFT, padx=4)

        ttk.Button(top_row2, text="Plot Z-score LJ", command=self.plot_analyte).pack(side=tk.LEFT, padx=4)
        ttk.Button(top_row2, text="Export/Print LJ report...", command=self.export_print_lj_report_multi).pack(side=tk.LEFT, padx=8)
        ttk.Button(top_row2, text="Export EQA data PDF...", command=self.export_eqa_data_pdf).pack(side=tk.LEFT, padx=8)
        ttk.Button(top_row2, text="Export EQA data Excel...", command=self.export_eqa_data_excel).pack(side=tk.LEFT, padx=4)

        # Paste panel
        paste = ttk.LabelFrame(self, text="Paste Bio-Rad report text (auto-parse)", padding=(10, 8))
        paste.pack(fill=tk.X, padx=10, pady=(0, 8))

        row1 = ttk.Frame(paste)
        row1.pack(fill=tk.X)

        ttk.Label(row1, text="Year:").pack(side=tk.LEFT)
        ttk.Entry(row1, textvariable=self.paste_year_var, width=8).pack(side=tk.LEFT, padx=(4, 12))

        ttk.Label(row1, text="Month:").pack(side=tk.LEFT)
        ttk.Combobox(row1, textvariable=self.paste_month_var, values=MONTHS, state="readonly", width=6)\
            .pack(side=tk.LEFT, padx=(4, 12))

        ttk.Label(row1, text="Report ID (optional):").pack(side=tk.LEFT)
        ttk.Entry(row1, textvariable=self.paste_report_id_var, width=22).pack(side=tk.LEFT, padx=(4, 12))

        ttk.Checkbutton(row1, text="Append (recommended)", variable=self.paste_append_var)\
            .pack(side=tk.LEFT, padx=(0, 12))

        ttk.Button(row1, text="Parse & Save to Month", command=self.parse_and_save_paste)\
            .pack(side=tk.LEFT, padx=4)

        ttk.Button(row1, text="Clear", command=self._clear_paste_box).pack(side=tk.LEFT, padx=4)

        box_frame = ttk.Frame(paste)
        box_frame.pack(fill=tk.BOTH, expand=False, pady=(8, 0))
        self.paste_text = tk.Text(box_frame, height=7, wrap="none")
        vsb = ttk.Scrollbar(box_frame, orient="vertical", command=self.paste_text.yview)
        hsb = ttk.Scrollbar(box_frame, orient="horizontal", command=self.paste_text.xview)
        self.paste_text.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)
        self.paste_text.grid(row=0, column=0, sticky="nsew")
        vsb.grid(row=0, column=1, sticky="ns")
        hsb.grid(row=1, column=0, sticky="ew")
        box_frame.columnconfigure(0, weight=1)

        ttk.Label(
            paste,
            text="Parser expects: Analyte Unit Result Mean Z-score RMZ (Comparator optional and ignored).",
            font=("Segoe UI", 9)
        ).pack(anchor="w", pady=(6, 0))

        # MAIN NOTEBOOK: per module tabs
        self.main_nb = ttk.Notebook(self)
        self.main_nb.pack(fill=tk.BOTH, expand=True, padx=8, pady=8)

        self.module_tabs: Dict[str, Dict[str, Any]] = {}
        for mod in MODULES:
            tab = ttk.Frame(self.main_nb)
            self.main_nb.add(tab, text=mod)

            # module internal notebook: Data + Stats
            nb = ttk.Notebook(tab)
            nb.pack(fill=tk.BOTH, expand=True)

            data_tab = ttk.Frame(nb)
            nb.add(data_tab, text="EQA data")

            year_nb = ttk.Notebook(data_tab)
            year_nb.pack(fill=tk.BOTH, expand=True, padx=6, pady=6)

            stats_tab = ttk.Frame(nb)
            nb.add(stats_tab, text="Overall stats")

            # build stats UI for this module
            stats_ui = self._build_stats_tab(stats_tab, module=mod)

            self.module_tabs[mod] = {
                "outer_tab": tab,
                "inner_nb": nb,
                "year_nb": year_nb,
                "stats_tree": stats_ui["tree"],
                "stats_year_cb": stats_ui["year_cb"],
            }

        # Status bar
        self.status_var = tk.StringVar(value=f"Store: {self.store_path}")
        ttk.Label(self, textvariable=self.status_var, relief=tk.SUNKEN, anchor="w").pack(side=tk.BOTTOM, fill=tk.X)

        self._refresh_all_ui()

    # ---------------- store ----------------

    def _default_store_path(self) -> str:
        try:
            return str(Path(__file__).with_name("eqa_store.json"))
        except Exception:
            return "eqa_store.json"

    def _ensure_store_shape(self):
        if "store" not in self.data or not isinstance(self.data["store"], dict):
            self.data["store"] = {}
        for mod in MODULES:
            self.data["store"].setdefault(mod, {})

    def _normalize_store_analyte_names(self):
        self._ensure_store_shape()
        for mod in MODULES:
            store_mod = self.data["store"].get(mod, {}) or {}
            for _y, months in store_mod.items():
                for _m, obj in (months or {}).items():
                    for r in (obj.get("rows", []) or []):
                        r["Analyte"] = clean_analyte_name(r.get("Analyte", ""))

    def _load_store(self):
        if os.path.exists(self.store_path):
            try:
                with open(self.store_path, "r", encoding="utf-8") as f:
                    self.data = json.load(f)
            except Exception:
                self.data = {"schema_version": 5, "store": {m: {} for m in MODULES}}

        # migrate/ensure
        self.data.setdefault("schema_version", 5)
        self._ensure_store_shape()
        self._normalize_store_analyte_names()

    def _save_store(self):
        try:
            self._ensure_store_shape()
            with open(self.store_path, "w", encoding="utf-8") as f:
                json.dump(self.data, f, indent=2)
            self.status_var.set(f"Saved: {self.store_path}")
        except Exception as e:
            messagebox.showerror("Save error", f"Could not save store:\n{e}")

    def _reload(self):
        self.data = {"schema_version": 5, "store": {m: {} for m in MODULES}}
        self._load_store()
        self._refresh_all_ui()
        self.status_var.set(f"Reloaded: {self.store_path}")

    def change_store_location(self):
        path = filedialog.asksaveasfilename(
            title="Choose store file",
            defaultextension=".json",
            filetypes=[("JSON", "*.json")]
        )
        if not path:
            return
        self.store_path = path
        self._save_store()
        self._refresh_all_ui()
        self.status_var.set(f"Store: {self.store_path}")

    def _store_for_module(self, module: str) -> Dict[str, Any]:
        self._ensure_store_shape()
        return self.data["store"].setdefault(module, {})

    # ---------------- template ----------------

    def create_template(self):
        path = filedialog.asksaveasfilename(
            title="Save EQA Template",
            defaultextension=".xlsx",
            filetypes=[("Excel", "*.xlsx")]
        )
        if not path:
            return

        df = pd.DataFrame(columns=[
            "Module", "Year", "Month",
            "Report_ID", "Provider", "Instrument",
            "Analyte", "Unit",
            "Result", "Mean", "Zscore", "RMZ",
            "Notes"
        ])

        example = pd.DataFrame([
            {
                "Module": "Module 1",
                "Year": datetime.now().year,
                "Month": "Jan",
                "Report_ID": "Bio-Rad-YYYYMM",
                "Provider": "Bio-Rad",
                "Instrument": "Siemens Atellica IM Analyzer",
                "Analyte": "AFP",
                "Unit": "ug/L",
                "Result": 34.8,
                "Mean": 33.8,
                "Zscore": 0.62,
                "RMZ": -0.07,
                "Notes": ""
            }
        ])

        try:
            with pd.ExcelWriter(path, engine="openpyxl") as writer:
                df.to_excel(writer, index=False, sheet_name="EQA_Entries")
                example.to_excel(writer, index=False, sheet_name="Example")
            messagebox.showinfo("Template created", f"Saved template:\n{path}\n\nFill sheet: EQA_Entries")
        except Exception as e:
            messagebox.showerror("Template error", f"Could not create template:\n{e}")

    # ---------------- ui helpers ----------------

    def _wrap_tree(self, parent, columns, height=16):
        container = ttk.Frame(parent)
        container.pack(fill=tk.BOTH, expand=True, padx=8, pady=8)

        tree = ttk.Treeview(container, columns=columns, show="headings", height=height)
        vsb = ttk.Scrollbar(container, orient="vertical", command=tree.yview)
        hsb = ttk.Scrollbar(container, orient="horizontal", command=tree.xview)
        tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)

        container.rowconfigure(0, weight=1)
        container.columnconfigure(0, weight=1)

        tree.grid(row=0, column=0, sticky="nsew")
        vsb.grid(row=0, column=1, sticky="ns")
        hsb.grid(row=1, column=0, sticky="ew")

        # month table tags
        tree.tag_configure("ok", background="#d9fdd3")
        tree.tag_configure("warn", background="#fff7cc")
        tree.tag_configure("fail", background="#ffd6d6")

        # stats tags
        tree.tag_configure("stats_ok", background="#d9fdd3")
        tree.tag_configure("stats_warn", background="#fff7cc")
        tree.tag_configure("stats_flag", background="#ffd6d6")  # >20% fail OR max|Z|>4

        return tree

    def _refresh_all_ui(self):
        # ensure module tabs exist; refresh each module year/month trees
        self._month_trees.clear()

        # refresh year notebooks for each module
        for mod in MODULES:
            year_nb: ttk.Notebook = self.module_tabs[mod]["year_nb"]

            # clear existing tabs
            for tab_id in year_nb.tabs():
                year_nb.forget(tab_id)

            store_mod = self._store_for_module(mod)
            years = sorted(store_mod.keys(), key=lambda y: int(y) if str(y).isdigit() else str(y))

            if not years:
                frame = ttk.Frame(year_nb)
                year_nb.add(frame, text="(No data)")
                ttk.Label(frame, text="No EQA data yet for this module.\n\nImport Excel or paste a report block above.", padding=20)\
                    .pack(anchor="w")
            else:
                for y in years:
                    y_frame = ttk.Frame(year_nb)
                    year_nb.add(y_frame, text=str(y))

                    month_nb = ttk.Notebook(y_frame)
                    month_nb.pack(fill=tk.BOTH, expand=True, padx=6, pady=6)

                    for m in MONTHS:
                        m_frame = ttk.Frame(month_nb)
                        month_nb.add(m_frame, text=m)

                        cols = ["Analyte", "Unit", "Result", "Mean", "Z-score", "RMZ", "Status", "Notes"]
                        tree = self._wrap_tree(m_frame, cols, height=16)
                        widths = {"Analyte": 260, "Notes": 360}
                        for c in cols:
                            tree.heading(c, text=c)
                            tree.column(c, width=widths.get(c, 110), anchor=tk.CENTER if c != "Notes" else tk.W)

                        self._month_trees[(mod, str(y), m)] = tree

            # refresh stats year combobox for module
            stats_year_cb: ttk.Combobox = self.module_tabs[mod]["stats_year_cb"]
            year_vals = ["(All)"] + years
            stats_year_cb["values"] = year_vals
            if self.stats_year_var.get() not in year_vals:
                self.stats_year_var.set("(All)")

        # refresh plot year/analyte combos based on CURRENT module
        self._refresh_plot_controls()

        # refresh tables + stats for all modules
        self._refresh_month_tables_all_modules()
        self.refresh_stats_all_modules()

        # switch visible module tab to current module
        current_mod = self.current_module.get()
        idx = MODULES.index(current_mod) if current_mod in MODULES else 0
        self.main_nb.select(idx)

    def _refresh_month_tables_all_modules(self):
        for (mod, y, m), tree in self._month_trees.items():
            for iid in tree.get_children():
                tree.delete(iid)

            store_mod = self._store_for_module(mod)
            month_obj = (store_mod.get(y, {}) or {}).get(m, {}) or {}
            rows = month_obj.get("rows", []) or []

            for r in rows:
                analyte = clean_analyte_name(r.get("Analyte", ""))
                unit = str(r.get("Unit", "")).strip()
                result = r.get("Result", np.nan)
                mean = r.get("Mean", np.nan)
                z = r.get("Z", np.nan)
                rmz = r.get("RMZ", np.nan)
                notes = str(r.get("Notes", "") or "")

                status_txt, tag = z_status(z)

                values = [
                    analyte,
                    unit,
                    "" if pd.isna(result) else round(float(result), 6),
                    "" if pd.isna(mean) else round(float(mean), 6),
                    "" if (z is None or (isinstance(z, float) and np.isnan(z))) else round(float(z), 3),
                    "" if pd.isna(rmz) else round(float(rmz), 6),
                    status_txt,
                    notes,
                ]
                tree.insert("", tk.END, values=values, tags=(tag,) if tag else ())

    def _refresh_plot_controls(self):
        mod = self.current_module.get()
        store_mod = self._store_for_module(mod)
        years = sorted(store_mod.keys(), key=lambda y: int(y) if str(y).isdigit() else str(y))
        year_vals = ["(All)"] + years
        self.year_cb["values"] = year_vals
        if self.year_filter_var.get() not in year_vals:
            self.year_filter_var.set("(All)")

        analytes = sorted(self._get_all_analytes_for_module(mod))
        self.analyte_cb["values"] = analytes
        if analytes:
            if self.analyte_var.get().strip() not in analytes:
                self.analyte_var.set(analytes[0])
        else:
            self.analyte_var.set("")

    def _get_all_analytes_for_module(self, module: str) -> Set[str]:
        analytes = set()
        store_mod = self._store_for_module(module)
        for _, months in store_mod.items():
            for _, obj in (months or {}).items():
                for r in (obj.get("rows", []) or []):
                    a = clean_analyte_name(r.get("Analyte", ""))
                    if a:
                        analytes.add(a)
        return analytes

    def _clear_paste_box(self):
        self.paste_text.delete("1.0", tk.END)

    # ---------------- paste parsing ----------------

    def parse_and_save_paste(self):
        raw = self.paste_text.get("1.0", tk.END).strip()
        if not raw:
            messagebox.showwarning("Paste", "Paste the report text first.")
            return

        year = self.paste_year_var.get().strip()
        month = self.paste_month_var.get().strip()
        module = self.current_module.get()

        if not year or not year.isdigit():
            messagebox.showwarning("Year", "Enter a valid numeric year (e.g., 2026).")
            return
        if month not in MONTHS:
            messagebox.showwarning("Month", "Select a valid month (Jan-Dec).")
            return

        rows, meta = parse_biorad_report_text(raw)
        if not rows:
            messagebox.showerror(
                "Parse failed",
                "No EQA rows were parsed.\n\nExpected:\n"
                "[status] AFP ug/L 34.8 33.8 0.62 -0.07 Peer\n"
                "(Result, Mean, Z-score, RMZ)"
            )
            return

        store_mod = self._store_for_module(module)
        y_obj = store_mod.setdefault(year, {})
        month_obj = y_obj.setdefault(month, {})
        month_obj.setdefault("meta", {})

        for k, v in meta.items():
            if v:
                month_obj["meta"][k] = v

        rid = self.paste_report_id_var.get().strip()
        if rid:
            month_obj["meta"]["report_id"] = rid

        # APPEND logic: never replace; always merge + sort alphabetically
        existing = month_obj.get("rows", []) or []
        month_obj["rows"] = merge_append_rows(existing, rows)

        self._save_store()
        self._refresh_all_ui()

        messagebox.showinfo(
            "Saved",
            f"Parsed and saved {len(rows)} rows to {module} - {year} {month}.\n"
            f"Month now contains {len(month_obj['rows'])} rows (appended + sorted)."
        )

    # ---------------- excel import ----------------

    def import_eqa_excel(self):
        path = filedialog.askopenfilename(
            title="Select EQA template Excel",
            filetypes=[("Excel", "*.xlsx *.xls")]
        )
        if not path:
            return

        try:
            df = pd.read_excel(path, sheet_name="EQA_Entries")
        except Exception as e:
            messagebox.showerror("Import error", f"Could not read sheet 'EQA_Entries':\n{e}")
            return

        # required
        for col in ["Year", "Month", "Analyte"]:
            if col not in df.columns:
                messagebox.showerror("Import error", f"Missing required column: {col}")
                return

        # module optional
        if "Module" not in df.columns:
            df["Module"] = self.current_module.get()

        df = df.copy()
        df["Month"] = df["Month"].apply(normalize_month)
        df["Module"] = df["Module"].astype(str).str.strip()
        df = df[df["Month"].isin(MONTHS)].copy()
        df = df.dropna(subset=["Year", "Month", "Analyte"], how="any")

        if df.empty:
            messagebox.showwarning("No data", "No valid rows found after cleaning. Check Year/Month/Analyte columns.")
            return

        # ensure optional columns exist
        opt_cols = ["Unit", "Result", "Mean", "Zscore", "RMZ", "Notes", "Report_ID", "Provider", "Instrument"]
        for c in opt_cols:
            if c not in df.columns:
                df[c] = np.nan

        imported = 0

        for (module, year, month), sub in df.groupby(["Module", "Year", "Month"]):
            mod_key = str(module).strip()
            if mod_key not in MODULES:
                # if user typed "1" or "Module1" etc., we try to map
                if "2" in mod_key:
                    mod_key = "Module 2"
                else:
                    mod_key = "Module 1"

            y_key = str(int(year)) if str(year).isdigit() else str(year).strip()
            m_key = str(month).strip()

            store_mod = self._store_for_module(mod_key)
            y_obj = store_mod.setdefault(y_key, {})
            month_obj = y_obj.setdefault(m_key, {})
            month_obj.setdefault("meta", {})

            rep = sub["Report_ID"].dropna().astype(str)
            prov = sub["Provider"].dropna().astype(str)
            inst = sub["Instrument"].dropna().astype(str)
            if not rep.empty:
                month_obj["meta"]["report_id"] = rep.iloc[0]
            if not prov.empty:
                month_obj["meta"]["provider"] = prov.iloc[0]
            if not inst.empty:
                month_obj["meta"]["instrument"] = inst.iloc[0]

            incoming: List[Dict[str, Any]] = []
            for _, r in sub.iterrows():
                analyte = clean_analyte_name(r.get("Analyte", ""))
                unit = "" if pd.isna(r.get("Unit")) else str(r.get("Unit")).strip()

                result = _safe_float(r.get("Result"))
                mean = _safe_float(r.get("Mean"))
                z = _safe_float(r.get("Zscore"))
                rmz = _safe_float(r.get("RMZ"))

                row = {
                    "Analyte": analyte,
                    "Unit": unit,
                    "Result": float(result) if not np.isnan(result) else np.nan,
                    "Mean": float(mean) if not np.isnan(mean) else np.nan,
                    "Z": float(z) if not np.isnan(z) else np.nan,
                    "RMZ": float(rmz) if not np.isnan(rmz) else np.nan,
                    "Notes": "" if pd.isna(r.get("Notes")) else str(r.get("Notes")).strip(),
                }
                if analyte:
                    incoming.append(row)
                    imported += 1

            existing = month_obj.get("rows", []) or []
            month_obj["rows"] = merge_append_rows(existing, incoming)

        self._save_store()
        self._refresh_all_ui()
        messagebox.showinfo("Imported", f"Imported/appended {imported} EQA rows from:\n{path}")

    # ---------------- delete month/year (module-specific) ----------------

    def delete_month_dialog(self):
        module = self.current_module.get()
        store_mod = self._store_for_module(module)
        years = sorted(store_mod.keys(), key=lambda y: int(y) if str(y).isdigit() else str(y))
        if not years:
            messagebox.showinfo("Delete month", f"No years exist for {module}.")
            return

        dlg = tk.Toplevel(self)
        dlg.title(f"Delete a month - {module}")
        dlg.resizable(False, False)
        dlg.transient(self)
        dlg.grab_set()

        ttk.Label(dlg, text=f"Select a year and month to delete for {module} (cannot be undone):").grid(
            row=0, column=0, columnspan=2, padx=10, pady=(10, 6), sticky="w"
        )

        ttk.Label(dlg, text="Year:").grid(row=1, column=0, padx=(10, 6), pady=6, sticky="w")
        y_var = tk.StringVar(value=years[0])
        y_cb = ttk.Combobox(dlg, textvariable=y_var, values=years, state="readonly", width=12)
        y_cb.grid(row=1, column=1, padx=(0, 10), pady=6, sticky="w")

        ttk.Label(dlg, text="Month:").grid(row=2, column=0, padx=(10, 6), pady=6, sticky="w")
        m_var = tk.StringVar(value="")
        m_cb = ttk.Combobox(dlg, textvariable=m_var, state="readonly", width=12)
        m_cb.grid(row=2, column=1, padx=(0, 10), pady=6, sticky="w")

        def refresh_month_values():
            y = y_var.get().strip()
            months_in_year = []
            for m in MONTHS:
                if m in (store_mod.get(y, {}) or {}):
                    months_in_year.append(m)
            m_cb["values"] = months_in_year
            m_var.set(months_in_year[0] if months_in_year else "")

        def do_delete():
            y = y_var.get().strip()
            m = m_var.get().strip()
            if not y or not m:
                return

            y_obj = store_mod.get(y, {}) or {}
            if m not in y_obj:
                messagebox.showerror("Delete month", "Month not found in store.")
                dlg.destroy()
                return

            if not messagebox.askyesno(
                "Confirm delete",
                f"Delete EQA data for:\n\n{module} - {y} {m}\n\nThis cannot be undone."
            ):
                return

            del y_obj[m]
            if not y_obj:
                del store_mod[y]

            self._save_store()
            self._refresh_all_ui()
            dlg.destroy()
            messagebox.showinfo("Deleted", f"Deleted {module} {y} {m}.")

        refresh_month_values()
        y_cb.bind("<<ComboboxSelected>>", lambda e: refresh_month_values())

        btns = ttk.Frame(dlg)
        btns.grid(row=3, column=0, columnspan=2, padx=10, pady=(6, 10), sticky="ew")
        ttk.Button(btns, text="Cancel", command=dlg.destroy).pack(side=tk.RIGHT)
        ttk.Button(btns, text="Delete", command=do_delete).pack(side=tk.RIGHT, padx=6)

    def delete_year_dialog(self):
        module = self.current_module.get()
        store_mod = self._store_for_module(module)
        years = sorted(store_mod.keys(), key=lambda y: int(y) if str(y).isdigit() else str(y))
        if not years:
            messagebox.showinfo("Delete year", f"No years exist for {module}.")
            return

        dlg = tk.Toplevel(self)
        dlg.title(f"Delete a year - {module}")
        dlg.resizable(False, False)
        dlg.transient(self)
        dlg.grab_set()

        ttk.Label(dlg, text=f"Select a year to delete for {module} (cannot be undone):").grid(
            row=0, column=0, padx=10, pady=(10, 6), sticky="w"
        )
        y_var = tk.StringVar(value=years[0])
        cb = ttk.Combobox(dlg, textvariable=y_var, values=years, state="readonly", width=12)
        cb.grid(row=1, column=0, padx=10, pady=6, sticky="w")

        def do_delete():
            y = y_var.get().strip()
            if not y:
                return
            if y not in store_mod:
                messagebox.showerror("Delete year", "Year not found in store.")
                dlg.destroy()
                return

            if not messagebox.askyesno(
                "Confirm delete",
                f"Delete ALL EQA data for:\n\n{module} - {y}\n\nThis cannot be undone."
            ):
                return

            del store_mod[y]
            self._save_store()
            self._refresh_all_ui()
            dlg.destroy()
            messagebox.showinfo("Deleted", f"Deleted {module} year {y}.")

        btns = ttk.Frame(dlg)
        btns.grid(row=2, column=0, padx=10, pady=(6, 10), sticky="ew")
        ttk.Button(btns, text="Cancel", command=dlg.destroy).pack(side=tk.RIGHT)
        ttk.Button(btns, text="Delete", command=do_delete).pack(side=tk.RIGHT, padx=6)

    # ---------------- plotting ----------------

    def _collect_analyte_points(self, module: str, analyte: str, year_filter: str, months_filter: Optional[List[str]] = None) -> List[Tuple[int, int, float, float]]:
        store_mod = self._store_for_module(module)
        pts: List[Tuple[int, int, float, float]] = []
        a_key = normalize_analyte_key(analyte)
        months_filter_set = set(months_filter) if months_filter else None

        for y, months in store_mod.items():
            if year_filter != "(All)" and str(y) != year_filter:
                continue
            try:
                yi = int(y)
            except Exception:
                continue

            for m, obj in (months or {}).items():
                m3 = str(m).strip()[:3].title()
                if m3 not in MONTH_TO_NUM:
                    continue
                if months_filter_set is not None and m3 not in months_filter_set:
                    continue

                mi = MONTH_TO_NUM[m3]
                for r in (obj.get("rows", []) or []):
                    if normalize_analyte_key(r.get("Analyte", "")) == a_key:
                        z = r.get("Z", np.nan)
                        try:
                            zf = float(z)
                        except Exception:
                            continue
                        if np.isnan(zf):
                            continue
                        rv = _safe_float(r.get("Result"))
                        pts.append((yi, mi, zf, rv))

        pts.sort(key=lambda t: (t[0], t[1]))
        return pts

    def _plot_single_analyte_figure(self, analyte: str, labels: List[str], yv: np.ndarray, title: str, result_labels: Optional[List[str]] = None):
        fig = plt.figure(figsize=(11, 4.6))
        x = np.arange(len(yv))

        plt.plot(x, yv, marker="o", linestyle="-", color="black", linewidth=1.0)
        plt.axhline(0, linestyle="--", color="blue", linewidth=1.0)
        plt.axhline(2, linestyle="--", color="orange", linewidth=1.0)
        plt.axhline(-2, linestyle="--", color="orange", linewidth=1.0)
        plt.axhline(3, linestyle="--", color="red", linewidth=1.0)
        plt.axhline(-3, linestyle="--", color="red", linewidth=1.0)

        if result_labels:
            for xi, yi, txt in zip(x, yv, result_labels):
                if txt:
                    plt.annotate(txt, (xi, yi), textcoords="offset points", xytext=(0, 6), ha="center", fontsize=7)

        plt.xticks(x, labels, rotation=45, ha="right")
        plt.ylabel("Z-score")
        plt.title(title)
        plt.grid(True, linestyle=":", alpha=0.3)
        plt.tight_layout()
        return fig

    def plot_analyte(self):
        module = self.current_module.get()
        analyte = self.analyte_var.get().strip()
        if not analyte:
            messagebox.showwarning("Plot", "No analyte selected.")
            return

        year_filter = self.year_filter_var.get().strip()
        pts = self._collect_analyte_points(module, analyte, year_filter)

        if not pts:
            messagebox.showinfo("Plot", f"No Z-score data found for: {analyte}")
            return

        labels = [f"{y}-{m:02d}" for (y, m, _, _) in pts]
        yv = np.array([z for (_, _, z, _) in pts], dtype=float)
        result_labels = [("" if pd.isna(r) else f"{float(r):g}") for (_, _, _, r) in pts]

        title = f"{module} - EQA Z-score trend (LJ-style): {analyte} (Year filter: {year_filter})"
        fig = self._plot_single_analyte_figure(analyte, labels, yv, title, result_labels=result_labels)
        plt.show()

    # ---------------- export/print multi-analyte ----------------

    def export_print_lj_report_multi(self):
        module = self.current_module.get()
        analytes = sorted(list(self._get_all_analytes_for_module(module)))
        if not analytes:
            messagebox.showwarning("Report", f"No analytes available for {module}.")
            return

        # Select analytes
        dlg_a = AnalyteSelectDialog(self, analytes, title="Select analytes for LJ report")
        self.wait_window(dlg_a)
        selected_analytes = dlg_a.selected
        if not selected_analytes:
            return

        # Select months
        dlg_m = MonthSelectDialog(self, title="Select months for LJ report")
        self.wait_window(dlg_m)
        selected_months = dlg_m.selected
        if not selected_months:
            return

        year_filter = self.year_filter_var.get().strip()

        # Choose output (single multi-page PDF)
        path = filedialog.asksaveasfilename(
            title="Save LJ report (multi-page PDF)",
            defaultextension=".pdf",
            filetypes=[("PDF", "*.pdf")]
        )
        if not path:
            return

        if not path.lower().endswith(".pdf"):
            path = f"{path}.pdf"

        # Generate
        try:
            with PdfPages(path) as pdf:
                pages = 0
                for analyte in selected_analytes:
                    pts = self._collect_analyte_points(module, analyte, year_filter, months_filter=selected_months)
                    if not pts:
                        continue
                    labels = [f"{y}-{m:02d}" for (y, m, _, _) in pts]
                    yv = np.array([z for (_, _, z, _) in pts], dtype=float)
                    result_labels = [("" if pd.isna(r) else f"{float(r):g}") for (_, _, _, r) in pts]
                    months_str = ", ".join(selected_months)
                    title = f"{module} - {analyte}\nMonths: {months_str}    Year filter: {year_filter}"
                    fig = self._plot_single_analyte_figure(analyte, labels, yv, title, result_labels=result_labels)
                    pdf.savefig(fig)
                    plt.close(fig)
                    pages += 1

            if pages == 0:
                messagebox.showinfo("Report", "No data points found for selected analytes/months/year filter.")
                return

        except Exception as e:
            messagebox.showerror("Report", f"Failed to export report:\n{e}")
            return

        # Attempt print on Windows
        try:
            if sys.platform.startswith("win"):
                if messagebox.askyesno("Print", "Report saved. Print it now? (Windows only)"):
                    os.startfile(path, "print")
        except Exception:
            pass

        messagebox.showinfo("Report saved", f"Saved LJ report:\n{path}")

    # ---------------- stats ----------------

    def _build_stats_tab(self, parent, module: str):
        ctrl = ttk.Frame(parent, padding=(10, 8))
        ctrl.pack(fill=tk.X)

        ttk.Label(ctrl, text="Year:").pack(side=tk.LEFT)
        year_cb = ttk.Combobox(ctrl, textvariable=self.stats_year_var, state="readonly", width=10)
        year_cb.pack(side=tk.LEFT, padx=(6, 18))
        year_cb.bind("<<ComboboxSelected>>", lambda e: self.refresh_stats_for_module(module))

        ttk.Button(ctrl, text="Refresh stats", command=lambda: self.refresh_stats_for_module(module)).pack(side=tk.LEFT, padx=4)
        ttk.Button(ctrl, text="Export stats Excel...", command=lambda: self.export_stats_excel(module)).pack(side=tk.LEFT, padx=4)
        ttk.Button(ctrl, text="Print stats table (PDF)...", command=lambda: self.export_stats_pdf(module)).pack(side=tk.LEFT, padx=4)

        ttk.Label(ctrl, text="Flag rule: Failure rate uses |Z|>2; categories remain unchanged. Flag if rate > 20% OR Max |Z| > 4.0", padding=(18, 0)).pack(side=tk.LEFT)

        cols = [
            "Analyte",
            "N_total",
            "OK(|Z|<=2)",
            "WARN(2<|Z|<=3)",
            "FAIL(|Z|>3)",
            "Fail_rate(|Z|>2)(%)",
            "Max_|Z|",
            "First_FAIL",
            "Last_FAIL",
            "FLAG",
        ]
        tree = self._wrap_tree(parent, cols, height=18)
        for c in cols:
            w = 150
            if c == "Analyte":
                w = 300
            tree.heading(c, text=c)
            tree.column(c, width=w, anchor=tk.CENTER if c != "Analyte" else tk.W)

        return {"tree": tree, "year_cb": year_cb}

    def _iter_all_rows(self, module: str, year_filter: str) -> List[Tuple[str, str, Dict[str, Any]]]:
        out: List[Tuple[str, str, Dict[str, Any]]] = []
        store_mod = self._store_for_module(module)
        for y, months in store_mod.items():
            if year_filter != "(All)" and str(y) != year_filter:
                continue
            for m, obj in (months or {}).items():
                m3 = str(m).strip()[:3].title()
                if m3 not in MONTH_TO_NUM:
                    continue
                for r in (obj.get("rows", []) or []):
                    out.append((str(y), m3, r))
        return out

    def refresh_stats_all_modules(self):
        for mod in MODULES:
            self.refresh_stats_for_module(mod)

    def refresh_stats_for_module(self, module: str):
        tree: ttk.Treeview = self.module_tabs[module]["stats_tree"]
        for iid in tree.get_children():
            tree.delete(iid)

        year_filter = self.stats_year_var.get().strip()
        rows = self._iter_all_rows(module, year_filter)
        if not rows:
            return

        agg: Dict[str, Dict[str, Any]] = {}

        for y, m, r in rows:
            analyte = clean_analyte_name(r.get("Analyte", ""))
            if not analyte:
                continue
            analyte_key = normalize_analyte_key(analyte)

            z = r.get("Z", np.nan)
            try:
                zf = float(z)
            except Exception:
                zf = np.nan

            a = agg.setdefault(analyte_key, {
                "Analyte": analyte,
                "N_total": 0,
                "ok": 0,
                "warn": 0,
                "fail": 0,
                "fail_eval": 0,
                "max_abs_z": np.nan,
                "fail_months": [],
                "fail_eval_months": [],
            })

            a["N_total"] += 1
            if not np.isnan(zf):
                az = abs(zf)
                if np.isnan(a["max_abs_z"]) or az > a["max_abs_z"]:
                    a["max_abs_z"] = az

                if az > 3:
                    a["fail"] += 1
                    a["fail_months"].append(f"{y}-{m}")
                elif az > 2:
                    a["warn"] += 1
                else:
                    a["ok"] += 1

                # Failure-rate rule uses |Z| > 2 while category buckets remain unchanged.
                if az > 2:
                    a["fail_eval"] += 1
                    a["fail_eval_months"].append(f"{y}-{m}")

        # Sort: most problematic first
        def sort_key(item):
            _akey, a = item
            return (-a["fail_eval"], -a["fail"], -a["warn"], a["Analyte"].lower())

        for _akey, a in sorted(agg.items(), key=sort_key):
            analyte = a["Analyte"]
            n = a["N_total"]
            ok = a["ok"]
            warn = a["warn"]
            fail = a["fail"]
            fail_eval = a["fail_eval"]
            fail_rate = (fail_eval / n * 100.0) if n else 0.0
            max_abs = a["max_abs_z"]

            fail_eval_months = a["fail_eval_months"]
            first_fail = fail_eval_months[0] if fail_eval_months else ""
            last_fail = fail_eval_months[-1] if fail_eval_months else ""

            # NEW flag rule:
            flagged = (fail_rate > 20.0) or ((not np.isnan(max_abs)) and (float(max_abs) > 4.0))
            flag_txt = "FLAG" if flagged else ""

            # colour tags
            if flagged:
                tag = "stats_flag"
            elif fail_eval > 0 or ((not np.isnan(max_abs)) and float(max_abs) > 3):
                tag = "stats_warn"
            else:
                tag = "stats_ok"

            values = [
                analyte,
                n,
                ok,
                warn,
                fail,
                round(fail_rate, 2),
                "" if np.isnan(max_abs) else round(float(max_abs), 2),
                first_fail,
                last_fail,
                flag_txt,
            ]
            tree.insert("", tk.END, values=values, tags=(tag,))

    def _render_table_pdf(self, df: pd.DataFrame, path: str, title: str, rows_per_page: int = 28):
        if df is None or df.empty:
            raise ValueError("No rows to render.")

        total_rows = len(df)
        with PdfPages(path) as pdf:
            for start in range(0, total_rows, rows_per_page):
                end = min(start + rows_per_page, total_rows)
                chunk = df.iloc[start:end].copy()

                fig, ax = plt.subplots(figsize=(11.69, 8.27))  # A4 landscape
                ax.axis("off")
                ax.set_title(f"{title} (rows {start + 1}-{end} of {total_rows})", fontsize=12, pad=10)

                cell_text = chunk.fillna("").astype(str).values
                table = ax.table(
                    cellText=cell_text,
                    colLabels=list(chunk.columns),
                    cellLoc="center",
                    loc="center",
                )
                table.auto_set_font_size(False)
                table.set_fontsize(8)
                table.scale(1.0, 1.25)

                for (r, c), cell in table.get_celld().items():
                    if r == 0:
                        cell.set_text_props(weight="bold")
                        cell.set_facecolor("#e8eef7")

                plt.tight_layout()
                pdf.savefig(fig)
                plt.close(fig)

    def _stats_dataframe_from_tree(self, module: str) -> pd.DataFrame:
        tree: ttk.Treeview = self.module_tabs[module]["stats_tree"]
        rows = [tree.item(iid, "values") for iid in tree.get_children()]

        cols = [
            "Analyte", "N_total", "OK(|Z|<=2)", "WARN(2<|Z|<=3)", "FAIL(|Z|>3)",
            "Fail_rate(|Z|>2)(%)", "Max_|Z|", "First_FAIL", "Last_FAIL", "FLAG",
        ]
        return pd.DataFrame(rows, columns=cols)

    def export_stats_csv(self, module: str):
        df = self._stats_dataframe_from_tree(module)
        if df.empty:
            messagebox.showwarning("Export", "No stats to export.")
            return

        path = filedialog.asksaveasfilename(
            title=f"Save stats CSV ({module})",
            defaultextension=".csv",
            filetypes=[("CSV", "*.csv")]
        )
        if not path:
            return
        try:
            df.to_csv(path, index=False)
            messagebox.showinfo("Exported", f"Saved stats CSV for {module} to:\n{path}")
        except Exception as e:
            messagebox.showerror("Export error", f"Failed to export:\n{e}")

    def export_stats_excel(self, module: str):
        df = self._stats_dataframe_from_tree(module)
        if df.empty:
            messagebox.showwarning("Export", "No stats to export.")
            return

        path = filedialog.asksaveasfilename(
            title=f"Save stats Excel ({module})",
            defaultextension=".xlsx",
            filetypes=[("Excel", "*.xlsx")]
        )
        if not path:
            return
        try:
            df.to_excel(path, index=False)
            messagebox.showinfo("Exported", f"Saved stats Excel for {module} to:\n{path}")
        except Exception as e:
            messagebox.showerror("Export error", f"Failed to export:\n{e}")

    def export_stats_pdf(self, module: str):
        df = self._stats_dataframe_from_tree(module)
        if df.empty:
            messagebox.showwarning("Export", "No stats to print/export.")
            return

        path = filedialog.asksaveasfilename(
            title=f"Save stats PDF ({module})",
            defaultextension=".pdf",
            filetypes=[("PDF", "*.pdf")]
        )
        if not path:
            return
        if not path.lower().endswith(".pdf"):
            path = f"{path}.pdf"

        try:
            year_filter = self.stats_year_var.get().strip()
            self._render_table_pdf(df, path, title=f"{module} Overall stats (Year: {year_filter})")
            if sys.platform.startswith("win"):
                if messagebox.askyesno("Print", "Stats PDF saved. Print it now? (Windows only)"):
                    os.startfile(path, "print")
            messagebox.showinfo("Saved", f"Saved stats PDF for {module} to:\n{path}")
        except Exception as e:
            messagebox.showerror("Export error", f"Failed to export stats PDF:\n{e}")

    def _collect_eqa_export_df(self, module: str, year_filter: str) -> pd.DataFrame:
        store_mod = self._store_for_module(module)
        out_rows: List[Dict[str, Any]] = []

        years = sorted(store_mod.keys(), key=lambda y: int(y) if str(y).isdigit() else str(y))
        for y in years:
            if year_filter != "(All)" and str(y) != year_filter:
                continue

            months_obj = store_mod.get(y, {}) or {}
            for m in MONTHS:
                obj = months_obj.get(m, {}) or {}
                if not obj:
                    continue

                for r in (obj.get("rows", []) or []):
                    z = r.get("Z", np.nan)
                    status_txt, _ = z_status(z)
                    out_rows.append({
                        "Module": module,
                        "Year": y,
                        "Month": m,
                        "Analyte": clean_analyte_name(r.get("Analyte", "")),
                        "Unit": str(r.get("Unit", "")).strip(),
                        "Result": r.get("Result", np.nan),
                        "Mean": r.get("Mean", np.nan),
                        "Zscore": r.get("Z", np.nan),
                        "RMZ": r.get("RMZ", np.nan),
                        "Status": status_txt,
                        "Notes": str(r.get("Notes", "") or ""),
                    })

        cols = [
            "Module", "Year", "Month",
            "Analyte", "Unit", "Result", "Mean", "Zscore", "RMZ", "Status", "Notes",
        ]
        return pd.DataFrame(out_rows, columns=cols)

    def export_eqa_data_pdf(self):
        module = self.current_module.get()
        year_filter = self.year_filter_var.get().strip()
        df = self._collect_eqa_export_df(module, year_filter)
        if df.empty:
            messagebox.showwarning("Export", f"No EQA data to export for {module} (Year filter: {year_filter}).")
            return

        pdf_path = filedialog.asksaveasfilename(
            title=f"Export EQA data PDF ({module})",
            defaultextension=".pdf",
            filetypes=[("PDF", "*.pdf")]
        )
        if not pdf_path:
            return

        try:
            if not pdf_path.lower().endswith(".pdf"):
                pdf_path = f"{pdf_path}.pdf"
            self._render_table_pdf(df, pdf_path, title=f"{module} EQA data export (Year: {year_filter})", rows_per_page=24)
            if sys.platform.startswith("win"):
                if messagebox.askyesno("Print", "EQA PDF saved. Print it now? (Windows only)"):
                    os.startfile(pdf_path, "print")
            messagebox.showinfo("Exported", f"Saved EQA data PDF for {module} to:\n{pdf_path}")
        except Exception as e:
            messagebox.showerror("Export error", f"Failed to export EQA data:\n{e}")

    def export_eqa_data_excel(self):
        module = self.current_module.get()
        year_filter = self.year_filter_var.get().strip()
        df = self._collect_eqa_export_df(module, year_filter)
        if df.empty:
            messagebox.showwarning("Export", f"No EQA data to export for {module} (Year filter: {year_filter}).")
            return

        xlsx_path = filedialog.asksaveasfilename(
            title=f"Export EQA data Excel ({module})",
            defaultextension=".xlsx",
            filetypes=[("Excel", "*.xlsx")]
        )
        if not xlsx_path:
            return

        try:
            if not xlsx_path.lower().endswith(".xlsx"):
                xlsx_path = f"{xlsx_path}.xlsx"
            df.to_excel(xlsx_path, index=False)
            messagebox.showinfo("Exported", f"Saved EQA data Excel for {module} to:\n{xlsx_path}")
        except Exception as e:
            messagebox.showerror("Export error", f"Failed to export EQA data:\n{e}")


if __name__ == "__main__":
    app = EQADashboard()
    app.mainloop()
