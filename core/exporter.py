# core/exporter.py
from __future__ import annotations
import io
import json
from pathlib import Path
from typing import Dict
import re
import pandas as pd


def sanitize_sheet_name(name: str) -> str:
    name = re.sub(r'[\/\\\?\*\[\]:]', '_', name)
    return (name or "Sheet")[:31]


def sanitize_table_name(name: str) -> str:
    name = re.sub(r'[^0-9A-Za-z_]', '_', name)
    if not re.match(r'^[A-Za-z_]', name):
        name = f"T_{name}"
    return name


def compute_col_widths(df: pd.DataFrame, max_width: int = 50) -> dict:
    widths = {}
    for i, col in enumerate(df.columns):
        header_len = len(str(col))
        data_len = 0 if df.empty else int(df[col].astype(str).map(len).max())
        widths[i] = min(max(header_len, data_len) + 2, max_width)
    return widths


def export_tables_to_excel_bytes(tables: Dict[str, pd.DataFrame]) -> bytes:
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="xlsxwriter") as writer:
        workbook = writer.book
        date_fmt = workbook.add_format({"num_format": "yyyy-mm-dd"})
        wrap_fmt = workbook.add_format({"text_wrap": True})
        header_fmt = workbook.add_format({"bold": True})

        for t, df in tables.items():
            sheet_name = sanitize_sheet_name(t)
            df.to_excel(writer, sheet_name=sheet_name, index=False, startrow=0, startcol=0)
            ws = writer.sheets[sheet_name]

            if len(df.columns) == 0:
                df = pd.DataFrame(columns=["(vide)"])
            col_widths = compute_col_widths(df)
            for idx, w in col_widths.items():
                ws.set_column(idx, idx, w, wrap_fmt)

            if not df.empty:
                for c, col in enumerate(df.columns):
                    ws.write(0, c, col, header_fmt)

            if "date_rapport" in df.columns and not df.empty:
                date_col_idx = list(df.columns).index("date_rapport")
                ws.set_column(date_col_idx, date_col_idx, col_widths.get(date_col_idx, 12), date_fmt)

            nrows = max(len(df), 1)
            ncols = max(len(df.columns), 1)
            last_row = nrows
            last_col = ncols - 1
            table_name = sanitize_table_name(f"tbl_{t}")
            columns_spec = [{"header": col} for col in (df.columns if len(df.columns) else ["Colonne"])]

            ws.add_table(
                0,
                0,
                last_row,
                last_col,
                {
                    "name": table_name,
                    "columns": columns_spec,
                    "style": "Table Style Medium 9",
                    "autofilter": True,
                },
            )
            ws.freeze_panes(1, 0)

    buf.seek(0)
    return buf.getvalue()


def save_report_bundle(out_dir: Path, tables: Dict[str, pd.DataFrame], excel_bytes: bytes, meta: dict) -> Path:
    """
    Sauvegarde automatique :
      - Excel consolidé
      - Manifest JSON (métadonnées, tailles)
      - CSVs (facultatif : ici on sauve Top 3 tables clés)
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    excel_path = out_dir / "Rapport_Hebdo_Resume.xlsx"
    excel_path.write_bytes(excel_bytes)

    manifest = {
        "meta": meta,
        "counts": {k: len(v) for k, v in tables.items()},
    }
    (out_dir / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")

    # Exemples d’exports CSV légers (modifiable à volonté)
    for name in ["actions_latest", "equipment_downtime", "transitions"]:
        if name in tables:
            tables[name].to_csv(out_dir / f"{name}.csv", index=False, encoding="utf-8", sep=";")

    return excel_path
