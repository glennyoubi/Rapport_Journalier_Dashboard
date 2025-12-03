# core/processing.py
from __future__ import annotations
import hashlib
import os
import tempfile
from typing import Dict, List, Tuple, Optional

import numpy as np
import pandas as pd
import unicodedata

# --------- Helpers texte / colonnes ---------
def strip_accents_spaces(s: str) -> str:
    if s is None:
        return ""
    if not isinstance(s, str):
        s = str(s)
    s = s.replace("\n", " ").replace("\r", " ")
    s = " ".join(s.split())
    nfkd = unicodedata.normalize("NFKD", s)
    return "".join([c for c in nfkd if not unicodedata.combining(c)])


def norm_cols(cols):
    mapped = []
    for c in cols:
        base = strip_accents_spaces(str(c)).strip()
        low = base.lower()
        if low.startswith("champ / zone") or low == "champ zone":
            mapped.append("champ_zone")
        elif "plateforme" in low or "platerforme" in low or "plateforme/sous-zone" in low:
            mapped.append("plateforme_sous_zone")
        elif low.startswith("n° puits") or "n puits" in low or low.startswith("n°puits"):
            mapped.append("num_puits")
        elif "tag equipement" in low or low == "tag" or "tag équipement" in low:
            mapped.append("tag_equipement")
        elif "sous-equipement" in low or "sous equipement" in low or "sous- equipement" in low:
            mapped.append("sous_equipement")
        elif low == "indisponible":
            mapped.append("indisponible")
        elif low in ("metier", "métier"):
            mapped.append("metier")
        elif "travail effectue" in low and "commentaires" in low:
            mapped.append("travaux_commentaires")
        elif low.startswith("termine") or "terminé" in low:
            mapped.append("termine")
        elif low.startswith("en cours"):
            mapped.append("en_cours")
        elif low.startswith("report") or low.startswith(" reporte"):
            mapped.append("reporte")
        else:
            mapped.append(low)
    return mapped


def to_bool(col: pd.Series) -> pd.Series:
    if col.dtype == bool:
        return col
    tmp = col.astype(str).str.strip().str.lower().isin(
        ["1", "true", "vrai", "oui", "yes", "y", "t", "x"]
    )
    return tmp.astype("boolean").fillna(False).astype(bool)


def sha1_bytes(b: Optional[bytes]) -> Optional[str]:
    if b is None:
        return None
    h = hashlib.sha1()
    h.update(b)
    return h.hexdigest()


def _strip_col(s: pd.Series) -> pd.Series:
    return s.astype(str).map(lambda x: strip_accents_spaces(x).upper())


def _action_key(row) -> str:
    parts = [
        strip_accents_spaces(str(row.get("champ_zone", ""))).lower(),
        strip_accents_spaces(str(row.get("plateforme_sous_zone", ""))).lower(),
        strip_accents_spaces(str(row.get("num_puits", ""))).lower(),
        strip_accents_spaces(str(row.get("tag_equipement", ""))).lower(),
        strip_accents_spaces(str(row.get("sous_equipement", ""))).lower(),
        strip_accents_spaces(str(row.get("metier", ""))).lower(),
        strip_accents_spaces(str(row.get("travaux_commentaires", ""))).lower(),
    ]
    payload = "||".join(parts)
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()


# --------- Lecture Excel & nettoyage ---------
def excel_sheet_names_from_bytes(file_bytes: bytes) -> List[str]:
    with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as tmp:
        tmp.write(file_bytes)
        tmp_path = tmp.name
    try:
        xls = pd.ExcelFile(tmp_path)
        return xls.sheet_names
    finally:
        try:
            os.remove(tmp_path)
        except Exception:
            pass


def _get_date_from_raw(df_raw: pd.DataFrame) -> pd.Timestamp:
    """
    Essaie de lire la date de rapport en (row=2, col=8). Retourne NaT si absent.
    """
    try:
        date_cell = df_raw.iloc[2, 8]
        date = pd.to_datetime(date_cell, errors="coerce")
        return pd.to_datetime(date.date())
    except Exception:
        return pd.NaT


def _get_prevision_index(df: pd.DataFrame) -> int | bool:
    if "champ_zone" not in df.columns:
        return False
    mask = _strip_col(df["champ_zone"]) == "PREVISION"
    idx = df.index[mask]
    return int(idx[0]) if len(idx) else False


def _clean_week_sheets_from_bytes(file_bytes: bytes, start_sheet: int, end_sheet: int) -> Tuple[List[pd.DataFrame], List[pd.Timestamp]]:
    with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as tmp:
        tmp.write(file_bytes)
        tmp_path = tmp.name
    try:
        xls = pd.ExcelFile(tmp_path)
        sheet_names = xls.sheet_names
        total_sheets = len(sheet_names)
        if start_sheet < 1 or end_sheet > total_sheets:
            raise ValueError(f"Plage de feuilles invalide ({start_sheet}-{end_sheet}) pour {total_sheets} feuilles disponibles.")

        final_dfs, dates = [], []

        for i in range(start_sheet - 1, end_sheet):
            sheet_name = sheet_names[i]
            df_raw = xls.parse(sheet_name, header=None)

            dt_report = _get_date_from_raw(df_raw)
            dates.append(dt_report)

            header = df_raw.iloc[4].values[:df_raw.shape[1]]
            df = df_raw.copy()
            df.columns = header
            df = df.iloc[5:].reset_index(drop=True)

            df.columns = norm_cols(df.columns)

            if "champ_zone" not in df.columns:
                continue
            df = df[df["champ_zone"].notna()].reset_index(drop=True)

            idx_prev = _get_prevision_index(df)
            if idx_prev is not False:
                df = df.iloc[:idx_prev].reset_index(drop=True)

            for c in ["termine", "en_cours", "reporte", "indisponible"]:
                if c in df.columns:
                    df[c] = to_bool(df[c])
                else:
                    df[c] = False

            df["date_rapport"] = dt_report
            df["feuille"] = sheet_name

            for c in [
                "champ_zone",
                "plateforme_sous_zone",
                "num_puits",
                "tag_equipement",
                "sous_equipement",
                "metier",
                "travaux_commentaires",
            ]:
                if c in df.columns:
                    df[c] = (
                        df[c].astype(str)
                        .map(lambda x: "" if x == "nan" else x)
                        .map(strip_accents_spaces)
                    )

            df["action_key"] = df.apply(_action_key, axis=1)

            useful = [
                "champ_zone",
                "plateforme_sous_zone",
                "num_puits",
                "tag_equipement",
                "sous_equipement",
                "metier",
                "travaux_commentaires",
            ]
            useful = [c for c in useful if c in df.columns]
            non_empty = df[useful].apply(lambda r: any([str(x).strip() for x in r]), axis=1)
            df = df[non_empty].reset_index(drop=True)

            final_dfs.append(df)

        return final_dfs, dates
    finally:
        try:
            os.remove(tmp_path)
        except Exception:
            pass


# --------- Construction des tables en mémoire ---------
def build_week_tables_from_excel_bytes(
    file_bytes: bytes, start_sheet: int, end_sheet: int
) -> Dict[str, pd.DataFrame]:
    daily_frames, _ = _clean_week_sheets_from_bytes(file_bytes, start_sheet, end_sheet)
    if not daily_frames:
        raise ValueError("No usable sheets in the selected range.")

    daily = pd.concat(daily_frames, ignore_index=True)

    base_cols = [
        "action_key",
        "date_rapport",
        "champ_zone",
        "plateforme_sous_zone",
        "num_puits",
        "tag_equipement",
        "sous_equipement",
        "metier",
        "travaux_commentaires",
        "termine",
        "en_cours",
        "reporte",
        "indisponible",
        "feuille",
    ]
    for c in base_cols:
        if c not in daily.columns:
            daily[c] = np.nan if c not in ["termine", "en_cours", "reporte", "indisponible"] else False
    daily = daily[base_cols].copy()

    if daily["date_rapport"].isna().all():
        raise ValueError("Aucune date trouvée dans les feuilles sélectionnées (colonne date_rapport vide).")

    daily_sorted = daily.sort_values(["action_key", "date_rapport"]).reset_index(drop=True)

    def _rollup(group: pd.DataFrame) -> pd.DataFrame:
        g = group.copy().sort_values("date_rapport")
        for _c in ["termine", "en_cours", "reporte"]:
            if _c in g.columns:
                g[_c] = g[_c].astype("boolean").fillna(False).astype(bool)
            else:
                g[_c] = False
        g["en_cours_ff"] = g["en_cours"].astype("boolean").ffill().fillna(False).astype(bool)
        g["reporte_ff"] = g["reporte"].astype("boolean").ffill().fillna(False).astype(bool)
        g["termine_seen"] = g["termine"].astype("boolean").fillna(False).astype(bool).cummax()
        g["termine_final"] = g["termine_seen"]
        g["en_cours_final"] = np.where(g["termine_final"], False, g["en_cours_ff"]).astype(bool)
        g["reporte_final"] = np.where(g["termine_final"], False, g["reporte_ff"]).astype(bool)
        return g

    try:
        rolled = daily_sorted.groupby("action_key", group_keys=False, include_groups=False).apply(_rollup)
    except TypeError:
        rolled = daily_sorted.groupby("action_key", group_keys=False).apply(_rollup)

    def _transitions(sub: pd.DataFrame) -> pd.DataFrame:
        g = sub.sort_values("date_rapport").copy()
        for col in ["termine_final", "en_cours_final", "reporte_final"]:
            if col not in g.columns:
                g[col] = False
            else:
                g[col] = g[col].astype("boolean").fillna(False).astype(bool)
        for col in ["termine_final", "en_cours_final", "reporte_final"]:
            base = g[col].astype("boolean")
            prev = base.shift(1).fillna(False)
            g[col + "_prev"] = prev.astype(bool)
            g[col + "_changed"] = g[col].astype(bool) != g[col + "_prev"]
        changed = g[g["termine_final_changed"] | g["en_cours_final_changed"] | g["reporte_final_changed"]].copy()
        if changed.empty:
            return pd.DataFrame(columns=["action_key", "date_rapport", "termine", "en_cours", "reporte", "change_desc"])

        def _desc(row):
            msgs = []
            if row["termine_final_changed"]:
                msgs.append(f"termine: {bool(row['termine_final_prev'])} -> {bool(row['termine_final'])}")
            if row["en_cours_final_changed"]:
                msgs.append(f"en_cours: {bool(row['en_cours_final_prev'])} -> {bool(row['en_cours_final'])}")
            if row["reporte_final_changed"]:
                msgs.append(f"reporte: {bool(row['reporte_final_prev'])} -> {bool(row['reporte_final'])}")
            return "; ".join(msgs)

        changed["change_desc"] = changed.apply(_desc, axis=1)
        return changed[
            [
                "action_key",
                "date_rapport",
                "termine_final",
                "en_cours_final",
                "reporte_final",
                "change_desc",
            ]
        ].rename(columns={"termine_final": "termine", "en_cours_final": "en_cours", "reporte_final": "reporte"})

    try:
        transitions = (
            rolled.groupby("action_key", group_keys=False, include_groups=False)
            .apply(_transitions)
            .reset_index(drop=True)
        )
    except TypeError:
        need = ["action_key", "date_rapport", "termine_final", "en_cours_final", "reporte_final"]
        try:
            transitions = (
                rolled[need]
                .groupby("action_key", group_keys=False, include_groups=False)
                .apply(_transitions)
                .reset_index(drop=True)
            )
        except TypeError:
            transitions = (
                rolled[need]
                .groupby("action_key", group_keys=False)
                .apply(_transitions)
                .reset_index(drop=True)
            )

    last_idx = (
        rolled.sort_values(["action_key", "date_rapport"])
        .groupby("action_key", as_index=False)
        .tail(1)
        .index
    )
    latest = rolled.loc[
        last_idx,
        [
            "action_key",
            "date_rapport",
            "champ_zone",
            "plateforme_sous_zone",
            "num_puits",
            "tag_equipement",
            "sous_equipement",
            "metier",
            "travaux_commentaires",
            "termine_final",
            "en_cours_final",
            "reporte_final",
        ],
    ].copy().rename(columns={"termine_final": "termine", "en_cours_final": "en_cours", "reporte_final": "reporte"})

    ended_actions = latest[latest["termine"]].copy()
    running_actions = latest[(~latest["termine"]) & (latest["en_cours"])].copy()
    postponed_actions = latest[(~latest["termine"]) & (latest["reporte"])].copy()
    equipment_downtime = daily[daily["indisponible"]].copy()

    consistent_view = rolled.copy()
    consistent_view = consistent_view.drop(columns=["termine", "en_cours", "reporte"], errors="ignore")
    consistent_view = consistent_view.rename(
        columns={"termine_final": "termine", "en_cours_final": "en_cours", "reporte_final": "reporte"}
    )
    consistent_cols = [
        "action_key",
        "date_rapport",
        "champ_zone",
        "plateforme_sous_zone",
        "num_puits",
        "tag_equipement",
        "sous_equipement",
        "metier",
        "travaux_commentaires",
        "termine",
        "en_cours",
        "reporte",
        "indisponible",
        "feuille",
    ]
    consistent_cols = [c for c in consistent_cols if c in consistent_view.columns]
    actions_consistent = consistent_view[consistent_cols].copy()

    return {
        "actions_daily": daily,
        "actions_consistent": actions_consistent,
        "actions_latest": latest,
        "ended_actions": ended_actions,
        "running_actions": running_actions,
        "postponed_actions": postponed_actions,
        "equipment_downtime": equipment_downtime,
        "transitions": transitions,
    }
