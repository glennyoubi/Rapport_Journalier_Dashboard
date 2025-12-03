# core/filters.py
from __future__ import annotations
import datetime as dt
from typing import Dict, Tuple
import pandas as pd

def date_bounds_from_tables(tables: Dict[str, pd.DataFrame]) -> Tuple[pd.Timestamp, pd.Timestamp]:
    df = tables.get("actions_daily")
    if df is None or df.empty or "date_rapport" not in df.columns:
        today = pd.Timestamp(dt.date.today())
        return today, today
    dates = pd.to_datetime(df["date_rapport"], errors="coerce").dropna()
    if dates.empty:
        today = pd.Timestamp(dt.date.today())
        return today, today
    dmin = dates.min().normalize()
    dmax = dates.max().normalize()
    return dmin, dmax

def clamp_date(value: dt.date, lo: dt.date, hi: dt.date) -> dt.date:
    if value < lo: return lo
    if value > hi: return hi
    return value

def normalize_date_filters(ss, tables: Dict[str, pd.DataFrame]):
    dmin_all_ts, dmax_all_ts = date_bounds_from_tables(tables)
    lo = dmin_all_ts.date()
    hi = dmax_all_ts.date()

    if ss.filter_dmin is None: ss.filter_dmin = lo
    if ss.filter_dmax is None: ss.filter_dmax = hi

    ss.filter_dmin = clamp_date(ss.filter_dmin, lo, hi)
    ss.filter_dmax = clamp_date(ss.filter_dmax, lo, hi)

    if ss.filter_dmin > ss.filter_dmax:
        ss.filter_dmin, ss.filter_dmax = ss.filter_dmax, ss.filter_dmin

    return lo, hi, ss.filter_dmin, ss.filter_dmax

def filter_tables_by_dates(tables: Dict[str, pd.DataFrame], dmin: pd.Timestamp, dmax: pd.Timestamp):
    out = {}
    for name, df in tables.items():
        if "date_rapport" in df.columns and not df.empty:
            dcol = pd.to_datetime(df["date_rapport"]).dt.normalize()
            out[name] = df[(dcol >= dmin) & (dcol <= dmax)].copy()
        else:
            out[name] = df.copy()
    return out

def recount(filtered_tables: Dict[str, pd.DataFrame]):
    return {k: len(v) for k, v in filtered_tables.items()}
