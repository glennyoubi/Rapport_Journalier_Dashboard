# core/charts.py
from __future__ import annotations
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go


def _is_empty(df: pd.DataFrame | None) -> bool:
    """True si df est None ou vide."""
    return (df is None) or df.empty


def time_status_counts(actions_consistent: pd.DataFrame):
    """Aire empilée : nombre d’actions par statut et par jour (vue consistante)."""
    if _is_empty(actions_consistent):
        return go.Figure()

    df = actions_consistent.copy()
    df["date"] = pd.to_datetime(df["date_rapport"]).dt.date

    agg = (
        df.groupby("date")
        .agg(en_cours=("en_cours", "sum"), reporte=("reporte", "sum"), termine=("termine", "sum"))
        .reset_index()
    )

    fig = px.area(
        agg,
        x="date",
        y=["en_cours", "reporte", "termine"],
        title="Statut des actions en fonction du temps",
    )
    fig.update_layout(legend_title_text="Statut", xaxis_title="", yaxis_title="Nombre d'actions")
    return fig


def latest_status_pie(actions_latest: pd.DataFrame):
    """Camembert des statuts fin de période (snapshot des actions_latest)."""
    if _is_empty(actions_latest):
        return go.Figure()

    s = pd.Series(
        {
            "Terminé": int(actions_latest["termine"].sum()),
            "En cours": int(actions_latest["en_cours"].sum()),
            "Reporté": int(actions_latest["reporte"].sum()),
        }
    )
    df = s.reset_index()
    df.columns = ["statut", "count"]

    fig = px.pie(df, names="statut", values="count", title="Pourcentage d'actions par statut")
    fig.update_traces(textposition="inside", textinfo="percent+label")
    return fig


def daily_stacked_by_metier(actions_consistent: pd.DataFrame, metiers: list[str] | None = None):
    """Barres empilées par jour et par statut, filtrable par métier(s)."""
    if _is_empty(actions_consistent):
        return go.Figure()

    df = actions_consistent.copy()
    if metiers:
        df = df[df["metier"].isin(metiers)]
    if df.empty:
        return go.Figure()

    df["date"] = pd.to_datetime(df["date_rapport"]).dt.date

    melted = pd.melt(
        df[["date", "metier", "en_cours", "reporte", "termine"]],
        id_vars=["date", "metier"],
        var_name="statut",
        value_name="val",
    )
    agg = melted.groupby(["date", "statut"])["val"].sum().reset_index()

    fig = px.bar(agg, x="date", y="val", color="statut", title="Suivi des statuts par jour")
    fig.update_layout(xaxis_title="", yaxis_title="Nombre d'actions")
    return fig


def cumulative_completed(actions_consistent: pd.DataFrame):
    """Courbe du cumul des actions terminées (termine == True) jour après jour."""
    if _is_empty(actions_consistent):
        return go.Figure()

    df = actions_consistent.copy()
    df["date"] = pd.to_datetime(df["date_rapport"]).dt.date
    daily_done = (
        df.groupby("date")["termine"]
        .sum()
        .reset_index()
        .rename(columns={"termine": "done"})
        .sort_values("date")
    )
    daily_done["cumul_done"] = daily_done["done"].cumsum()

    fig = px.line(daily_done, x="date", y="cumul_done", title="Actions terminées (cumul)")
    fig.update_layout(xaxis_title="", yaxis_title="Cumul terminé")
    return fig


def completion_rate(actions_latest: pd.DataFrame) -> float:
    """% d’actions terminées parmi les actions actives (snapshot latest)."""
    if _is_empty(actions_latest):
        return 0.0
    total = len(actions_latest)
    if total == 0:
        return 0.0
    done = int(actions_latest["termine"].sum())
    return (done / total) * 100.0


def actions_daily_counts(actions_consistent: pd.DataFrame):
    """Barres quotidiennes du volume d'actions (toutes) + ligne cumulée."""
    if _is_empty(actions_consistent):
        return go.Figure()

    df = actions_consistent.copy()
    df["date"] = pd.to_datetime(df["date_rapport"]).dt.date

    daily_counts = (
        df.groupby("date")["action_key"]
        .nunique()
        .reset_index()
        .rename(columns={"action_key": "nb_actions"})
        .sort_values("date")
    )
    daily_counts["cumul_actions"] = daily_counts["nb_actions"].cumsum()

    fig = go.Figure()
    fig.add_bar(x=daily_counts["date"], y=daily_counts["nb_actions"], name="Actions du jour", marker_color="#4b9fea")
    fig.add_trace(
        go.Scatter(
            x=daily_counts["date"],
            y=daily_counts["cumul_actions"],
            name="Cumul actions",
            mode="lines+markers",
            line=dict(color="#f28e2b"),
            yaxis="y2",
        )
    )
    fig.update_layout(
        title="Volume quotidien et cumul",
        yaxis_title="Actions du jour",
        yaxis2=dict(title="Cumul", overlaying="y", side="right", showgrid=False),
        xaxis_title="",
        legend_title_text="",
    )
    return fig


def stacked_status_by_dimension(actions_latest: pd.DataFrame, dimension: str):
    """Barres empilées statut latest par dimension (zone / plateforme / métier)."""
    if _is_empty(actions_latest) or dimension not in actions_latest.columns:
        return go.Figure()

    df = actions_latest.copy()
    df["statut"] = df.apply(
        lambda r: "Terminé" if r.get("termine") else ("Reporté" if r.get("reporte") else "En cours"),
        axis=1,
    )
    df["_dim"] = df[dimension].fillna("(Non renseigné)")

    agg = df.groupby(["_dim", "statut"]).size().reset_index(name="count").sort_values("count", ascending=False)

    fig = px.bar(
        agg,
        x="_dim",
        y="count",
        color="statut",
        title=f"Statuts par {dimension}",
    )
    fig.update_layout(
        xaxis_title=dimension,
        yaxis_title="Nombre d'actions",
        xaxis_tickangle=-35,
        margin=dict(b=180),
        height=520,
    )
    return fig


def pareto_causes(actions_latest: pd.DataFrame, cause_col: str = "metier", top_n: int = 15):
    """Pareto sur une colonne cause (ex: metier) avec cumul%."""
    if _is_empty(actions_latest) or cause_col not in actions_latest.columns:
        return go.Figure()

    df = actions_latest.copy()
    df[cause_col] = df[cause_col].fillna("(Non renseigné)")
    agg = (
        df.groupby(cause_col)
        .size()
        .reset_index(name="count")
        .sort_values("count", ascending=False)
        .head(top_n)
    )
    agg["cumul_pct"] = agg["count"].cumsum() / agg["count"].sum() * 100

    fig = go.Figure()
    fig.add_bar(x=agg[cause_col], y=agg["count"], name="Volume")
    fig.add_trace(
        go.Scatter(
            x=agg[cause_col],
            y=agg["cumul_pct"],
            name="Cumul %",
            mode="lines+markers",
            yaxis="y2",
            line=dict(color="#f28e2b"),
        )
    )
    fig.update_layout(
        title="Nombre d'actions par métier",
        yaxis_title="Actions",
        yaxis2=dict(title="Cumul %", overlaying="y", side="right", showgrid=False),
        xaxis_title=cause_col,
    )
    return fig


def heatmap_day_zone(actions_consistent: pd.DataFrame):
    """Heatmap des volumes par jour et par zone."""
    if _is_empty(actions_consistent) or "champ_zone" not in actions_consistent.columns:
        return go.Figure()

    df = actions_consistent.copy()
    df["date"] = pd.to_datetime(df["date_rapport"]).dt.date
    agg = (
        df.groupby(["date", "champ_zone"])["action_key"]
        .nunique()
        .reset_index()
        .rename(columns={"action_key": "nb_actions"})
    )
    if agg.empty:
        return go.Figure()

    fig = px.density_heatmap(
        agg,
        x="date",
        y="champ_zone",
        z="nb_actions",
        title="Heatmap des actions par jour et par zone",
        color_continuous_scale="Blues",
    )
    fig.update_layout(xaxis_title="Date", yaxis_title="Zone")
    return fig


def age_median_open(actions_consistent: pd.DataFrame):
    """Courbe de l'âge médian des actions non terminées (en jours) par date."""
    if _is_empty(actions_consistent):
        return go.Figure()

    df = actions_consistent.copy()
    df["date"] = pd.to_datetime(df["date_rapport"], errors="coerce").dt.date
    df = df.dropna(subset=["date"])
    first_seen = df.groupby("action_key")["date"].min().reset_index().rename(columns={"date": "first_date"})
    merged = df.merge(first_seen, on="action_key", how="left")
    open_df = merged[~merged["termine"]].copy()
    if open_df.empty:
        return go.Figure()
    open_df["date"] = pd.to_datetime(open_df["date"], errors="coerce")
    open_df["first_date"] = pd.to_datetime(open_df["first_date"], errors="coerce")
    open_df = open_df.dropna(subset=["date", "first_date"])
    if open_df.empty:
        return go.Figure()
    open_df["age_jours"] = (open_df["date"] - open_df["first_date"]).dt.days
    med = open_df.groupby("date")["age_jours"].median().reset_index().sort_values("date")
    fig = px.line(med, x="date", y="age_jours", title="Âge médian des actions ouvertes")
    fig.update_layout(xaxis_title="Date", yaxis_title="Jours depuis apparition")
    return fig


def boxplot_time_in_progress(actions_consistent: pd.DataFrame):
    """Boxplot des durées passées en statut en_cours (en jours)."""
    if _is_empty(actions_consistent):
        return go.Figure()

    df = actions_consistent.copy()
    df["date"] = pd.to_datetime(df["date_rapport"]).dt.date

    in_progress = df[df["en_cours"]].groupby("action_key")["date"].agg(["min", "max"]).reset_index()
    if in_progress.empty:
        return go.Figure()
    termini = (
        df[df["termine"]]
        .groupby("action_key")["date"]
        .max()
        .reset_index()
        .rename(columns={"date": "done_date"})
    )
    merged = in_progress.merge(termini, on="action_key", how="left")
    merged["min"] = pd.to_datetime(merged["min"], errors="coerce")
    merged["max"] = pd.to_datetime(merged["max"], errors="coerce")
    merged["done_date"] = pd.to_datetime(merged["done_date"], errors="coerce")
    merged["end_date"] = merged["done_date"].fillna(merged["max"])
    merged = merged.dropna(subset=["min", "end_date"])
    if merged.empty:
        return go.Figure()
    merged["duree_jours"] = (merged["end_date"] - merged["min"]).dt.days.clip(lower=0)

    fig = px.box(merged, y="duree_jours", points="outliers", title="Temps passé en statut 'en cours'")
    fig.update_layout(yaxis_title="Jours", xaxis_title="")
    return fig
