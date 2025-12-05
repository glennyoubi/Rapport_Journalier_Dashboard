# app.py
import io
import json
import zipfile
import datetime as dt
from collections import OrderedDict

import pandas as pd
import streamlit as st
from core.processing import (
    build_week_tables_from_excel_bytes,
    excel_sheet_names_from_bytes,
    sha1_bytes,
)
from core.exporter import export_tables_to_excel_bytes
from core.filters import (
    normalize_date_filters,
    filter_tables_by_dates,
    recount,
)
from core.charts import (
    time_status_counts,
    latest_status_pie,
    daily_stacked_by_metier,
    cumulative_completed,
    completion_rate,
    stacked_status_by_dimension,
    actions_daily_counts,
    pareto_causes,
)

st.set_page_config(page_title="Résumé des rapports journaliers", layout="wide")


# ---------------- Session state ----------------
def ensure_ss():
    SS = st.session_state
    SS.setdefault("file_bytes", None)
    SS.setdefault("uploaded_hash", None)
    SS.setdefault("start_sheet", 1)
    SS.setdefault("end_sheet_opt", "")
    SS.setdefault("analyst_mode", False)

    SS.setdefault("tables_full", None)
    SS.setdefault("tables_view", None)
    SS.setdefault("counts_view", None)
    SS.setdefault("last_error", None)

    SS.setdefault("filter_dmin", None)
    SS.setdefault("filter_dmax", None)
    SS.setdefault("filter_metier", [])
    SS.setdefault("filter_zone", [])
    SS.setdefault("filter_plateforme", [])
    SS.setdefault("filter_tag_select", [])
    SS.setdefault("filter_tag_pattern", "")
    SS.setdefault("filter_text", "")

    SS.setdefault("last_run_file_hash", None)
    SS.setdefault("last_run_start_sheet", None)
    SS.setdefault("last_run_end_sheet", None)

    SS.setdefault("reports_dir", "reports")
    return SS


SS = ensure_ss()

# ---------------- Header ----------------
st.title("📈 Résumé Hebdomadaire")
msg = st.empty()

# Noms d'affichage pour les tables (onglets + Excel)
TABLE_DISPLAY_NAMES = {
    "running_actions": "Actions en cours",
    "ended_actions": "Actions terminées",
    "postponed_actions": "Actions reportées",
    "equipment_downtime": "Indisponibilités équipements",
    "actions_latest": "Statut des actions (statut à la date de fin)",
    "actions_consistent": "Actions (historique consolidé)",
    "transitions": "Transitions de statut",
    "actions_daily": "Actions (journalier brut)",
}

# ---------------- Sidebar ----------------
with st.sidebar:
    st.header(" Paramètres")
    up = st.file_uploader("Charger le fichier hebdo (.xlsx)", type=["xlsx"], key="uploader")
    if up is not None:
        new_bytes = up.getvalue()
        new_hash = sha1_bytes(new_bytes)
        if new_hash != SS.uploaded_hash:
            # Nouveau fichier charge : on remet l'etat a plat pour eviter les erreurs transitoires
            st.cache_data.clear()
            SS.uploaded_hash = new_hash
            SS.tables_full = None
            SS.tables_view = None
            SS.counts_view = None
            SS.last_error = None
            SS.filter_dmin = None
            SS.filter_dmax = None
        SS.file_bytes = new_bytes

    SS.start_sheet = st.number_input("Feuille de début (1-based)", min_value=1, value=int(SS.start_sheet), step=1)
    SS.end_sheet_opt = st.text_input("Feuille de fin (vide = dernière)", value=SS.end_sheet_opt)
    SS.analyst_mode = st.toggle("Mode analyste (tables avancées)", value=SS.analyst_mode)

    st.markdown("---")
    run_btn = st.button(" Lancer / Mettre à jour", type="primary")
    st.markdown("---")


# ---------------- Utils ----------------
@st.cache_data(show_spinner=False)
def detect_last_sheet(file_bytes: bytes) -> int:
    return len(excel_sheet_names_from_bytes(file_bytes))


def needs_update(file_bytes: bytes | None, start_sheet: int, end_sheet: int) -> bool:
    if file_bytes is None or SS.tables_full is None:
        return False
    curr = sha1_bytes(file_bytes)
    return (
        SS.last_run_file_hash != curr
        or SS.last_run_start_sheet != int(start_sheet)
        or SS.last_run_end_sheet != int(end_sheet)
    )


def resolve_range_or_warn():
    if SS.file_bytes is None:
        msg.warning("Merci duploader un fichier Excel.")
        return None
    try:
        if not SS.end_sheet_opt:
            end_sheet = detect_last_sheet(SS.file_bytes)
        else:
            end_sheet = int(SS.end_sheet_opt)
        start_sheet = int(SS.start_sheet)

        total_sheets = detect_last_sheet(SS.file_bytes)
        if start_sheet < 1 or end_sheet > total_sheets:
            st.error(f"Plage invalide : le fichier contient {total_sheets} feuilles.")
            return None
        if end_sheet < start_sheet:
            st.error("La feuille de fin doit être  la feuille de début.")
            return None
        return start_sheet, end_sheet
    except ValueError:
        st.error("La feuille de fin doit être un entier si renseignée.")
        return None


def warn_if_missing_dates(tables: dict):
    df = tables.get("actions_daily")
    if df is None or df.empty or "date_rapport" not in df.columns:
        return
    missing = int(df["date_rapport"].isna().sum())
    if missing:
        st.warning(
            f"{missing} lignes sans date détectées (colonne date_rapport). "
            "Elles sont ignorées dans les filtres/graphes. Vérifiez les fichiers sources.",
            icon="",
        )


def run_pipeline_excel_only(file_bytes: bytes, start_sheet: int, end_sheet: int):
    tables = build_week_tables_from_excel_bytes(file_bytes, start_sheet, end_sheet)
    SS.tables_full = tables
    SS.last_run_file_hash = sha1_bytes(file_bytes)
    SS.last_run_start_sheet = int(start_sheet)
    SS.last_run_end_sheet = int(end_sheet)
    lo, hi, fmin, fmax = normalize_date_filters(SS, SS.tables_full)
    SS.tables_view = filter_tables_by_dates(SS.tables_full, pd.Timestamp(fmin), pd.Timestamp(fmax))
    SS.counts_view = recount(SS.tables_view)


# ---------------- Run ----------------
if run_btn:
    SS.last_error = None
    rng = resolve_range_or_warn()
    if rng:
        s, e = rng
        try:
            with st.spinner("Traitement en cours"):
                run_pipeline_excel_only(SS.file_bytes, s, e)
            msg.success("Analyse mise à jour ✅")
        except Exception as ex:
            SS.last_error = str(ex)
            st.exception(ex)

# ---------------- Render ----------------
if SS.last_error:
    st.error(SS.last_error)

if SS.tables_full:
    rng = resolve_range_or_warn()
    if rng:
        s, e = rng
        if needs_update(SS.file_bytes, s, e):
            st.warning(
                "Les paramètres de feuilles ont changé depuis la dernière exécution. "
                "Cliquez **Lancer / Mettre à jour**.",
                icon="",
            )

    warn_if_missing_dates(SS.tables_full)

    # ====== Filtres ======
    st.subheader("🎛️ Filtres")
    lo, hi, fmin, fmax = normalize_date_filters(SS, SS.tables_full)

    ac = SS.tables_full["actions_consistent"]
    metiers_all = sorted([x for x in ac["metier"].dropna().unique() if x])
    zones_all = sorted([x for x in ac["champ_zone"].dropna().unique() if x])
    plats_all = sorted([x for x in ac["plateforme_sous_zone"].dropna().unique() if x])
    tags_all = sorted([x for x in ac["tag_equipement"].dropna().unique() if str(x).strip()])

    row1 = st.columns([1, 1, 1, 1])
    with row1[0]:
        new_min = st.date_input("Date début", value=fmin, min_value=lo, max_value=hi, format="YYYY-MM-DD")
    with row1[1]:
        new_max = st.date_input("Date fin", value=fmax, min_value=lo, max_value=hi, format="YYYY-MM-DD")
    with row1[2]:
        SS.filter_metier = st.multiselect("Métier", metiers_all, default=SS.filter_metier)
    with row1[3]:
        SS.filter_zone = st.multiselect("Champ / Zone", zones_all, default=SS.filter_zone)

    row2 = st.columns([1, 1, 1, 1])
    with row2[0]:
        SS.filter_plateforme = st.multiselect("Plateforme / Sous-zone", plats_all, default=SS.filter_plateforme)
    with row2[1]:
        SS.filter_tag_select = st.multiselect("TAG (choix multiples)", options=tags_all, default=SS.filter_tag_select)
    with row2[2]:
        SS.filter_tag_pattern = st.text_input("TAG (contient)", value=SS.filter_tag_pattern, placeholder="ex: P-")
    with row2[3]:
        SS.filter_text = st.text_input("Texte (travaux/commentaires)", value=SS.filter_text, placeholder="mots clés")

    apply_filters = st.button("Appliquer filtres", use_container_width=True)

    if apply_filters:
        from core.filters import clamp_date

        new_min = clamp_date(new_min, lo, hi)
        new_max = clamp_date(new_max, lo, hi)
        if new_min > new_max:
            new_min, new_max = new_max, new_min
        SS.filter_dmin, SS.filter_dmax = new_min, new_max

        SS.tables_view = filter_tables_by_dates(SS.tables_full, pd.Timestamp(new_min), pd.Timestamp(new_max))

        def apply_entity_filters(df: pd.DataFrame) -> pd.DataFrame:
            out = df.copy()
            if SS.filter_metier:
                out = out[out["metier"].isin(SS.filter_metier)]
            if SS.filter_zone:
                out = out[out["champ_zone"].isin(SS.filter_zone)]
            if SS.filter_plateforme:
                out = out[out["plateforme_sous_zone"].isin(SS.filter_plateforme)]
            if SS.filter_tag_select:
                out = out[out["tag_equipement"].astype(str).isin(SS.filter_tag_select)]
            if SS.filter_tag_pattern:
                out = out[out["tag_equipement"].astype(str).str.contains(SS.filter_tag_pattern, case=False, na=False)]
            if SS.filter_text:
                txt = SS.filter_text.lower()
                out = out[out["travaux_commentaires"].astype(str).str.lower().str.contains(txt, na=False)]
            return out

        SS.tables_view = {
            name: apply_entity_filters(df) if "travaux_commentaires" in df.columns else df
            for name, df in SS.tables_view.items()
        }
        SS.counts_view = recount(SS.tables_view)
        st.toast("Filtres appliqués")

    # ====== KPIs ======
    st.subheader("📊 Indicateurs")
    tv = SS.tables_view or SS.tables_full
    latest = tv.get("actions_latest")
    if latest is not None and not latest.empty:
        done_cnt = int(latest["termine"].sum())
        running_cnt = int(latest["en_cours"].sum())
        postponed_cnt = int(latest["reporte"].sum())
    else:
        done_cnt = running_cnt = postponed_cnt = 0
    rate = completion_rate(latest) if latest is not None else 0.0

    a, b, c, d = st.columns(4)
    a.metric("Actions terminées (date de fin)", f"{done_cnt:,}".replace(",", " "))
    b.metric("Actions en cours (date de fin)", f"{running_cnt:,}".replace(",", " "))
    c.metric("Actions reportées (date de fin)", f"{postponed_cnt:,}".replace(",", " "))
    d.metric("Taux de complétion", f"{rate:.1f}%")

    st.divider()

    # ====== Graphiques ======
    st.subheader("📉 Visualisations")

    g1, g2 = st.columns(2)
    with g1:
        st.plotly_chart(time_status_counts(tv["actions_consistent"]), use_container_width=True)
    with g2:
        st.plotly_chart(latest_status_pie(tv["actions_latest"]), use_container_width=True)

    g3, g4 = st.columns(2)
    with g3:
        metiers_sel = SS.filter_metier if SS.filter_metier else None
        st.plotly_chart(daily_stacked_by_metier(tv["actions_consistent"], metiers_sel), use_container_width=True)
    with g4:
        st.plotly_chart(actions_daily_counts(tv["actions_consistent"]), use_container_width=True)

    g5 = st.columns(1)[0]
    with g5:
        st.plotly_chart(cumulative_completed(tv["actions_consistent"]), use_container_width=True)

    st.plotly_chart(stacked_status_by_dimension(tv["actions_latest"], "champ_zone"), use_container_width=True)
    st.plotly_chart(pareto_causes(tv["actions_latest"]), use_container_width=True)

    st.divider()

    # ====== Indisponibilités & TAGs ouverts ======
    st.subheader("🚧 Indisponibilités équipements")
    downtime_df = tv.get("equipment_downtime")
    if downtime_df is not None and not downtime_df.empty:
        dow_agg = (
            downtime_df.assign(date=pd.to_datetime(downtime_df["date_rapport"]).dt.date)
            .groupby("tag_equipement")
            .agg(
                debut=("date", "min"),
                fin=("date", "max"),
                jours_indispo=("date", "nunique"),
                occurrences=("date", "count"),
            )
            .reset_index()
            .sort_values("jours_indispo", ascending=False)
        )
        dow_view = dow_agg.drop(columns=["occurrences"], errors="ignore")
        st.dataframe(dow_view, use_container_width=True, hide_index=True)
    else:
        st.caption("Aucune indisponibilité détectée.")

    st.subheader("🏷️ Nombre d'actions par équipements")
    latest_df = tv.get("actions_latest")
    if latest_df is not None and not latest_df.empty:
        open_df = latest_df[(~latest_df["termine"]) & (latest_df["en_cours"] | latest_df["reporte"])]
        open_df = open_df[open_df["tag_equipement"].astype(str).str.strip() != ""]
        if not open_df.empty:
            open_tags = (
                open_df.groupby("tag_equipement")
                .size()
                .reset_index(name="actions_ouvertes")
                .sort_values("actions_ouvertes", ascending=False)
            )
            st.dataframe(open_tags, use_container_width=True, hide_index=True)
        else:
            st.caption("Aucune action ouverte avec TAG renseigné.")
    else:
        st.caption("Aucune action ouverte.")

    st.subheader("🔎 Top risques (zones & équipements)")
    if latest_df is not None and not latest_df.empty:
        open_df = latest_df[(~latest_df["termine"]) & (latest_df["en_cours"] | latest_df["reporte"])]
        zone_count = (
            open_df.groupby("champ_zone")
            .size()
            .reset_index(name="actions_ouvertes")
            .sort_values("actions_ouvertes", ascending=False)
        )
        tag_count = (
            open_df[open_df["tag_equipement"].astype(str).str.strip() != ""]
            .groupby("tag_equipement")
            .size()
            .reset_index(name="actions_ouvertes")
            .sort_values("actions_ouvertes", ascending=False)
        )
        c1, c2 = st.columns(2)
        with c1:
            if not zone_count.empty:
                top3 = zone_count.head(3)
                st.write("Zones à risque (top 3)")
                st.dataframe(top3, use_container_width=True, hide_index=True)
            else:
                st.caption("Pas de zone à risque.")
        with c2:
            if not tag_count.empty:
                top3 = tag_count.head(3)
                st.write("quipements à risque (top 3)")
                st.dataframe(top3, use_container_width=True, hide_index=True)
            else:
                st.caption("Pas de TAG à risque.")
    else:
        st.caption("Pas de données disponibles pour les risques.")

    st.subheader("💾 Export & Sauvegarde")
    exp_col1, exp_col2 = st.columns(2)

    with exp_col1:
        st.markdown("**Excel consolidé**")
        try:
            excel_tables_named = OrderedDict(
                (TABLE_DISPLAY_NAMES.get(k, k), v) for k, v in tv.items()
            )
            excel_bytes = export_tables_to_excel_bytes(excel_tables_named)
            st.download_button(
                "Télécharger (filtres appliqués)",
                data=excel_bytes,
                file_name="Rapport_Hebdo_Resume.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True,
            )
        except Exception as ex:
            st.error(f"Export Excel échoué : {ex}")

    with exp_col2:
        st.markdown("💡 Bundle ZIP (Excel complet + manifest + CSV clés)")
        if SS.tables_full:
            ts = dt.datetime.now().strftime("%Y%m%d_%H%M%S")

            excel_full_named = OrderedDict(
                (TABLE_DISPLAY_NAMES.get(k, k), v) for k, v in SS.tables_full.items()
            )
            excel_bytes_all = export_tables_to_excel_bytes(excel_full_named)

            meta = {
                "start_sheet": SS.last_run_start_sheet,
                "end_sheet": SS.last_run_end_sheet,
                "file_hash": SS.last_run_file_hash,
                "generated_at": ts,
                "filters": {
                    "date_min": str(SS.filter_dmin),
                    "date_max": str(SS.filter_dmax),
                    "metier": SS.filter_metier,
                    "zone": SS.filter_zone,
                    "plateforme": SS.filter_plateforme,
                    "tag_select": SS.filter_tag_select,
                    "tag_pattern": SS.filter_tag_pattern,
                    "text_like": SS.filter_text,
                },
            }

            bundle_buf = io.BytesIO()
            manifest = {
                "meta": meta,
                "counts": {k: len(v) for k, v in SS.tables_full.items()},
            }

            with zipfile.ZipFile(bundle_buf, "w", zipfile.ZIP_DEFLATED) as zf:
                zf.writestr("Rapport_Hebdo_Resume.xlsx", excel_bytes_all)
                zf.writestr("manifest.json", json.dumps(manifest, ensure_ascii=False, indent=2))
                for name in ["actions_latest", "equipment_downtime", "transitions"]:
                    if name in SS.tables_full:
                        csv_buf = io.StringIO()
                        SS.tables_full[name].to_csv(csv_buf, index=False, sep=";")
                        zf.writestr(f"{name}.csv", csv_buf.getvalue())

            bundle_buf.seek(0)

            st.download_button(
                "📦 Télécharger le bundle (ZIP)",
                data=bundle_buf.getvalue(),
                file_name=f"rapport_bundle_{ts}.zip",
                mime="application/zip",
                use_container_width=True,
            )
            st.caption("Généré en mémoire : aucune sauvegarde côté serveur.")
        else:
            st.caption("Le bundle complet sera disponible après la première exécution du traitement.")

    st.divider()

    # ====== Tables ======
    st.subheader("📑 Tables")

    table_entries = [
        ("running_actions", "Actions en cours"),
        ("ended_actions", "Actions terminées"),
        ("postponed_actions", "Actions reportées"),
        ("equipment_downtime", "Indisponibilités équipements"),
        ("actions_latest", "Statut des actions (statut à la date de fin)"),
    ]
    if SS.analyst_mode:
        table_entries.extend([
            ("actions_consistent", "Actions (historique consolidé)"),
            ("transitions", "Transitions de statut"),
        ])

    tabs = st.tabs([label for _, label in table_entries])

    for tab_obj, (name, label) in zip(tabs, table_entries):
        with tab_obj:
            if name not in tv:
                st.warning(f"La table '{label}' est absente.")
                continue

            df = tv[name]
            st.write(f"**{label}**  {len(df):,} lignes".replace(",", " "))
            df_view = df.drop(columns=["action_key"], errors="ignore")
            st.dataframe(df_view, use_container_width=True, height=420, hide_index=True)

            st.download_button(
                "CSV",
                data=df.to_csv(index=False, sep=";").encode("utf-8"),
                file_name=f"{name}.csv",
                mime="text/csv",
                use_container_width=True,
            )

    st.caption(
        "Chaque action possède une clé unique `action_key` (masquée ici) pour la suivre dans le temps. "
        "Tables : "
        " Actions en cours / terminées / reportées = état actuel par sous-ensemble ; "
        " Statut des actions (statut à la date de fin) = snapshot complet au dernier jour (1 ligne par action) ; "
        " Indisponibilités équipements = périodes où les équipements sont marqués indisponibles ; "
        " (Mode analyste) Historique consolidé = statuts cohérents jour par jour ; Transitions = changements de statut. "
        "Les exports CSV conservent `action_key` pour vos croisements."
    )

else:
    st.info(" Uploade un fichier, choisis la plage de feuilles, puis clique **Lancer / Mettre à jour**.")
