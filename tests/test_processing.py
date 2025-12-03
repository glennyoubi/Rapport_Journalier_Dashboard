import io

import pandas as pd
import pytest

from core.processing import norm_cols, build_week_tables_from_excel_bytes
from core.filters import date_bounds_from_tables


def _make_excel_bytes():
    buf = io.BytesIO()
    header = [
        "Champ / Zone",
        "Plateforme",
        "N° puits",
        "TAG Equipement",
        "Sous-equipement",
        "Metier",
        "Travail effectue + commentaires",
        "Terminé",
        "En cours",
        "Reporté",
        "Indisponible",
    ]

    def _write_sheet(writer, name: str, date_str: str, rows: list[list]):
        raw = pd.DataFrame([[None] * len(header) for _ in range(8)])
        raw.iloc[2, 8] = pd.Timestamp(date_str)  # cellule date
        raw.iloc[4, : len(header)] = header
        for idx, row in enumerate(rows):
            raw.iloc[5 + idx, : len(header)] = row
        raw.to_excel(writer, sheet_name=name, index=False, header=False)

    with pd.ExcelWriter(buf, engine="xlsxwriter") as writer:
        _write_sheet(
            writer,
            "S1",
            "2023-01-01",
            [
                ["Z1", "PF", "1", "TAG1", "SOUS1", "Prod", "Action 1", False, True, False, False],
                ["Z2", "PF", "2", "TAG2", "SOUS2", "Maint", "Action 2", False, False, True, True],
            ],
        )
        _write_sheet(
            writer,
            "S2",
            "2023-01-02",
            [
                ["Z1", "PF", "1", "TAG1", "SOUS1", "Prod", "Action 1", True, False, False, False],
                ["Z2", "PF", "2", "TAG2", "SOUS2", "Maint", "Action 2", False, False, True, True],
            ],
        )
    buf.seek(0)
    return buf.getvalue()


def test_norm_cols_maps_variants():
    cols = [
        "Champ / Zone",
        "plateforme/sous-zone",
        "N° puits",
        "TAG équipement",
        "Sous equipement",
        "Métier",
        "Travail effectue + commentaires",
        "Reporté",
    ]
    mapped = norm_cols(cols)
    assert mapped[:6] == [
        "champ_zone",
        "plateforme_sous_zone",
        "num_puits",
        "tag_equipement",
        "sous_equipement",
        "metier",
    ]
    assert mapped[6:] == ["travaux_commentaires", "reporte"]


def test_build_week_tables_rollup_and_bounds():
    excel_bytes = _make_excel_bytes()
    tables = build_week_tables_from_excel_bytes(excel_bytes, 1, 2)

    assert set(tables.keys()) == {
        "actions_daily",
        "actions_consistent",
        "actions_latest",
        "ended_actions",
        "running_actions",
        "postponed_actions",
        "equipment_downtime",
        "transitions",
    }
    assert len(tables["actions_daily"]) == 4

    dmin, dmax = date_bounds_from_tables(tables)
    assert str(dmin.date()) == "2023-01-01"
    assert str(dmax.date()) == "2023-01-02"

    latest = tables["actions_latest"]
    tag1 = latest[latest["tag_equipement"] == "TAG1"].iloc[0]
    tag2 = latest[latest["tag_equipement"] == "TAG2"].iloc[0]
    assert bool(tag1["termine"]) is True
    assert bool(tag2["reporte"]) is True

    transitions = tables["transitions"]
    assert (transitions["change_desc"].str.contains("termine", case=False)).any()

    downtime = tables["equipment_downtime"]
    assert len(downtime) == 2  # Action 2 indisponible sur 2 jours


def test_build_week_tables_invalid_range():
    excel_bytes = _make_excel_bytes()
    with pytest.raises(ValueError):
        build_week_tables_from_excel_bytes(excel_bytes, 3, 5)
