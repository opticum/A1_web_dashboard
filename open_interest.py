import numpy as np
import pandas as pd
import streamlit as st

from db import get_conn
from formatters import fmt_localized, fmt_percent


@st.cache_data(ttl=1)
def load_open_interest_data():
    conn = get_conn()

    inputs_sql = """
        select instrument_code
        from public.open_interest_inputs
    """

    md_sql = """
        select id, open_interest
        from public.a1_md_all
    """

    snapshot_sql = """
        select instrument_code, oi
        from public.fut_oi_snapshot
    """

    with conn.cursor() as cur:
        cur.execute(inputs_sql)
        i_rows = cur.fetchall()
        i_cols = [d[0] for d in cur.description]
        df_inputs = pd.DataFrame(i_rows, columns=i_cols)

        cur.execute(md_sql)
        md_rows = cur.fetchall()
        md_cols = [d[0] for d in cur.description]
        df_md = pd.DataFrame(md_rows, columns=md_cols)

        cur.execute(snapshot_sql)
        s_rows = cur.fetchall()
        s_cols = [d[0] for d in cur.description]
        df_snapshot = pd.DataFrame(s_rows, columns=s_cols)

    return df_inputs, df_md, df_snapshot


def build_open_interest(df_inputs: pd.DataFrame, df_md: pd.DataFrame, df_snapshot: pd.DataFrame) -> pd.DataFrame:
    out = df_inputs.copy()
    df_md = df_md.copy()
    df_snapshot = df_snapshot.copy()

    out["instrument_code"] = out["instrument_code"].astype(str).str.strip()

    df_md["id"] = df_md["id"].astype(str).str.strip()
    df_md["open_interest"] = pd.to_numeric(df_md["open_interest"], errors="coerce")

    df_snapshot["instrument_code"] = df_snapshot["instrument_code"].astype(str).str.strip()
    df_snapshot["oi"] = pd.to_numeric(df_snapshot["oi"], errors="coerce")

    md_map = dict(zip(df_md["id"], df_md["open_interest"]))
    prev_map = dict(zip(df_snapshot["instrument_code"], df_snapshot["oi"]))

    out["md_id"] = "MOEX:" + out["instrument_code"]
    out["Open Interest"] = out["md_id"].map(md_map)
    out["Open Interest Prev"] = out["instrument_code"].map(prev_map)
    out["Change"] = out["Open Interest"] - out["Open Interest Prev"]

    max_oi = out[["Open Interest", "Open Interest Prev"]].max(axis=1)
    out["Change %"] = np.where(
        max_oi > 0,
        out["Change"].abs() / max_oi,
        np.nan
    )

    out = out.rename(columns={"instrument_code": "Instrument"})

    return out[["Instrument", "Change", "Change %", "Open Interest", "Open Interest Prev"]]


def style_open_interest(df: pd.DataFrame):
    def change_font_style(val):
        if pd.isna(val):
            return ""
        if val < 0:
            return "color: red;"
        if val > 0:
            return "color: darkgreen;"
        return ""

    styler = (
        df.style
        .format({
            "Change": fmt_localized,
            "Change %": fmt_percent,
            "Open Interest": fmt_localized,
            "Open Interest Prev": fmt_localized,
        })
        .map(change_font_style, subset=["Change"])
    )

    return styler


def render_open_interest_page():
    df_inputs, df_md, df_snapshot = load_open_interest_data()
    df_oi = build_open_interest(df_inputs, df_md, df_snapshot)
    st.dataframe(style_open_interest(df_oi), use_container_width=True)