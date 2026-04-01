import numpy as np
import pandas as pd
import streamlit as st

from alerts import process_spread_alerts
from db import get_conn
from formatters import fmt_auto, fmt_fixed


@st.cache_data(ttl=1)
def load_spreads_data():
    conn = get_conn()

    spreads_inputs_sql = """
        select
            "desc",
            desc_custom,
            instrument_1,
            instrument_2,
            fx_hedge,
            instrument_fx,
            mult_1,
            mult_2,
            mult_fx,
            spread_dec,
            alert_counter,
            "offset",
            "l_bnd",
            "u_bnd"
        from public.spreads_inputs
        order by "desc"
    """

    mtm_sql = """
        select
            id,
            mtm
        from public.a1_md_all
    """

    with conn.cursor() as cur:
        cur.execute(spreads_inputs_sql)
        s_rows = cur.fetchall()
        s_cols = [d[0] for d in cur.description]
        df_spreads_inputs = pd.DataFrame(s_rows, columns=s_cols)

        cur.execute(mtm_sql)
        m_rows = cur.fetchall()
        m_cols = [d[0] for d in cur.description]
        df_mtm = pd.DataFrame(m_rows, columns=m_cols)

    return df_spreads_inputs, df_mtm


def build_spreads(df_spreads_inputs: pd.DataFrame, df_mtm: pd.DataFrame) -> pd.DataFrame:
    out = df_spreads_inputs.copy()
    df_mtm = df_mtm.copy()

    for c in ["desc", "desc_custom", "instrument_1", "instrument_2", "instrument_fx"]:
        if c in out.columns:
            out[c] = out[c].fillna("").astype(str).str.strip()

    for c in ["mult_1", "mult_2", "mult_fx", "offset", "l_bnd", "u_bnd", "spread_dec", "alert_counter"]:
        if c in out.columns:
            out[c] = pd.to_numeric(out[c], errors="coerce")

    df_mtm["id"] = df_mtm["id"].astype(str).str.strip()
    df_mtm["mtm"] = pd.to_numeric(df_mtm["mtm"], errors="coerce")

    mtm_map = dict(zip(df_mtm["id"], df_mtm["mtm"]))

    out["mtm_1"] = out["instrument_1"].map(mtm_map)
    out["mtm_2"] = out["instrument_2"].map(mtm_map)
    out["mtm_fx"] = out["instrument_fx"].map(mtm_map)

    default_spread = out["instrument_1"] + "-" + out["instrument_2"]
    custom = out["desc_custom"].fillna("").astype(str).str.strip()
    out["Spread"] = np.where(custom != "", custom, default_spread)

    base = out["mtm_1"] * out["mult_1"] - out["mtm_2"] * out["mult_2"] + out["offset"]

    denom = out["mtm_fx"] * out["mult_fx"]
    hedged_leg2 = (out["mtm_2"] * out["mult_2"]) / denom
    hedged = out["mtm_1"] * out["mult_1"] - hedged_leg2 + out["offset"]

    fx = out["fx_hedge"].fillna(False).astype(bool)
    calc_value = np.where(fx, hedged, base)

    out["Value_num"] = np.where(
        (out["mtm_1"] <= 0) | (out["mtm_2"] <= 0),
        np.nan,
        calc_value
    )

    out["spread_dec"] = pd.to_numeric(out["spread_dec"], errors="coerce").fillna(2).astype(int)

    out["Value"] = out.apply(
        lambda r: fmt_fixed(r["Value_num"], r["spread_dec"]) if pd.notna(r["Value_num"]) else "",
        axis=1
    )

    out["ref1"] = out["mtm_1"]
    out["ref2"] = out["mtm_2"]

    return out[[
        "desc",
        "instrument_1",
        "instrument_2",
        "Spread",
        "Value",
        "ref1",
        "ref2",
        "Value_num",
        "alert_counter",
        "l_bnd",
        "u_bnd",
    ]]


def style_spreads(df: pd.DataFrame):
    visible_df = df[["Spread", "Value", "ref1", "ref2"]].copy()

    def value_cell_style(row):
        raw_row = df.loc[row.name]
        value_num = raw_row["Value_num"]
        l_bnd = raw_row["l_bnd"]
        u_bnd = raw_row["u_bnd"]

        styles = [""] * len(row)

        if pd.notna(value_num) and pd.notna(l_bnd) and value_num <= l_bnd:
            styles[row.index.get_loc("Value")] = "background-color: #b6f2b6;"
        elif pd.notna(value_num) and pd.notna(u_bnd) and value_num >= u_bnd:
            styles[row.index.get_loc("Value")] = "background-color: #f7b1b1;"

        return styles

    styler = (
        visible_df.style
        .apply(value_cell_style, axis=1)
        .format({
            "ref1": fmt_auto,
            "ref2": fmt_auto,
        })
        .set_properties(subset=["Value"], **{"font-weight": "bold", "font-size": "120%"})
        .set_properties(subset=["ref1", "ref2"], **{"font-style": "italic"})
    )

    return styler


def render_spreads_page():
    df_spreads_inputs, df_mtm = load_spreads_data()
    df_spreads = build_spreads(df_spreads_inputs, df_mtm)

    process_spread_alerts(df_spreads)

    st.dataframe(style_spreads(df_spreads), use_container_width=True)