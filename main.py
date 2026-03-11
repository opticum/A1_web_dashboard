import numpy as np
import pandas as pd
import psycopg
import streamlit as st
from babel.numbers import format_decimal
from streamlit_autorefresh import st_autorefresh

st.set_page_config(page_title="Monitor_A1", layout="wide")

# --------------------------------------------------
# Global settings
# --------------------------------------------------
REFRESH_MS = 10000  # 10 seconds
APP_LOCALE = "en_US"  # examples: en_US, ru_RU, de_DE

st_autorefresh(interval=REFRESH_MS, key="global_refresh")

# Sidebar navigation
st.sidebar.header("Tables")
page = st.sidebar.radio(
    "Select table",
    ["Spreads", "Open Interest"],
    index=0
)

# --------------------------------------------------
# DB connection
# --------------------------------------------------
@st.cache_resource
def get_conn():
    host = st.secrets["SUPABASE_DB_HOST"]
    port = st.secrets.get("SUPABASE_DB_PORT", "5432")
    dbname = st.secrets.get("SUPABASE_DB_NAME", "postgres")
    user = st.secrets.get("SUPABASE_DB_USER", "postgres")
    password = st.secrets["SUPABASE_DB_PASSWORD"]
    sslmode = st.secrets.get("SUPABASE_DB_SSLMODE", "require")

    conn_str = (
        f"host={host} port={port} dbname={dbname} user={user} "
        f"password={password} sslmode={sslmode}"
    )
    return psycopg.connect(conn_str)


# --------------------------------------------------
# Formatting helpers
# --------------------------------------------------
def fmt_auto(x):
    if pd.isna(x):
        return ""
    x = float(x)
    if x.is_integer():
        return str(int(x))
    return f"{x:,.10f}".rstrip("0").rstrip(".")


def fmt_fixed(x, decimals):
    if pd.isna(x):
        return ""
    return f"{float(x):.{int(decimals)}f}"


def fmt_localized(x):
    if pd.isna(x):
        return ""
    return format_decimal(x, locale=APP_LOCALE)


# --------------------------------------------------
# SPREADS
# --------------------------------------------------
@st.cache_data(ttl=1)
def load_spreads_data():
    conn = get_conn()

    spreads_inputs_sql = """
        SELECT
            "desc",
            desc_custom,
            instrument_1, instrument_2,
            fx_hedge, instrument_fx,
            mult_1, mult_2, mult_fx,
            spread_dec,
            "offset", "l_bnd", "u_bnd"
        FROM public.spreads_inputs
        ORDER BY "desc"
    """

    mtm_sql = """
        SELECT
            id,
            mtm
        FROM public.a1_md_all
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

    for c in ["instrument_1", "instrument_2", "instrument_fx", "desc_custom", "desc"]:
        if c in out.columns:
            out[c] = out[c].fillna("").astype(str).str.strip()

    for c in ["mult_1", "mult_2", "mult_fx", "offset", "l_bnd", "u_bnd", "spread_dec"]:
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
    out["Value_num"] = np.where(fx, hedged, base)

    out["spread_dec"] = pd.to_numeric(out["spread_dec"], errors="coerce").fillna(2).astype(int)

    out["Value"] = out.apply(
        lambda r: fmt_fixed(r["Value_num"], r["spread_dec"]) if pd.notna(r["Value_num"]) else "",
        axis=1
    )

    out["ref1"] = out["mtm_1"]
    out["ref2"] = out["mtm_2"]

    return out[["Spread", "Value", "ref1", "ref2", "Value_num", "l_bnd", "u_bnd"]]


def style_spreads(df: pd.DataFrame):
    def value_cell_style(row):
        v = row["Value_num"]
        lb = row["l_bnd"]
        ub = row["u_bnd"]
        s = [""] * len(row)

        if pd.notna(v) and pd.notna(lb) and v <= lb:
            s[row.index.get_loc("Value")] = "background-color: #b6f2b6;"
        elif pd.notna(v) and pd.notna(ub) and v >= ub:
            s[row.index.get_loc("Value")] = "background-color: #f7b1b1;"
        return s

    styler = (
        df.style
        .apply(value_cell_style, axis=1)
        .format({
            "ref1": fmt_auto,
            "ref2": fmt_auto,
        })
        .set_properties(subset=["Value"], **{"font-weight": "bold", "font-size": "120%"})
        .set_properties(subset=["ref1", "ref2"], **{"font-style": "italic"})
    )

    try:
        styler = styler.hide(axis="columns", subset=["Value_num", "l_bnd", "u_bnd"])
    except Exception:
        pass

    return styler


# --------------------------------------------------
# OPEN INTEREST
# --------------------------------------------------
@st.cache_data(ttl=1)
def load_open_interest_data():
    conn = get_conn()

    inputs_sql = """
        SELECT instrument_code
        FROM public.open_interest_inputs
    """

    md_sql = """
        SELECT id, open_interest
        FROM public.a1_md_all
    """

    snapshot_sql = """
        SELECT instrument_code, oi
        FROM public.fut_oi_snapshot
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

    out = out.rename(columns={"instrument_code": "Instrument"})

    return out[["Instrument", "Change", "Open Interest", "Open Interest Prev"]]


def style_open_interest(df: pd.DataFrame):
    styler = (
        df.style
        .format({
            "Change": fmt_localized,
            "Open Interest": fmt_localized,
            "Open Interest Prev": fmt_localized,
        })
    )
    return styler


# --------------------------------------------------
# Main
# --------------------------------------------------
try:
    if page == "Spreads":
        df_spreads_inputs, df_mtm = load_spreads_data()
        df_spreads = build_spreads(df_spreads_inputs, df_mtm)
        st.dataframe(style_spreads(df_spreads), use_container_width=True)

    elif page == "Open Interest":
        df_inputs, df_md, df_snapshot = load_open_interest_data()
        df_oi = build_open_interest(df_inputs, df_md, df_snapshot)
        st.dataframe(style_open_interest(df_oi), use_container_width=True)

except Exception as e:
    st.error("Failed to load data from Supabase.")
    st.exception(e)
    st.stop()

st.caption(f"Auto-refresh: {REFRESH_MS // 1000} seconds")