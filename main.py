import numpy as np
import streamlit as st
import psycopg
import pandas as pd
from decimal import Decimal
from streamlit_autorefresh import st_autorefresh

st.set_page_config(page_title="Monitor_A1 - SPREADS", layout="wide")

# ----------------------------
# Global parameters (sidebar)
# ----------------------------
st.sidebar.header("Settings")
REFRESH_MS = st.sidebar.number_input(
    "Refresh interval (ms)",
    min_value=250,
    max_value=60_000,
    value=1000,
    step=250,
    help="How often the page refreshes. 1000 ms = 1 second.",
)

# Refresh the page
st_autorefresh(interval=int(REFRESH_MS), key="spreads_refresh")

# ----------------------------
# DB connection
# ----------------------------
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


@st.cache_data(ttl=1)
def load_data():
    conn = get_conn()

    spreads_inputs_sql = """
        SELECT
            desc_custom,
            instrument_1, instrument_2,
            fx_hedge, instrument_fx,
            mult_1, mult_2, mult_fx,
            spread_dec,
            "offset", "l_bnd", "u_bnd"
        FROM spreads_inputs
    """

    # include mtm::text for decimal counting
    mtm_sql = """
        SELECT id, mtm, mtm::text AS mtm_text
        FROM a1_md_all
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


# ----------------------------
# Business logic
# ----------------------------
def decimals_ignoring_trailing_zeros(mtm_text: str) -> int:
    if mtm_text is None:
        return 0
    s = str(mtm_text)
    if "." not in s:
        return 0
    frac = s.split(".", 1)[1].rstrip("0")
    return len(frac)


def build_spreads(df_spreads_inputs: pd.DataFrame, df_mtm: pd.DataFrame) -> pd.DataFrame:
    df_spreads_inputs = df_spreads_inputs.copy()

    # cleanup strings
    for c in ["instrument_1", "instrument_2", "instrument_fx"]:
        if c in df_spreads_inputs.columns:
            df_spreads_inputs[c] = df_spreads_inputs[c].astype(str).str.strip()

    # numeric safety
    for c in ["mult_1", "mult_2", "mult_fx", "offset", "l_bnd", "u_bnd", "spread_dec"]:
        if c in df_spreads_inputs.columns:
            df_spreads_inputs[c] = pd.to_numeric(df_spreads_inputs[c], errors="coerce")

    df_mtm = df_mtm.copy()
    df_mtm["id"] = df_mtm["id"].astype(str).str.strip()
    df_mtm["mtm"] = pd.to_numeric(df_mtm["mtm"], errors="coerce")
    df_mtm["decimals"] = df_mtm["mtm_text"].map(decimals_ignoring_trailing_zeros)

    # maps
    mtm_map = dict(zip(df_mtm["id"], df_mtm["mtm"]))
    dec_map = dict(zip(df_mtm["id"], df_mtm["decimals"]))

    out = df_spreads_inputs.copy()
    out["mtm_1"] = out["instrument_1"].map(mtm_map)
    out["mtm_2"] = out["instrument_2"].map(mtm_map)
    out["mtm_fx"] = out["instrument_fx"].map(mtm_map)


    # Default spread name = instrument_1-instrument_2
    default_spread = out["instrument_1"] + "-" + out["instrument_2"]

    # Use desc_custom if not null/blank, otherwise default
    custom = out.get("desc_custom", "").fillna("").astype(str).str.strip()
    out["Spread"] = np.where(custom != "", custom, default_spread)

    # base value
    base = out["mtm_1"] * out["mult_1"] - out["mtm_2"] * out["mult_2"] + out["offset"]

    # hedged value (as you confirmed)
    denom = out["mtm_fx"] * out["mult_fx"]
    hedged_leg2 = (out["mtm_2"] * out["mult_2"]) / denom
    hedged = out["mtm_1"] * out["mult_1"] - hedged_leg2 + out["offset"]

    fx = out["fx_hedge"].fillna(False).astype(bool)
    out["Value_raw"] = np.where(fx, hedged, base)

    # decimals from table (NULL -> default 2)
    out["spread_dec"] = pd.to_numeric(out["spread_dec"], errors="coerce").fillna(2).astype(int)

    out["Value"] = out.apply(
        lambda r: round(r["Value_raw"], r["spread_dec"]) if pd.notna(r["Value_raw"]) else r["Value_raw"],
        axis=1
    )

    # refs
    out["ref1"] = out["mtm_1"]
    out["ref2"] = out["mtm_2"]
    out["Value"] = out.apply(lambda r: float(f"{r['Value']:.{r['spread_dec']}f}"), axis=1)

    # keep bounds for styling (we'll hide them later)

    return out[["Spread", "Value", "ref1", "ref2", "l_bnd", "u_bnd"]]


# ----------------------------
# Styling / display
# ----------------------------
def fmt_value(val, dec):
    if pd.isna(val):
        return ""
    return f"{val:.{dec}f}"


def style_spreads(df: pd.DataFrame):
    def value_cell_style(row):
        v = row["Value"]
        lb = row["l_bnd"]
        ub = row["u_bnd"]
        s = [""] * len(row)

        # Apply only to the Value column
        if pd.notna(v) and pd.notna(lb) and v <= lb:
            s[row.index.get_loc("Value")] = "background-color: #b6f2b6;"  # green
        elif pd.notna(v) and pd.notna(ub) and v >= ub:
            s[row.index.get_loc("Value")] = "background-color: #f7b1b1;"  # red
        return s

    styler = (
        df.style
        .apply(value_cell_style, axis=1)
        .format({
            "Value": lambda v: v,
            "ref1": lambda v: v,
            "ref2": lambda v: v
        })
        .set_properties(subset=["Value"], **{"font-weight": "bold", "font-size": "120%"})
        .set_properties(subset=["ref1", "ref2"], **{"font-style": "italic"})
    )

    # Hide bounds columns (kept only for styling)
    try:
        styler = styler.hide(axis="columns", subset=["l_bnd", "u_bnd"])
    except Exception:
        pass

    return styler


# ----------------------------
# Main
# ----------------------------
try:
    df_spreads_inputs, df_mtm = load_data()
    df_spreads = build_spreads(df_spreads_inputs, df_mtm)
except Exception as e:
    st.error("Failed to load/build SPREADS from Supabase.")
    st.exception(e)
    st.stop()

st.dataframe(style_spreads(df_spreads), use_container_width=True)
st.caption(f"Auto-refresh: {int(REFRESH_MS)} ms")