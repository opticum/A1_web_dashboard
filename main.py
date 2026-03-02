# main.py
import pandas as pd
import streamlit as st
import psycopg
import pandas as pd
from streamlit_autorefresh import st_autorefresh

st.set_page_config(page_title="Monitor_A1 - SPREADS", layout="wide")

# Refresh the page every 1s
st_autorefresh(interval=1000, key="spreads_refresh")


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
            "desc",
            instrument_1, instrument_2,
            fx_hedge, instrument_fx,
            mult_1, mult_2, mult_fx,
            "offset", "l_bnd", "u_bnd"
        FROM spreads_inputs
        ORDER BY "desc"
    """

    mtm_sql = """
        SELECT id, mtm
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




def build_spreads(df_spreads_inputs: pd.DataFrame, df_mtm: pd.DataFrame) -> pd.DataFrame:
    df_spreads_inputs = df_spreads_inputs.copy()

    for c in ["instrument_1", "instrument_2", "instrument_fx"]:
        if c in df_spreads_inputs.columns:
            df_spreads_inputs[c] = df_spreads_inputs[c].astype(str).str.strip()

    for c in ["mult_1", "mult_2", "mult_fx", "offset", "l_bnd", "u_bnd"]:
        if c in df_spreads_inputs.columns:
            df_spreads_inputs[c] = pd.to_numeric(df_spreads_inputs[c], errors="coerce")

    df_mtm = df_mtm.copy()
    df_mtm["id"] = df_mtm["id"].astype(str).str.strip()
    df_mtm["mtm"] = pd.to_numeric(df_mtm["mtm"], errors="coerce")

    mtm_map = dict(zip(df_mtm["id"], df_mtm["mtm"]))

    out = df_spreads_inputs.copy()
    out["mtm_1"] = out["instrument_1"].map(mtm_map)
    out["mtm_2"] = out["instrument_2"].map(mtm_map)
    out["mtm_fx"] = out["instrument_fx"].map(mtm_map)

    out["Spread"] = out["instrument_1"] + "-" + out["instrument_2"]

    base = out["mtm_1"] * out["mult_1"] - out["mtm_2"] * out["mult_2"] + out["offset"]

    denom = out["mtm_fx"] * out["mult_fx"]
    hedged_leg2 = (out["mtm_2"] * out["mult_2"]) / denom
    hedged = out["mtm_1"] * out["mult_1"] - hedged_leg2 + out["offset"]

    fx = out["fx_hedge"].fillna(False).astype(bool)
    out["Value"] = np.where(fx, hedged, base)

    out["ref1"] = out["mtm_1"]
    out["ref2"] = out["mtm_2"]

    return out[["Spread", "Value", "ref1", "ref2", "l_bnd", "u_bnd"]]

st.title("SPREADS")

try:
    df_spreads_inputs, df_mtm = load_data()
    df_spreads = build_spreads(df_spreads_inputs, df_mtm)
except Exception as e:
    st.error("Failed to load/build SPREADS from Supabase.")
    st.exception(e)
    st.stop()

df_spreads = build_spreads(df_spreads_inputs, df_mtm)

def style_spreads(df: pd.DataFrame):
    def color_value(row):
        v = row["Value"]
        lb = row["l_bnd"]
        ub = row["u_bnd"]
        # only color the Value cell
        styles = [""] * len(row)
        if pd.notna(v) and pd.notna(lb) and v <= lb:
            styles[df.index.get_loc(row.name)] = ""  # not used; keep for clarity
        return styles

    def value_cell_style(row):
        v = row["Value"]
        lb = row["l_bnd"]
        ub = row["u_bnd"]
        s = [""] * len(row)
        # Apply only to the Value column
        if pd.notna(v) and pd.notna(lb) and v <= lb:
            s[row.index.get_loc("Value")] = "background-color: #b6f2b6;"  # green-ish
        elif pd.notna(v) and pd.notna(ub) and v >= ub:
            s[row.index.get_loc("Value")] = "background-color: #f7b1b1;"  # red-ish
        return s

    styler = (
        df.style
        .apply(value_cell_style, axis=1)
        .set_properties(subset=["Value"], **{"font-weight": "bold", "font-size": "120%"})
        .set_properties(subset=["ref1", "ref2"], **{"font-style": "italic"})
    )

    # Hide bounds columns (keep them for styling only)
    try:
        styler = styler.hide(axis="columns", subset=["l_bnd", "u_bnd"])
    except Exception:
        # older pandas: if hide isn't available, just leave them visible
        pass

    return styler

st.title("SPREADS")

# order exactly: Spread, Value, ref1, ref2 (bounds hidden if possible)
st.dataframe(style_spreads(df_spreads), use_container_width=True)

st.caption("Auto-refresh: 1s")