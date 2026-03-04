import pandas as pd
import streamlit as st
import psycopg
from streamlit_autorefresh import st_autorefresh

st.set_page_config(page_title="Monitor_A1 - SPREADS", layout="wide")

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
            desc_custom,
            instrument_1, instrument_2,
            fx_hedge, instrument_fx,
            mult_1, mult_2, mult_fx,
            spread_dec,
            "offset", "l_bnd", "u_bnd"
        FROM spreads_inputs
    """

    # IMPORTANT: match spreads_inputs.instrument_* to a1_md_all.id
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
    df_spreads_inputs["instrument_1"] = df_spreads_inputs["instrument_1"].astype(str).str.strip()
    df_spreads_inputs["instrument_2"] = df_spreads_inputs["instrument_2"].astype(str).str.strip()

    # numeric safety
    for c in ["mult_1", "mult_2", "mult_fx", "offset", "l_bnd", "u_bnd", "spread_dec"]:
        if c in df_spreads_inputs.columns:
            df_spreads_inputs[c] = pd.to_numeric(df_spreads_inputs[c], errors="coerce")

    df_mtm = df_mtm.copy()
    df_mtm["id"] = df_mtm["id"].astype(str).str.strip()
    df_mtm["mtm"] = pd.to_numeric(df_mtm["mtm"], errors="coerce")

    # map: id -> mtm
    mtm_map = dict(zip(df_mtm["id"], df_mtm["mtm"]))

    out = df_spreads_inputs.copy()
    out["spread_dec"] = out["spread_dec"].fillna(2).astype(int)
    out["mtm_1"] = out["instrument_1"].map(mtm_map)
    out["mtm_2"] = out["instrument_2"].map(mtm_map)

    out["Spread"] = out["instrument_1"] + "-" + out["instrument_2"]

    # Your rule:
    out["Value_raw"] = out["mtm_1"] * out["mult_1"] - out["mtm_2"] * out["mult_2"]

    # decimals for display: spread_dec (NULL -> default 2)
    out["spread_dec"] = out["spread_dec"].fillna(2).astype(int)

    # rounded Value (0 => no decimals, 1 => 1 decimal, etc.)
    out["Value"] = out.apply(
        lambda r: round(r["Value_raw"], int(r["spread_dec"])) if pd.notna(r["Value_raw"]) else r["Value_raw"],
        axis=1
    )

    # Debug flags if something doesn't match
    out["Missing_mtm_1"] = out["mtm_1"].isna()
    out["Missing_mtm_2"] = out["mtm_2"].isna()

    # Optional: check bounds if provided
    if "l_bnd" in out.columns and "u_bnd" in out.columns:
        out["Out_of_bounds"] = (out["Value"] < out["l_bnd"]) | (out["Value"] > out["u_bnd"])
    else:
        out["Out_of_bounds"] = False

    # Final table: only what you want to display
    final = out[["Spread", "Value"]].copy()
    final["ref1"] = out["mtm_1"]
    final["ref2"] = out["mtm_2"]

    # enforce column order
    final = final[["Spread", "Value", "ref1", "ref2"]]

    return final

st.title("SPREADS")

try:
    df_spreads_inputs, df_mtm = load_data()
    df_spreads = build_spreads(df_spreads_inputs, df_mtm)
except Exception as e:
    st.error("Failed to load/build SPREADS from Supabase.")
    st.exception(e)
    st.stop()

st.dataframe(df_spreads, use_container_width=True, hide_index=True)
st.caption("Auto-refresh: 1s")