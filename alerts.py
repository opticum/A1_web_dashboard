import pandas as pd
import requests
import streamlit as st

from db import get_conn
from formatters import fmt_auto


@st.cache_resource
def initialize_alert_counters_once():
    """
    Runs once per app process start / restart.
    Resets all spreads_inputs.alert_counter values to 0.
    """
    reset_all_alert_counters()
    return True


def reset_all_alert_counters():
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("""
            update public.spreads_inputs
            set alert_counter = 0
        """)
    conn.commit()


def send_telegram(message: str):
    token = st.secrets.get("TELEGRAM_BOT_TOKEN", "")
    chat_id = st.secrets.get("TELEGRAM_CHAT_ID", "")

    if not token or not chat_id:
        return

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    requests.post(
        url,
        json={"chat_id": chat_id, "text": message},
        timeout=10,
    )


def increment_alert_counter(desc_value: str, instrument_1: str, instrument_2: str):
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("""
            update public.spreads_inputs
            set alert_counter = coalesce(alert_counter, 0) + 1
            where "desc" = %s
              and instrument_1 = %s
              and instrument_2 = %s
        """, (desc_value, instrument_1, instrument_2))
    conn.commit()


def process_spread_alerts(df: pd.DataFrame):
    """
    Uses spreads_inputs.alert_counter as the persistent counter.
    Max 2 alerts per spread total.
    """
    for _, row in df.iterrows():
        spread_name = row["Spread"]
        desc_value = row["desc"]
        instrument_1 = row["instrument_1"]
        instrument_2 = row["instrument_2"]

        value_num = row["Value_num"]
        value_text = row["Value"]
        l_bnd = row["l_bnd"]
        u_bnd = row["u_bnd"]
        alert_counter = int(row["alert_counter"]) if pd.notna(row["alert_counter"]) else 0

        if pd.isna(value_num):
            continue

        if alert_counter >= 2:
            continue

        if pd.notna(u_bnd) and value_num >= u_bnd:
            msg = f"{spread_name} spread is {value_text} (exceeds {fmt_auto(u_bnd)})"
            send_telegram(msg)
            increment_alert_counter(desc_value, instrument_1, instrument_2)
            continue

        if pd.notna(l_bnd) and value_num <= l_bnd:
            msg = f"{spread_name} spread is {value_text} (below {fmt_auto(l_bnd)})"
            send_telegram(msg)
            increment_alert_counter(desc_value, instrument_1, instrument_2)