import streamlit as st
from streamlit_autorefresh import st_autorefresh

from alerts import initialize_alert_counters_once, reset_all_alert_counters
from open_interest import render_open_interest_page
from spreads import render_spreads_page


def check_password():
    def password_entered():
        if st.session_state["password"] == st.secrets["APP_PASSWORD"]:
            st.session_state["password_correct"] = True
            del st.session_state["password"]
        else:
            st.session_state["password_correct"] = False

    if "password_correct" not in st.session_state:
        st.text_input("Password", type="password", on_change=password_entered, key="password")
        st.stop()

    elif not st.session_state["password_correct"]:
        st.text_input("Password", type="password", on_change=password_entered, key="password")
        st.error("Incorrect password")
        st.stop()


check_password()

st.set_page_config(page_title="Monitor_A1", layout="wide")

REFRESH_MS = 10000  # 10 seconds
st_autorefresh(interval=REFRESH_MS, key="global_refresh")

try:
    initialize_alert_counters_once()
except Exception as e:
    st.error("Failed to initialize alert counters.")
    st.exception(e)
    st.stop()

st.sidebar.header("Tables")
page = st.sidebar.radio(
    "Select table",
    ["Spreads", "Open Interest"],
    index=0,
)

st.sidebar.divider()
if st.sidebar.button("Reset Telegram Counters", use_container_width=True):
    try:
        reset_all_alert_counters()
        st.sidebar.success("All alert counters reset to 0")
    except Exception as e:
        st.sidebar.error("Failed to reset counters")
        st.exception(e)

try:
    if page == "Spreads":
        render_spreads_page()
    elif page == "Open Interest":
        render_open_interest_page()
except Exception as e:
    st.error("Failed to load data from Supabase.")
    st.exception(e)
    st.stop()

st.caption(f"Auto-refresh: {REFRESH_MS // 1000} seconds")