import streamlit as st

def main_page():
    st.markdown("# Main")
    st.sidebar.markdown("Menu")

def ts_tickers():
    st.markdown("Timescale Tickers")
    st.sidebar.markdown("Timescale Tickers")

page_names_to_funcs = {
    "Main Page": main_page,
    "Timescale Tickers": ts_tickers
}

selected_page = st.sidebar.selectbox("Choose page", page_names_to_funcs.keys())
page_names_to_funcs[selected_page]()
