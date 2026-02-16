import streamlit as st
import pandas as pd

st.title("🧭 Regime Analysis")

P = r"C:\Quant\data\analytics\_picks_v1.csv"
df = pd.read_csv(P)

if "regime_label" not in df.columns:
    st.error("� regime_label missing from picks file")
else:
    st.success("✔ regime_label found")

    st.subheader("Regime Distribution")
    st.bar_chart(df.groupby("regime_label")["ticker"].count())

    st.subheader("Regime by Strategy")
    pivot = df.groupby(["regime_label", "strategy"])["ticker"].count().unstack(fill_value=0)
    st.dataframe(pivot)