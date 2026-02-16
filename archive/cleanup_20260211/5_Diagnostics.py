import streamlit as st
import pandas as pd

st.title("🩺 Diagnostics")

P = r"C:\Quant\data\analytics\_picks_v1.csv"
F = r"C:\Quant\data\analytics\_perf_v1.csv"

dfp = pd.read_csv(P)
dff = pd.read_csv(F)

def tick(ok):
    return "✔" if ok else "�"

st.subheader("Health Checks")

checks = {
    "Picks file not empty": not dfp.empty,
    "Performance file not empty": not dff.empty,
    "regime_label present": "regime_label" in dfp.columns,
    "weekly_return non‑zero": dff["weekly_return"].abs().sum() > 0 if not dff.empty else False,
    "multiple weeks present": dff["week_start"].nunique() > 3 if not dff.empty else False,
}

for label, ok in checks.items():
    st.write(f"{tick(ok)} {label}")

st.subheader("Missing Data")
st.write("Picks missing:")
st.dataframe(dfp.isna().sum())

st.write("Performance missing:")
st.dataframe(dff.isna().sum())

st.subheader("Schema")
st.write("Picks columns:", list(dfp.columns))
st.write("Performance columns:", list(dff.columns))