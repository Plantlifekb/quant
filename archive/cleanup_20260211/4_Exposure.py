import streamlit as st
import pandas as pd

st.title("📊 Exposure Overview")

P = r"C:\Quant\data\analytics\_picks_v1.csv"
df = pd.read_csv(P)
df["week_start"] = pd.to_datetime(df["week_start"])

exp = df.groupby(["week_start", "strategy"])["weight"].agg(["sum", "count"])
exp = exp.rename(columns={"sum": "net_exposure", "count": "n_positions"})

st.subheader("Exposure by Week")
st.dataframe(exp)

wk = exp.index.get_level_values("week_start").max()
st.subheader(f"Latest Week Exposure ({wk.date()})")
st.dataframe(exp.loc[wk])