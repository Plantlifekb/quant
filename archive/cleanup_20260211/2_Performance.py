import streamlit as st
import pandas as pd
import altair as alt

st.title("📈 Performance")

F = r"C:\Quant\data\analytics\_perf_v1.csv"
df = pd.read_csv(F)

if df.empty:
    st.warning("Performance file is empty.")
else:
    df["week_start"] = pd.to_datetime(df["week_start"])

    st.subheader("Cumulative Returns")
    chart = (
        alt.Chart(df)
        .mark_line()
        .encode(
            x="week_start:T",
            y="cumulative_return:Q",
            color="strategy:N",
        )
        .interactive()
    )
    st.altair_chart(chart, use_container_width=True)

    st.subheader("Drawdowns")
    dd = (
        alt.Chart(df)
        .mark_area(opacity=0.4)
        .encode(
            x="week_start:T",
            y="drawdown:Q",
            color="strategy:N",
        )
        .interactive()
    )
    st.altair_chart(dd, use_container_width=True)