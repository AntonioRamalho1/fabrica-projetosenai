import streamlit as st
from app.domain.formatters import human_number

def kpi_row(total_pecas, total_refugo, total_defeitos, avg_temp):
    c1, c2, c3, c4 = st.columns([1.5,1.2,1,1])
    c1.metric("Peças feitas hoje", human_number(total_pecas))
    c2.metric("Peças refugadas hoje", human_number(total_refugo))
    c3.metric("Defeitos detectados (hoje)", human_number(total_defeitos))
    c4.metric("Temperatura média (°C)", f"{avg_temp:.1f}" if avg_temp == avg_temp else "—")