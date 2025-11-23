# app.py — App completo, formatado e pronto para rodar localmente
# Observação: este app usa os CSVs em: app/data/processed/
# Uploads disponíveis durante o desenvolvimento (informativo):
# /mnt/data/telemetria_detalhada_30dias.csv
# /mnt/data/historico_producao_1ano.csv
# /mnt/data/eventos_industriais.csv

import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import os
from textwrap import dedent

# ---------------------------
# 0. CONFIGURAÇÕES GLOBAIS
# ---------------------------
st.set_page_config(layout="wide", page_title="EcoData Monitor - Painel Profissional", page_icon="🏭")
PRECO_VENDA = 1.20       # R$ por tijolo (ajustável)
CUSTO_POR_TIJOLO = 0.45  # estimativa (opcional)

# Estilo simples para métricas
st.markdown("""
<style>
    div[data-testid="stMetric"] { background-color: #F7F9FB; border-radius: 8px; padding: 10px; }
    section[data-testid="stSidebar"] { background-color: #f6f8fb; }
    .big-title { font-size: 22px; font-weight:700; }
</style>
""", unsafe_allow_html=True)

# ---------------------------
# 1. FUNÇÃO DE CARREGAMENTO (data/processed a partir do _file_)
# ---------------------------
@st.cache_data(ttl=600)
def load_data():
    """
    Carrega os arquivos telemetria_silver.csv, producao_silver.csv, eventos_silver.csv
    dentro da pasta data/processed (relativa ao local do app.py).
    """
    try:
        base_dir = os.path.dirname(os.path.abspath(__file__))
        data_dir = os.path.join(base_dir, "data", "processed")

        tele_path = os.path.join(data_dir, "telemetria_silver.csv")
        prod_path = os.path.join(data_dir, "producao_silver.csv")
        evt_path  = os.path.join(data_dir, "eventos_silver.csv")

        missing = [p for p in [tele_path, prod_path, evt_path] if not os.path.exists(p)]
        if missing:
            raise FileNotFoundError(
                "Arquivos faltando: " + ", ".join([os.path.basename(m) for m in missing]) +
                f". Coloque-os em: {data_dir}"
            )

        tele = pd.read_csv(tele_path)
        prod = pd.read_csv(prod_path)
        evt  = pd.read_csv(evt_path)

        # parse timestamp quando existir
        for df in (tele, prod, evt):
            if "timestamp" in df.columns:
                df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")

        return tele, prod, evt

    except Exception as e:
        st.error(f"Erro ao carregar dados: {e}")
        return None, None, None


# ---------------------------
# 2. FUNÇÕES AUXILIARES / CÁLCULOS
# ---------------------------
def aggregate_by_period(df, freq="5T"):
    if df is None or df.empty or "timestamp" not in df.columns:
        return pd.DataFrame()
    d = df.copy()
    d["period"] = d["timestamp"].dt.floor(freq)
    agg = d.groupby(["maquina_id", "period"]).agg({
        "pecas_produzidas": "sum" if "pecas_produzidas" in d.columns else "count",
        "flag_defeito": "sum" if "flag_defeito" in d.columns else 0,
        "pressao_mpa": "mean" if "pressao_mpa" in d.columns else np.nan,
        "temp_matriz_c": "mean" if "temp_matriz_c" in d.columns else np.nan
    }).reset_index()
    return agg


def compute_kpis(prod_df, tele_df):
    if prod_df is None or prod_df.empty:
        return 0, 0, 0, np.nan
    # determinar último dia com produção
    if "timestamp" in prod_df.columns:
        last_date = prod_df["timestamp"].max().date()
        prod_today = prod_df[prod_df["timestamp"].dt.date == last_date]
    else:
        prod_today = prod_df
    total_pecas = int(prod_today["pecas_produzidas"].sum()) if "pecas_produzidas" in prod_today.columns else len(prod_today)
    total_refugo = int(prod_today["pecas_refugadas"].sum()) if "pecas_refugadas" in prod_today.columns else 0

    total_defeitos = 0
    avg_temp = np.nan
    if tele_df is not None and not tele_df.empty and "timestamp" in tele_df.columns:
        tele_today = tele_df[tele_df["timestamp"].dt.date == last_date]
        if "flag_defeito" in tele_today.columns:
            total_defeitos = int(tele_today["flag_defeito"].sum())
        if "temp_matriz_c" in tele_today.columns:
            avg_temp = tele_today["temp_matriz_c"].mean()
    return total_pecas, total_refugo, total_defeitos, avg_temp


def check_alerts(tele_agg):
    alerts = []
    if tele_agg is None or tele_agg.empty:
        return alerts
    crit = tele_agg[tele_agg["pressao_mpa"] < 12]
    for _, r in crit.tail(5).iterrows():
        alerts.append(f"Máquina {r['maquina_id']}: Pressão Baixa ({r['pressao_mpa']:.1f} MPa) às {r['period'].strftime('%H:%M')}")
    return alerts


def compute_refugo_by_turno(prod_df):
    if prod_df is None or prod_df.empty or "turno" not in prod_df.columns:
        return pd.DataFrame()
    grp = prod_df.groupby("turno").agg({"pecas_produzidas": "sum", "pecas_refugadas": "sum"}).reset_index().rename(columns={"pecas_refugadas": "refugos"})
    grp["pct_refugo"] = (grp["refugos"] / grp["pecas_produzidas"].replace(0, np.nan) * 100).fillna(0).round(1)
    return grp.sort_values("pct_refugo", ascending=False)


def pareto_paradas(evt_df, top_n=5):
    """Retorna top motivos robustamente, mesmo que as colunas tenham nomes diferentes."""
    if evt_df is None or evt_df.empty:
        return pd.DataFrame(columns=["motivo", "count"])
    df = evt_df.copy()
    candidate_cols = ["evento", "motivo", "descricao", "tipo", "categoria"]
    reason_col = next((c for c in candidate_cols if c in df.columns), None)
    severity_col = next((c for c in ["severidade", "gravidade", "nivel", "severity", "sev_codigo"] if c in df.columns), None)

    # filtrar severidade média/alta quando houver coluna
    if severity_col:
        cond_text = df[severity_col].astype(str).str.lower().isin(["média", "media", "alta", "high"])
        cond_code = pd.to_numeric(df[severity_col], errors="coerce").fillna(0) >= 2
        df_f = df[cond_text | cond_code]
    else:
        df_f = df

    if reason_col:
        vc = df_f[reason_col].value_counts().reset_index()
        vc.columns = ["motivo", "count"]
    else:
        df_f["motivo"] = "Motivo não informado"
        vc = df_f["motivo"].value_counts().reset_index()
        vc.columns = ["motivo", "count"]

    return vc.head(top_n)


def build_pressure_humidity_scatter(tele_df):
    if tele_df is None or tele_df.empty:
        return pd.DataFrame()
    df = tele_df.copy()
    pres_col = next((c for c in ["pressao_mpa", "pressao", "pressure", "pressão"] if c in df.columns), None)
    hum_col = next((c for c in ["umidade_pct", "umidade", "humidity"] if c in df.columns), None)
    defect_col = next((c for c in ["flag_defeito", "defeito", "is_defect", "defect"] if c in df.columns), None)
    if pres_col is None or hum_col is None:
        return pd.DataFrame()
    cols = [pres_col, hum_col]
    if defect_col:
        cols.append(defect_col)
    df_plot = df[cols].dropna(subset=[pres_col, hum_col]).copy()
    if defect_col:
        df_plot["status"] = df_plot[defect_col].apply(lambda x: "Defeito" if str(x) not in ["0", "False", "false", "nan", "None", "NaN", ""] else "OK")
    else:
        df_plot["status"] = "OK"
    df_plot = df_plot.rename(columns={pres_col: "pressao_mpa", hum_col: "umidade"})
    return df_plot


def aggregate_events(evt_df):
    if evt_df is None or evt_df.empty:
        return pd.DataFrame()
    df = evt_df.copy()
    sev_col = next((c for c in ["severidade", "gravidade", "nivel", "severity", "sev_codigo"] if c in df.columns), None)
    if sev_col:
        high = df[df[sev_col].astype(str).str.lower().str.contains("alta|high") | (pd.to_numeric(df[sev_col], errors="coerce") >= 2)]
    else:
        high = df
    if "timestamp" in high.columns:
        high = high.sort_values("timestamp", ascending=False)
    return high.head(30)


# ---------------------------
# 3. CARREGAR DADOS
# ---------------------------
with st.spinner("Carregando dados..."):
    tele_df, prod_df, evt_df = load_data()

if tele_df is None or prod_df is None or evt_df is None:
    st.stop()

# pré-process
tele_agg = aggregate_by_period(tele_df)
pecas, refugo, defeitos, temp = compute_kpis(prod_df, tele_df)
refugo_turno = compute_refugo_by_turno(prod_df)
pareto = pareto_paradas(evt_df)
scatter_df = build_pressure_humidity_scatter(tele_df)
evt_criticos = aggregate_events(evt_df)
alerts = check_alerts(tele_agg)


# ---------------------------
# 4. SIDEBAR / NAVEGAÇÃO
# ---------------------------
st.sidebar.title("📌 Navegação")
pagina = st.sidebar.radio("", ["Resumo (Lucro)", "Onde Está Meu Lucro", "Qualidade", "Manutenção", "Telemetria (Mapa)", "Eventos"])
st.sidebar.markdown("---")
st.sidebar.write("EcoData Monitor — SENAI")
st.sidebar.caption(f"Preço venda: R$ {PRECO_VENDA:.2f} | Custo estimado: R$ {CUSTO_POR_TIJOLO:.2f}")


# ---------------------------
# 5. PÁGINAS
# ---------------------------

# ---------- RESUMO (LUCRO) ----------
if pagina == "Resumo (Lucro)":
    st.title("🏭 Visão Geral — Resumo Rápido")
    st.subheader("KPIs de Operação – Último Dia Registrado")
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Peças Produzidas", f"{pecas:,}".replace(",", "."))
    c2.metric("Refugo Total (un)", f"{refugo:,}".replace(",", "."))
    c3.metric("Defeitos Detectados (telemetria)", f"{defeitos:,}")
    c4.metric("Temp. Média Matriz", f"{temp:.1f} °C" if not pd.isna(temp) else "N/D")

    st.markdown("---")
    st.subheader("Tendência de Produção (acumulado)")
    if not tele_agg.empty:
        df_acc = tele_agg.sort_values("period").copy()
        df_acc["acumulado"] = df_acc.groupby("maquina_id")["pecas_produzidas"].cumsum()
        fig = px.line(df_acc, x="period", y="acumulado", color="maquina_id", title="Produção Acumulada por Máquina")
        fig.update_layout(template="plotly_white", hovermode="x unified", yaxis_title="Peças acumuladas")
        st.plotly_chart(fig, use_container_width=True)
        st.markdown(dedent("""
            *O que isso significa (direto):*  
            - Mostra quantos tijolos foram produzidos até agora por máquina.  
            - Se uma máquina estiver muito atrás, ela está custando tempo e dinheiro.
        """))
        with st.expander("Detalhes técnicos (engenharia)"):
            st.write(dedent("""
                - Agregação padrão: 5 minutos.  
                - 'Acumulado' = soma cumulativa por máquina.
            """))
    else:
        st.info("Dados de telemetria insuficientes para a linha do tempo.")


# ---------- ONDE ESTÁ MEU LUCRO ----------
elif pagina == "Onde Está Meu Lucro":
    st.title("💰 Onde Está Meu Lucro?")
    st.write("Comparação entre faturamento real e potencial máximo (hora a hora).")

    tijolos_bons = int(prod_df["pecas_produzidas"].sum()) if "pecas_produzidas" in prod_df.columns else 0
    tijolos_refugados = int(prod_df["pecas_refugadas"].sum()) if "pecas_refugadas" in prod_df.columns else 0
    faturamento_real = tijolos_bons * PRECO_VENDA
    dinheiro_refugo = tijolos_refugados * PRECO_VENDA

    if prod_df is not None and "timestamp" in prod_df.columns and "pecas_produzidas" in prod_df.columns:
        prod_df["hour"] = prod_df["timestamp"].dt.floor("H")
        hourly = prod_df.groupby("hour")["pecas_produzidas"].sum().reset_index()
        potencial_max_hora = int(hourly["pecas_produzidas"].max()) if not hourly.empty else 0
        horas_totais = prod_df["hour"].nunique()
        lucro_potencial = potencial_max_hora * horas_totais * PRECO_VENDA
        dinheiro_evaporado = lucro_potencial - faturamento_real
    else:
        potencial_max_hora = 0
        horas_totais = 0
        lucro_potencial = 0.0
        dinheiro_evaporado = 0.0

    k1, k2, k3 = st.columns(3)
    k1.metric("📦 Faturamento Real", f"R$ {faturamento_real:,.2f}".replace(",", "."))
    k2.metric("🔥 Dinheiro Jogando Fora (Refugo)", f"R$ {dinheiro_refugo:,.2f}".replace(",", "."))
    k3.metric("💨 Dinheiro que Evaporou (Ineficiência)", f"R$ {dinheiro_evaporado:,.2f}".replace(",", "."))

    st.markdown("---")
    st.subheader("Análise do Potencial")
    st.write(dedent(f"""
        - Pico observado (melhor hora): *{potencial_max_hora} tijolos/hora*.  
        - Horas observadas no histórico: *{horas_totais} horas*.  
        - Faturamento potencial (se tivesse produzido no pico): *R$ {lucro_potencial:,.2f}*.  
        - Diferença (dinheiro evaporado): *R$ {dinheiro_evaporado:,.2f}*.
    """))

    with st.expander("Como interpretar (direto para o Sr. Roberto)"):
        st.write(dedent("""
            - 'Dinheiro evaporado' = perda por paradas e operação abaixo do pico.  
            - Priorize reduzir paradas/baixo ritmo nas horas críticas para recuperar faturamento.
        """))

    st.markdown("---")
    st.subheader("🧮 Calculadora Reversa de Custo (opcional)")
    st.write("Transforme custos mensais em custo unitário e margem.")
    gastos_totais = st.number_input("Quanto gastou no mês (R$)?", min_value=0.0, value=50000.0, step=100.0, format="%.2f")
    if gastos_totais > 0 and tijolos_bons > 0:
        custo_unitario = gastos_totais / tijolos_bons
        margem_unitaria = PRECO_VENDA - custo_unitario
        margem_pct = (margem_unitaria / PRECO_VENDA) * 100
        st.markdown(dedent(f"""
            *Resultado:*  
            - Custo por tijolo: *R$ {custo_unitario:.2f}*  
            - Margem por tijolo: *R$ {margem_unitaria:.2f}*  
            - Margem percentual: *{margem_pct:.1f}%*
        """))
        st.info("Margem saudável: 40%–60%. Abaixo de 30% é sinal de alerta.")
    elif gastos_totais > 0 and tijolos_bons == 0:
        st.warning("Não há produção válida na base para calcular custo unitário.")


# ---------- QUALIDADE ----------
elif pagina == "Qualidade":
    st.title("Qualidade — Onde estamos perdendo material?")
    # Refugo por turno
    if not refugo_turno.empty:
        fig_rt = px.bar(refugo_turno, x="turno", y="pct_refugo", text="pct_refugo", title="Taxa de Refugo por Turno (%)")
        fig_rt.update_layout(template="plotly_white", yaxis_title="% Refugo")
        st.plotly_chart(fig_rt, use_container_width=True)
        st.markdown(dedent("""
            *Explicação direta:*  
            - Mostra qual turno gera mais desperdício (%).  
            - Ação: conversar com o supervisor do turno no topo do gráfico.
        """))
        with st.expander("Detalhes técnicos (engenharia)"):
            st.write(dedent("""
                - % Refugo = (refugos / peças_produzidas) * 100 por turno.
                - Agregação por turno com soma de peças dentro do período disponível.
            """))
    else:
        st.info("Dados de 'turno' ausentes — não é possível calcular refugo por turno.")

    st.markdown("---")
    st.subheader("Linha do tempo da produção (todas as máquinas)")
    if not tele_agg.empty:
        df_total_period = tele_agg.groupby("period")["pecas_produzidas"].sum().reset_index()
        fig_tot = px.area(df_total_period, x="period", y="pecas_produzidas", title="Produção Total por Período (Todas as Máquinas)")
        fig_tot.update_layout(template="plotly_white", yaxis_title="Peças por período")
        st.plotly_chart(fig_tot, use_container_width=True)
        st.markdown(dedent("""
            *Explicação direta:*  
            - Mostra quando a produção sobe e cai ao longo do dia.  
            - Se estamos abaixo da meta em horários críticos, ajustar pessoal/produção.
        """))
    else:
        st.info("Dados de telemetria insuficientes para a linha do tempo consolidada.")


# ---------- MANUTENÇÃO ----------
elif pagina == "Manutenção":
    st.title("🔧 Manutenção Inteligente — Onde focar")
    st.markdown("Análise automática das causas que mais param sua fábrica.")

    st.subheader("📊 Pareto de Paradas (Média/Alta Severidade)")
    pareto = pareto_paradas(evt_df)
    if pareto.empty:
        st.info("Nenhum evento de severidade média/alta encontrado.")
    else:
        fig_pareto = px.bar(pareto, x="motivo", y="count", title="Top causas de parada (Pareto simplificado)", text_auto=True)
        fig_pareto.update_layout(template="plotly_white", xaxis_title="Causa", yaxis_title="Ocorrências")
        st.plotly_chart(fig_pareto, use_container_width=True)

    st.markdown("---")
    st.subheader("🚨 Últimos Eventos Críticos (Alta severidade)")
    # filtro por codigo ou texto de severidade
    if "sev_codigo" in evt_df.columns:
        evt_crit = evt_df[pd.to_numeric(evt_df["sev_codigo"], errors="coerce").fillna(0) >= 3]
    elif "severidade" in evt_df.columns:
        evt_crit = evt_df[evt_df["severidade"].astype(str).str.lower().str.contains("alta")]
    else:
        evt_crit = pd.DataFrame()
    if evt_crit.empty:
        st.success("Nenhum evento crítico detectado.")
    else:
        st.data_editor(evt_crit.sort_values("timestamp", ascending=False).head(20), use_container_width=True, height=420)


# ---------- TELEMETRIA (MAPA) ----------
elif pagina == "Telemetria (Mapa)":
    st.title("Mapa do Tesouro Operacional — Pressão x Umidade")
    if not scatter_df.empty:
        fig_sc = px.scatter(scatter_df, x="pressao_mpa", y="umidade", color="status",
                           title="Pressão x Umidade — Verde = OK / Vermelho = Defeito",
                           labels={"pressao_mpa": "Pressão (MPa)", "umidade": "Umidade (%)"})
        fig_sc.update_layout(template="plotly_white")
        st.plotly_chart(fig_sc, use_container_width=True)
        st.markdown(dedent("""
            *Explicação direta:*  
            - Pontos vermelhos mostram onde estamos perdendo dinheiro (peças defeituosas).  
            - Ação: se pressão/umidade saírem da "zona verde", intervir.
        """))
        with st.expander("Detalhes técnicos (engenharia)"):
            st.write(dedent("""
                - Recomenda-se amostragem física das peças nas zonas vermelhas para validar limites.
            """))
    else:
        st.info("Dados de pressão/umidade insuficientes para gerar o mapa operacional.")


# ---------- EVENTOS ----------
elif pagina == "Eventos":
    st.title("Eventos — Histórico")
    st.subheader("Últimos eventos (completo)")
    if evt_df is not None and not evt_df.empty:
        df_evt_recent = evt_df.sort_values("timestamp", ascending=False).head(200) if "timestamp" in evt_df.columns else evt_df.head(200)
        st.data_editor(df_evt_recent, use_container_width=True, height=520)
    else:
        st.info("Sem registros de eventos.")

# ---------------------------
# RODAPÉ / NOTAS
# ---------------------------
st.markdown("---")
st.caption("Dicas rápidas: 1) Verifique supervisor do turno com maior % de refugo. 2) Priorize ordens de serviço pelas causas do Pareto. 3) Use o mapa de pressão x umidade para instruções visuais aos operadores.")