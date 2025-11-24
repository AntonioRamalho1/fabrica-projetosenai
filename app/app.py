import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
import os
from textwrap import dedent
from domain.kpis import (
    aggregate_by_period, compute_kpis, check_alerts, 
    compute_refugo_by_turno, pareto_paradas, 
    build_pressure_humidity_scatter, aggregate_events,
    compute_oee_kpis, compute_energy_cost,
    calculate_mttr_mtbf, load_gold_kpis, # Novas funções
    map_isa95 # ISA-95
)
from ml.predictor import predict_defeito_prob

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
        data_dir = os.path.join(base_dir, "data", "silver") # Alterado de 'processed' para 'silver'

        tele_path = os.path.join(data_dir, "telemetria_silver.csv")
        prod_path = os.path.join(data_dir, "producao_silver.csv")
        evt_path  = os.path.join(data_dir, "eventos_silver.csv")

        missing = [p for p in [tele_path, prod_path, evt_path] if not os.path.exists(p)]
        if missing:
            raise FileNotFoundError(
                f"Arquivos faltando: {', '.join([os.path.basename(m) for m in missing])}. Coloque-os em: {data_dir}"
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
# 2. CARREGAR DADOS
# ---------------------------
with st.spinner("Carregando dados da Camada Silver..."):
    # Carrega dados da Camada Silver (limpos e padronizados)
    tele_df, prod_df, evt_df = load_data()

if tele_df is None or prod_df is None or evt_df is None:
    st.stop()

# --- CORREÇÃO DE ROBUSTEZ: FALLBACK PARA duracao_min ---
# Garante que a coluna duracao_min exista no DataFrame de Eventos antes de qualquer função de KPI
if evt_df is not None and "duracao_min" not in evt_df.columns:
    print("AVISO: Coluna 'duracao_min' ausente no arquivo. Criando com valor padrão de 10 min para evitar KeyError.")
    evt_df["duracao_min"] = 10.0
# --------------------------------------------------------

# --- CONTEXTUALIZAÇÃO ISA-95 ---
# Aplica a hierarquia ISA-95 aos DataFrames para contextualização
tele_df = map_isa95(tele_df)
prod_df = map_isa95(prod_df)
evt_df = map_isa95(evt_df)

# ---------------------------
# 3. SIDEBAR / NAVEGAÇÃO (SIMPLIFICADA)
# ---------------------------
st.sidebar.title("📌 Menu Principal")
pagina = st.sidebar.radio(
    "Selecione o Módulo:", 
    [
        "📊 Visão Geral da Fábrica", 
        "💰 Perdas Financeiras", 
        "📉 Qualidade & Refugo", 
        "🔧 Paradas & Confiabilidade", 
        "📡 Sensores em Tempo Real", 
        "🤖 Inteligência Artificial", 
        "📋 Histórico de Alertas"
    ]
)
st.sidebar.markdown("---")

# Filtro Simplificado para o Sr. Roberto
st.sidebar.title("📅 Filtro de Data")
opcao_visualizacao = st.sidebar.radio(
    "O que você quer analisar?",
    ["Hoje (Tempo Real)", "Ontem (Fechamento)", "Últimas 24h"],
    index=0
)

# Tradução para o código (Backend)
# O sistema ainda usa 'auto', 'ontem', '24h', mas o usuário vê nomes bonitos
mapa_modos = {
    "Hoje (Tempo Real)": "auto",       # A lógica inteligente continua aqui
    "Ontem (Fechamento)": "ontem",
    "Últimas 24h": "24h"
}
modo_codigo = mapa_modos[opcao_visualizacao]

# Badge de Arquitetura (Mantido, pois conta ponto no Edital)
st.sidebar.markdown("---")
st.sidebar.markdown(
    """
    <div style="background-color: #e8f5e9; padding: 10px; border-radius: 5px; border: 1px solid #4caf50; text-align: center;">
        <small style="color: #2e7d32; font-weight: bold;">📡 Conexão Ativa</small><br>
        <span style="font-size: 11px; color: #333;">Edge ➡ UNS ➡ Cloud</span>
    </div>
    """, 
    unsafe_allow_html=True
)

st.sidebar.caption(f"💰 Preço Venda: R$ {PRECO_VENDA:.2f}")
st.sidebar.caption(f"📉 Custo Est.: R$ {CUSTO_POR_TIJOLO:.2f}")

# ---------------------------
# 4. PROCESSAMENTO E CÁLCULOS (AGORA COM MODO_CODIGO DEFINIDO)
# ---------------------------

# pré-process
tele_agg = aggregate_by_period(tele_df)

# CÁLCULOS PRINCIPAIS (Agora modo_codigo existe!)
pecas, refugo, defeitos, temp, periodo_desc = compute_kpis(prod_df, tele_df, modo_codigo)
refugo_turno = compute_refugo_by_turno(prod_df)
pareto = pareto_paradas(evt_df)
scatter_df = build_pressure_humidity_scatter(tele_df)
evt_criticos = aggregate_events(evt_df)
alerts = check_alerts(tele_agg)

# Novos KPIs
DISP, PERF, QUAL, OEE = compute_oee_kpis(prod_df, tele_df, modo_periodo=modo_codigo)
custo_energetico_peca, custo_total_energia = compute_energy_cost(prod_df, PRECO_VENDA, CUSTO_POR_TIJOLO)

# KPIs de Confiabilidade (MTTR/MTBF)
MTTR, MTBF = calculate_mttr_mtbf(evt_df)

# Dados Gold
kpis_gold_df = load_gold_kpis()


# ---------------------------
# 5. PÁGINAS
# ---------------------------

# ---------- RESUMO (LUCRO) ----------
if pagina == "📊 Visão Geral da Fábrica":
    st.title("📊 Visão Geral da Fábrica")
    st.subheader(f"KPIs de Operação – {periodo_desc}")
    
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Peças Produzidas", f"{pecas:,}".replace(",", "."))
    c2.metric("Refugo Total (un)", f"{refugo:,}".replace(",", "."))
    c3.metric("Defeitos Detectados (telemetria)", f"{defeitos:,}")
    c4.metric("Temp. Média Matriz", f"{temp:.1f} °C" if not pd.isna(temp) else "N/D")

    st.markdown("---")
    st.subheader("Eficiência Global (OEE)")
    
    o1, o2, o3, o4 = st.columns(4)
    o1.metric("Disponibilidade (D)", f"{DISP*100:.1f}%", help="Tempo Operando / Tempo Programado")
    o2.metric("Performance (P)", f"{PERF*100:.1f}%", help="Produção Real / Capacidade Nominal")
    o3.metric("Qualidade (Q)", f"{QUAL*100:.1f}%", help="Peças Boas / Total Produzido")
    o4.metric("OEE Global (D x P x Q)", f"{OEE*100:.1f}%", help="Eficiência Total da Fábrica")

    st.markdown("---")
    st.subheader("Custo Operacional")
    c_e1, c_e2 = st.columns(2)
    c_e1.metric("Custo Energético por Peça", f"R$ {custo_energetico_peca:.3f}")
    c_e2.metric("Custo Total de Energia (Período)", f"R$ {custo_total_energia:,.2f}".replace(",", "."))

    st.markdown("---")
    st.subheader("Tendência de Produção (acumulado)")
    if prod_df is not None and not prod_df.empty and "pecas_produzidas" in prod_df.columns:
        df_acc = prod_df.sort_values("timestamp").copy()
        # Acumula a produção hora a hora para cada máquina
        # Agrupa por Linha ISA-95 e Máquina (Equipamento)
        df_acc["acumulado"] = df_acc.groupby("isa95_equipamento")["pecas_produzidas"].cumsum()
        
        fig = px.line(df_acc, x="timestamp", y="acumulado", color="isa95_equipamento", 
                      title="Produção Acumulada por Equipamento (ISA-95)",
                      labels={"timestamp": "Data/Hora", "acumulado": "Peças Produzidas", "isa95_equipamento": "Equipamento"})
        fig.update_layout(template="plotly_white", hovermode="x unified", yaxis_title="Peças acumuladas")
        st.plotly_chart(fig, use_container_width=True)
        
        st.markdown(dedent("""
            *O que isso significa (direto):* - Mostra a corrida de produção entre os **Equipamentos (ISA-95)** ao longo do tempo.  
            - A diferença entre as linhas mostra claramente qual equipamento é mais eficiente.
        """))
        with st.expander("Detalhes técnicos (engenharia)"):
            st.write(dedent("""
                - Fonte de dados: Histórico de Produção (producao_silver.csv).  
                - Agregação: Horária.  
                - Diferença de inclinação = Diferença de OEE.
            """))
    else:
        st.info("Dados de produção insuficientes para a linha do tempo.")


# ---------- ONDE ESTÁ MEU LUCRO (VERSÃO PROFISSIONAL E SIMPLIFICADA) ----------
elif pagina == "💰 Perdas Financeiras":
    st.title("💰 Análise de Perdas Financeiras")
    st.markdown("Identifique onde o dinheiro está sendo perdido (Refugo vs. Ineficiência).")

    # --- 1. CÁLCULOS BASE ---
    total_produzido = prod_df["pecas_produzidas"].sum() if "pecas_produzidas" in prod_df else 0
    total_refugo = prod_df["pecas_refugadas"].sum() if "pecas_refugadas" in prod_df else 0
    total_boas = total_produzido - total_refugo

    # Faturamento real
    faturamento_real = total_boas * PRECO_VENDA
    dinheiro_lixo = total_refugo * PRECO_VENDA

    # Eficiência real
    eficiencia_real = (total_boas / total_produzido) * 100 if total_produzido > 0 else 0

    # Potencial estimado (20% acima do real)
    faturamento_potencial = faturamento_real * 1.20
    perda_por_ineficiencia = faturamento_potencial - faturamento_real

    # --- 2. PAINEL EXECUTIVO ---
    k1, k2, k3 = st.columns(3)

    k1.metric("Eficiência Real da Fábrica", f"{eficiencia_real:.1f}%", delta="Eficiência Operacional")
    k2.metric("Dinheiro no Lixo (Refugo)", f"R$ {dinheiro_lixo:,.2f}".replace(",", "."), delta="- Perda direta", delta_color="inverse")
    k3.metric("Perda Oculta (Ineficiência)", f"R$ {perda_por_ineficiencia:,.2f}".replace(",", "."), delta="- Potencial não capturado", delta_color="inverse")

    st.markdown("---")

    # --- 3. SIMULADOR DE GANHO REAL ---
    st.subheader("🔮 Simulador de Ganhos com Melhoria de Eficiência")

    melhoria = st.slider("Melhoria de Eficiência (%)", 1, 50, 10)

    receita_extra = faturamento_real * (melhoria / 100)

    st.markdown(f"""
    <div style="background-color:#e8f5e9;padding:25px;border-radius:10px;margin-top:15px;border-left:6px solid #2e7d32;">
        <h3 style="color:#2e7d32;margin:0;">Receita Extra Projetada</h3>
        <p style="font-size:28px;color:#1b5e20;font-weight:bold;margin:0;">+ R$ {receita_extra:,.2f}</p>
        <p style="color:#555;margin-top:10px;">(Se a eficiência subir para <b>{eficiencia_real + melhoria:.1f}%</b> )</p>
    </div>
    """, unsafe_allow_html=True)

    st.markdown("---")

    # --- 4. AÇÕES PRÁTICAS PARA GERAR O GANHO ---
    st.subheader("🛠️ O que fazer na fábrica para capturar essa Receita Extra?")

    st.markdown("""
    Para que a melhoria de eficiência realmente gere ganho financeiro, recomenda-se:

    ### ✅ 1. Reduzir Paradas e Microparadas
    * Organizar manutenção preventiva semanal  
    * Trocar sensores instáveis (principalmente pressão e temperatura)

    ### ✅ 2. Reduzir Refugo (Perda Direta)
    * Manter temperatura estável da matriz (evitar picos > 65 °C)  
    * Garantir pressão acima de 12 MPa  
    * Controlar umidade das peças antes da prensa

    ### ✅ 3. Aumentar Produção por Hora
    * Padronizar setup → Operador sempre iniciar com mesmos parâmetros  
    * Automatizar alarmes de limites (telemetria já tem!)

    ### ✅ 4. Atuar na Máquina Crítica
    * A Máquina 02 (a pior) deve ser o foco  
    * Reduzir defeitos nela aumenta o ganho estimado imediatamente  

    ### 🎯 Ação Direta
    Se implementar **metas operacionais de eficiência diária**, o ganho calculado acima deixa de ser uma simulação e vira **dinheiro real no caixa**.
    """)

    st.success("💡 Quanto maior a consistência diária, maior a captura do potencial financeiro da fábrica.")



# ---------- QUALIDADE (CORRIGIDA E BLINDADA) ----------
elif pagina == "📉 Qualidade & Refugo":
    st.title("📉 Controle de Qualidade & Refugo")
    st.markdown("Diagnóstico de causas raízes e volume de desperdício por máquina.")

    # --- 0. DETECÇÃO INTELIGENTE DE COLUNAS ---
    # Descobre qual o nome da coluna de máquina (maquina_id, isa95_equipamento, etc.)
    col_maq_prod = "maquina_id"
    if "isa95_equipamento" in prod_df.columns: col_maq_prod = "isa95_equipamento"
    elif "id_maquina" in prod_df.columns: col_maq_prod = "id_maquina"

    col_maq_tele = "maquina_id"
    if "isa95_equipamento" in tele_df.columns: col_maq_tele = "isa95_equipamento"
    elif "id_maquina" in tele_df.columns: col_maq_tele = "id_maquina"

    # --- 1. KPIs DE IMPACTO ---
    total_refugo = int(prod_df["pecas_refugadas"].sum())
    custo_refugo = total_refugo * PRECO_VENDA
    
    c1, c2 = st.columns(2)
    c1.metric("Peças Perdidas (Total)", f"{total_refugo:,}".replace(",", "."), delta="Refugo Acumulado", delta_color="inverse")
    c2.metric("Prejuízo Financeiro", f"R$ {custo_refugo:,.2f}", delta="Perda Monetária", delta_color="inverse")

    st.markdown("---")

    # --- 2. TENDÊNCIA TEMPORAL ---
    st.subheader("📈 Evolução Diária de Defeitos")
    
    if tele_df is not None and not tele_df.empty:
        df_trend = tele_df.copy()
        df_trend["Data"] = df_trend["timestamp"].dt.date
        
        # Agrupa usando a coluna detectada (col_maq_tele)
        if col_maq_tele in df_trend.columns:
            trend_data = df_trend.groupby(["Data", col_maq_tele])["flag_defeito"].sum().reset_index()
            
            fig_trend = px.line(trend_data, x="Data", y="flag_defeito", color=col_maq_tele,
                                title="Quantidade de Defeitos por Dia",
                                labels={"flag_defeito": "Qtd. Defeitos", "Data": "Dia do Mês"},
                                markers=True)
            fig_trend.update_layout(template="plotly_white", hovermode="x unified")
            st.plotly_chart(fig_trend, use_container_width=True)
            st.caption("💡 **Dica:** Picos altos indicam dias onde a máquina operou descalibrada.")
        else:
            st.warning(f"Coluna de máquina '{col_maq_tele}' não encontrada na telemetria.")
    else:
        st.info("Sem dados de telemetria.")

    st.markdown("---")

    # --- 3. DIAGNÓSTICO: QUEM E POR QUE? ---
    c_who, c_why = st.columns(2)

    with c_who:
        st.subheader("🔍 Onde está o problema?")
        
        if not prod_df.empty:
            # Agrupa usando a coluna detectada (col_maq_prod)
            refugo_maq = prod_df.groupby(col_maq_prod)["pecas_refugadas"].sum().reset_index()
            
            # Cria Label bonita
            try:
                refugo_maq["Nome"] = refugo_maq[col_maq_prod].apply(lambda x: f"Equip. {x}")
            except:
                refugo_maq["Nome"] = refugo_maq[col_maq_prod].astype(str)
            
            fig_bar = px.bar(refugo_maq, x="Nome", y="pecas_refugadas", 
                             title="Total de Refugo por Máquina",
                             text_auto=True,
                             color="pecas_refugadas", 
                             color_continuous_scale=["green", "red"])
            st.plotly_chart(fig_bar, use_container_width=True)
        else:
            st.info("Sem dados de produção.")

    with c_why:
        st.subheader("📊 Causa Provável (Técnica)")
        
        # --- CORREÇÃO: Função blindada contra erro de tipo ---
        def classificar(row):
            try:
                # Se não for defeito, ignora
                if int(row.get("flag_defeito", 0)) == 0: return None
                
                # Força conversão para float para evitar erro de comparação
                p = float(row.get("pressao_mpa", 15))
                t = float(row.get("temp_matriz_c", 60))
                u = float(row.get("umidade_pct", 12))
                
                if p < 12: return "Pressão Baixa (<12)"
                if t > 65: return "Temp. Alta (>65)"
                if u > 14: return "Umidade Alta (>14)"
                return "Outros"
            except Exception:
                return "Erro de Leitura" # Fallback seguro

        if not tele_df.empty:
            # Filtra apenas defeitos
            df_causes = tele_df[tele_df["flag_defeito"] == 1].copy()
            
            if not df_causes.empty:
                # Aplica a classificação
                df_causes["Causa"] = df_causes.apply(classificar, axis=1)
                
                # Remove nulos e conta
                counts = df_causes["Causa"].value_counts().reset_index()
                counts.columns = ["Causa", "Qtd"]
                
                # Gráfico
                fig_cause = px.bar(counts, x="Qtd", y="Causa", orientation='h', 
                                 title="Top Causas Técnicas",
                                 text_auto=True,
                                 color="Qtd", color_continuous_scale="Reds")
                st.plotly_chart(fig_cause, use_container_width=True)
            else:
                st.success("Sem defeitos registrados na amostra recente.")

    # --- 4. RECOMENDAÇÕES ---
    st.markdown("---")
    st.success("✅ **Plano de Ação:** O diagnóstico aponta instabilidade. Verifique a calibração da máquina com maior barra vermelha no gráfico à esquerda.")

# ---------- MANUTENÇÃO ----------
elif pagina == "🔧 Paradas & Confiabilidade":
    st.title("🔧 Gestão de Paradas & Confiabilidade")
    st.markdown("Indicadores de MTTR, MTBF e Pareto de causas de parada.")

    # --- 0. PREPARAÇÃO DOS DADOS (CORREÇÃO DO ERRO) ---
    if evt_df is not None and not evt_df.empty:
        df_maint = evt_df.copy()
        
        # Padroniza nome da coluna de causa (resolve o KeyError 'motivo')
        col_causa = "evento" if "evento" in df_maint.columns else ("descricao" if "descricao" in df_maint.columns else None)
        
        if col_causa:
            df_maint = df_maint.rename(columns={col_causa: "Causa"})
            
            # Garante coluna de duração
            if "duracao_min" not in df_maint.columns:
                df_maint["duracao_min"] = 60.0 # Fallback
            else:
                df_maint["duracao_min"] = df_maint["duracao_min"].fillna(60.0)
                
            # --- 1. CÁLCULO DE KPIs DE CONFIABILIDADE ---
            # Filtra apenas paradas (Severidade Média/Alta ou códigos específicos)
            # Assumindo que tudo no log de eventos é uma parada/intervenção
            total_paradas = len(df_maint)
            tempo_total_parado = df_maint["duracao_min"].sum()
            
            # MTTR (Mean Time To Repair) = Tempo Total Parado / Número de Falhas
            mttr = tempo_total_parado / total_paradas if total_paradas > 0 else 0
            
            # MTBF (Mean Time Between Failures)
            # Tempo total de calendário (estimado pelo range de datas)
            inicio_ops = df_maint["timestamp"].min()
            fim_ops = df_maint["timestamp"].max()
            horas_totais = (fim_ops - inicio_ops).total_seconds() / 3600 if pd.notnull(inicio_ops) else 720
            tempo_disponivel_min = (horas_totais * 60) - tempo_total_parado
            
            mtbf = tempo_disponivel_min / total_paradas if total_paradas > 0 else 0
            
            # Disponibilidade Técnica (baseada em eventos)
            disponibilidade_tec = (tempo_disponivel_min / (horas_totais * 60)) * 100

            # --- EXIBIÇÃO DOS KPIs ---
            m1, m2, m3, m4 = st.columns(4)
            m1.metric("MTTR (Tempo Médio Reparo)", f"{mttr:.1f} min", help="Meta: < 60 min", delta_color="inverse")
            m2.metric("MTBF (Tempo Entre Falhas)", f"{mtbf/60:.1f} horas", help="Meta: > 48h")
            m3.metric("Disponibilidade Técnica", f"{disponibilidade_tec:.1f}%", help="Tempo que a máquina ficou disponível")
            m4.metric("Total Horas Paradas", f"{tempo_total_parado/60:.1f} h", delta="Acumulado", delta_color="inverse")
            
            st.markdown("---")

            # --- 2. PARETO DE IMPACTO (GRÁFICO DE BARRAS) ---
            c_pareto, c_timeline = st.columns([1, 1])
            
            with c_pareto:
                st.subheader("📊 Onde perdemos mais tempo? (Pareto)")
                # Agrupa por Causa e soma o tempo (Impacto real)
                pareto_data = df_maint.groupby("Causa")["duracao_min"].sum().reset_index()
                pareto_data = pareto_data.sort_values("duracao_min", ascending=True) # Ascendente para barra horizontal ficar certa
                
                fig_p = px.bar(pareto_data, x="duracao_min", y="Causa", orientation='h',
                               title="Top Causas por Tempo Total de Parada (min)",
                               text_auto=".0f",
                               color="duracao_min", color_continuous_scale="Reds")
                fig_p.update_layout(template="plotly_white", xaxis_title="Minutos Parados")
                st.plotly_chart(fig_p, use_container_width=True)
                
            with c_timeline:
                st.subheader("📅 Linha do Tempo de Falhas")
                st.markdown("Identifique se as falhas estão ficando mais frequentes.")
                
                # Gráfico de dispersão no tempo
                fig_time = px.scatter(df_maint, x="timestamp", y="Causa", 
                                      size="duracao_min", color="severidade",
                                      title="Ocorrências no Tempo (Tamanho = Duração)",
                                      color_discrete_map={"Alta": "red", "Média": "orange", "Baixa": "green"})
                fig_time.update_layout(template="plotly_white")
                st.plotly_chart(fig_time, use_container_width=True)

            # --- 3. TABELA DETALHADA E RECOMENDAÇÕES ---
            st.markdown("---")
            st.subheader("📋 Log de Intervenções Recentes")
            
            # Tabela limpa
            cols_show = ["timestamp", "Causa", "severidade", "duracao_min", "origem", "maquina_id"]
            cols_existentes = [c for c in cols_show if c in df_maint.columns]
            
            st.dataframe(
                df_maint.sort_values("timestamp", ascending=False).head(10)[cols_existentes],
                use_container_width=True,
                hide_index=True
            )
            
            # Insight Automático
            top_cause = pareto_data.iloc[-1]["Causa"] # Pega o último (maior) pois ordenamos ascendente
            st.info(f"💡 **Insight de Gestão:** A causa **'{top_cause}'** é a maior ofensora, consumindo a maior parte do tempo de manutenção. Recomenda-se análise de causa raiz (5 Porquês) especificamente para este item.")

        else:
            st.error("Erro de Dados: Coluna de 'Evento' ou 'Descrição' não encontrada no arquivo de eventos.")
    else:
        st.info("Sem dados de eventos de manutenção registrados.")


# ---------- TELEMETRIA ----------
elif pagina == "📡 Sensores em Tempo Real":
    st.title("📡 Monitoramento de Sensores (IoT)")
    st.markdown("Acompanhamento ciclo a ciclo de Pressão e Temperatura para engenharia.")
    
    # Verifica se o dataframe não está vazio
    if not scatter_df.empty:
        
        # CORREÇÃO AQUI: troquei y="umidade" por y="umidade_pct"
        # Também adicionei verificação para garantir que as colunas existem
        col_y = "umidade" if "umidade" in scatter_df.columns else "umidade_pct"
        col_x = "pressao_mpa"
        
        fig_sc = px.scatter(scatter_df, x=col_x, y=col_y, color="status",
                            title="Pressão x Umidade — Verde = OK / Vermelho = Defeito",
                            labels={col_x: "Pressão (MPa)", col_y: "Umidade (%)"},
                            color_discrete_map={"OK": "green", "Defeito": "red"})
        
        fig_sc.update_layout(template="plotly_white")
        st.plotly_chart(fig_sc, use_container_width=True)
        
        st.markdown(dedent("""
            *Explicação direta:* - Pontos vermelhos mostram onde estamos perdendo dinheiro (peças defeituosas).  
            - Ação: se pressão/umidade saírem da "zona verde", intervir.
        """))
        
        with st.expander("Detalhes técnicos (engenharia)"):
            st.write(dedent("""
                - Recomenda-se amostragem física das peças nas zonas vermelhas para validar limites.
                - Gráfico gerado com base na telemetria histórica (amostra).
            """))
    else:
        st.info("Dados de pressão/umidade insuficientes para gerar o mapa operacional.")


# ---------- SIMULADOR DE QUALIDADE (ML) ----------
elif pagina == "🤖 Inteligência Artificial":
    st.title("🤖 Simulador de Qualidade (IA Preditiva)")
    st.markdown("Utilize o modelo de IA para testar parâmetros e prever riscos antes de configurar a máquina.")
    st.subheader("Ajuste os Parâmetros de Entrada")
    
    # Valores médios/meta para o Sr. Roberto
    PRESSAO_META = 15.0
    UMIDADE_META = 12.0
    TEMP_META = 60.0
    
    col_p, col_u, col_t = st.columns(3)
    
    pressao = col_p.slider("Pressão (MPa)", min_value=10.0, max_value=20.0, value=PRESSAO_META, step=0.1)
    umidade = col_u.slider("Umidade (%)", min_value=5.0, max_value=20.0, value=UMIDADE_META, step=0.1)
    temperatura = col_t.slider("Temperatura (°C)", min_value=50.0, max_value=70.0, value=TEMP_META, step=0.1)
    
    prob_defeito = predict_defeito_prob(pressao, umidade, temperatura)
    
    st.markdown("---")
    st.subheader("Relógio de Risco (Previsão do Modelo)")
    
    if prob_defeito is not None:
        prob_pct = prob_defeito * 100
        
        # Lógica de cores para o "Relógio de Risco"
        if prob_pct < 5:
            cor = "green"
            status = "Baixo Risco"
            emoji = "✅"
        elif prob_pct < 15:
            cor = "orange"
            status = "Risco Moderado"
            emoji = "⚠️"
        else:
            cor = "red"
            status = "Alto Risco"
            emoji = "🚨"
            
        st.markdown(f"""
        <div style="background-color: #F7F9FB; border-radius: 10px; padding: 20px; text-align: center; border: 3px solid {cor};">
            <p style="font-size: 18px; color: #555;">Probabilidade de Defeito:</p>
            <p style="font-size: 48px; font-weight: 900; color: {cor}; margin: 0;">{emoji} {prob_pct:.2f}%</p>
            <p style="font-size: 24px; font-weight: 700; color: {cor}; margin-top: 5px;">{status}</p>
        </div>
        """, unsafe_allow_html=True)
        
        st.markdown(dedent(f"""
            *Explicação para o Sr. Roberto:* - O modelo de Inteligência Artificial (IA) prevê que, com **Pressão de {pressao:.1f} MPa**, **Umidade de {umidade:.1f}%** e **Temperatura de {temperatura:.1f} °C**, a chance de produzir uma peça defeituosa é de **{prob_pct:.2f}%**.
            - **Ação:** Mantenha os parâmetros na zona verde (abaixo de 5%) para garantir a qualidade.
        """))
        
    else:
        st.warning("O modelo de Machine Learning não pôde ser carregado. Verifique o arquivo `rf_defeito.joblib`.")

# ---------- EVENTOS ----------
elif pagina == "📋 Histórico de Alertas":
    st.title("📋 Histórico Completo de Alertas")
    st.markdown("Log auditável de todas as ocorrências, alarmes e paradas registradas.")
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