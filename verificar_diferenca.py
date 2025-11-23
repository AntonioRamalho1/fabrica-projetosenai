import pandas as pd

# Ajuste o caminho se precisar
df = pd.read_csv(r"app/data/processed/producao_silver.csv")

# Agrupa por máquina para ver o total
resumo = df.groupby("maquina_id")["pecas_produzidas"].sum()

print("--- TOTAL ACUMULADO (1 ANO) ---")
print(resumo)

diferenca = resumo[1] - resumo[2]
print(f"\nDIFFERENÇA: {diferenca} peças")