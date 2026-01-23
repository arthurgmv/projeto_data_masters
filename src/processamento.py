import sys
import os
from pyspark.sql.functions import col, regexp_replace

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from config import Config
from data_quality import DataQuality
from ui import print_tab 

def processar_silver():
    print("🚀 Iniciando processamento SILVER (Com Data Quality)...")
    
    # 1. Setup
    spark = Config.get_spark_session("SilverJob")
    dq = DataQuality(spark)
    
    # 2. Leitura (Bronze)
    path_bronze = f"{Config.get_base_path('bronze')}/raw/*.json" 
    
    try:
        df = spark.read.json(path_bronze)
    except Exception as e:
        print(f"⚠️ Aviso: Tentando leitura recursiva. Erro anterior: {e}")
        path_bronze_recursive = f"{Config.get_base_path('bronze')}/raw/*/*/*/*.json"
        df = spark.read.json(path_bronze_recursive)

    
    # --- CHECKPOINT 1: OBSERVABILITY ---
    dq.count_rows(df, "Bronze (Raw)")
    
    # --- CHECKPOINT 2: DATA QUALITY (Validações) ---
    print("\n--- 🕵️ Executando Testes de Qualidade ---")
    # Verifica se IDs ou Nomes vieram vazios
    dq.check_nulls(df, ["id_transacao", "cliente_nome"])
    # Verifica valores monetários
    dq.check_positive_values(df, ["valor"])
    print("------------------------------------------\n")

    # 3. Transformação (LGPD - Privacy by Design)
    print("🛡️ Aplicando máscaras LGPD...")
    df_silver = df.withColumn(
        "cpf_mascarado", 
        regexp_replace(col("cliente_cpf"), r"\d{3}\.\d{3}\.\d{3}", "***.***.***")
    ).withColumn(
        "cartao_tokenizado",
        regexp_replace(col("cartao"), r"^\d{12}", "**** **** **** ")
    ).drop("cliente_cpf", "cartao")

    # 4. Escrita (Silver)
    path_silver = f"{Config.get_base_path('silver')}/transacoes_seguras"
    print(f"💾 Salvando em: {path_silver}")
    df_silver.write.mode("overwrite").parquet(path_silver)
    
    # 5. Visualização (UI)
    print_tab(df_silver, "SILVER (Tratada & Anonimizada)")

    print("✅ Sucesso! Pipeline Silver concluído com validações.")

if __name__ == "__main__":
    processar_silver()