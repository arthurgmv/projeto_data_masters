from pyspark.sql.functions import col, regexp_replace
from config import Config 

def processar_silver():
    print("🚀 Iniciando processamento SILVER (Modo Híbrido)...")
    
    # 1. Pega a sessão (O Config decide se é Local ou Databricks)
    spark = Config.get_spark_session("DataMasters_Silver")
    spark.sparkContext.setLogLevel("WARN")

    try:
        path_bronze = f"{Config.get_base_path('bronze')}/raw/*/*/*/*.json"
        path_silver = f"{Config.get_base_path('silver')}/transacoes_seguras"

        # 1. LEITURA
        print(f"📥 Lendo de: {path_bronze}")
        df_bronze = spark.read.json(path_bronze)
        
        if df_bronze.count() == 0:
            print("⚠️ Nada encontrado.")
            return

        # 2. TRANSFORMAÇÃO 
        print("🛡️ Aplicando máscaras...")
        df_silver = df_bronze.withColumn(
            "cpf_mascarado", 
            regexp_replace(col("cliente_cpf"), r"\d{3}\.\d{3}\.\d{3}", "***.***.***")
        ).withColumn(
            "cartao_tokenizado",
            regexp_replace(col("cartao"), r"^\d{12}", "**** **** **** ")
        ).drop("cliente_cpf", "cartao")

        # 3. ESCRITA
        print(f"💾 Salvando em: {path_silver}")
        df_silver.write.mode("overwrite").parquet(path_silver)
            
        print("✅ Sucesso!")
        df_silver.show(5, truncate=False)

    except Exception as e:
        print(f"❌ Erro: {e}")
    finally:
        if not Config.IS_DATABRICKS:
            spark.stop()

if __name__ == "__main__":
    processar_silver()