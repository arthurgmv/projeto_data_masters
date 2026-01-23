import sys
import os
from pyspark.sql.functions import col, sum, count, desc, round

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from config import Config
from ui import print_tab 

def processar_gold():
    print("🚀 [GOLD] Iniciando BI (Modo Híbrido)...")
    
    spark = Config.get_spark_session("DataMasters_Gold")
    spark.sparkContext.setLogLevel("WARN")

    try:
        path_silver = f"{Config.get_base_path('silver')}/transacoes_seguras"
        path_gold = f"{Config.get_base_path('gold')}/vendas_por_estado"

        # 1. LEITURA
        print(f"📥 Lendo de: {path_silver}")
        df_silver = spark.read.parquet(path_silver)
        
        # 2. TRANSFORMAÇÃO
        print("💰 Calculando KPIs...")
        df_gold = df_silver.groupBy("estado") \
            .agg(
                sum("valor").alias("total_vendas"),
                count("id_transacao").alias("qtd_transacoes")
            ) \
            .withColumn("total_vendas", round(col("total_vendas"), 2)) \
            .orderBy(desc("total_vendas"))

        # 3. ESCRITA
        print(f"💾 Salvando em: {path_gold}")
        df_gold.write.mode("overwrite").parquet(path_gold)
        
        # 4. VISUALIZAÇÃO (UI)
        print_tab(df_gold, "GOLD (Ranking de Vendas/Estado)")
        
        print("✅ Sucesso!")

    except Exception as e:
        print(f"❌ Erro: {e}")
    finally:
        if not Config.IS_DATABRICKS:
            spark.stop()

if __name__ == "__main__":
    processar_gold()