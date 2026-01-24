import sys
import os


sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from config import Config
from ui import print_tab

def inspecionar(spark, nome_camada, caminho_bucket, formato):
    print(f"\n🔍 Acessando camada: {nome_camada}...")
    
    try:
        df = None
        
        if formato == "json":
            try:
                df = spark.read.json(caminho_bucket)
            except Exception:
                print("⚠️  Tentando leitura recursiva profunda...")
                caminho_recursivo = caminho_bucket.replace("*.json", "*/*/*/*.json")
                df = spark.read.json(caminho_recursivo)
                
        elif formato == "parquet":
            df = spark.read.parquet(caminho_bucket)
            
        # Exibição Visual (UI)
        if df and df.count() > 0:
            print_tab(df, f"AUDITORIA: {nome_camada.upper()}")
        else:
            print(f"⚠️ A tabela {nome_camada} está vazia ou não foi encontrada.")
        
    except Exception as e:
        print(f"❌ Erro ao ler a camada {nome_camada}: {e}")

def main():
    # Usa a Config centralizada (Híbrida)
    spark = Config.get_spark_session("LeitorUniversal")
    spark.sparkContext.setLogLevel("WARN")

    # 1. BRONZE (Dados Brutos - JSON)
    path_bronze = f"{Config.get_base_path('bronze')}/raw/*.json"
    inspecionar(spark, "Bronze (Raw)", path_bronze, "json")

    # 2. SILVER (Dados Tratados - Parquet)
    path_silver = f"{Config.get_base_path('silver')}/transacoes_seguras"
    inspecionar(spark, "Silver (Trusted)", path_silver, "parquet")

    # 3. GOLD (Dados Agregados - Parquet)
    path_gold = f"{Config.get_base_path('gold')}/vendas_por_estado"
    inspecionar(spark, "Gold (Refined)", path_gold, "parquet")

    if not Config.IS_DATABRICKS:
        spark.stop()

if __name__ == "__main__":
    main()