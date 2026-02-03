import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, current_timestamp
from dotenv import load_dotenv

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from data_quality import DataQuality
from ui import print_tab

load_dotenv()

def init_spark():
    """
    Inicia a sessão Spark conectada ao Data Lake (MinIO).
    Configura drivers S3 e força o uso do Python correto.
    """
    print("🔌 Conectando ao Data Lake (MinIO)...")
    
    spark = SparkSession.builder \
        .appName("SilverJob_LGPD") \
        .master("local[*]") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.11.1026") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://datalake:9000") \
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ROOT_USER", "minioadmin")) \
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.sql.execution.pyspark.udf.faulthandler.enabled", "true") \
        .getOrCreate()
        
    spark.sparkContext.setLogLevel("ERROR")
    return spark

def processar_silver():
    print("🚀 Iniciando processamento SILVER (LGPD + Data Quality)...")
    
    spark = init_spark()
    dq = DataQuality(spark)
    
    # 1. LEITURA (Bronze do Data Lake)
    path_bronze = "s3a://bronze/raw/"
    
    try:
        print(f"📂 Lendo dados de: {path_bronze} (Recursivo)")
        df = spark.read.option("recursiveFileLookup", "true").json(path_bronze)
        
        if df.rdd.isEmpty():
            raise Exception("Dataframe vazio")
            
        print(f"✅ Leitura concluída. Total registros: {df.count()}")
        
    except Exception as e:
        print(f"⚠️ Aviso: Não foi possível ler do MinIO ou Bronze vazia ({e}).")
        print("   -> Gerando dados DUMMY em memória para validar o fluxo.")
        data = [
            ("1", "Arthur", "123.456.789-00", "1234567812345678", 100.0),
            ("2", "Teste", "111.222.333-44", "0000000000000000", -50.0), # Valor Negativo
            ("3", "Maria", "999.888.777-66", "1111222233334444", 200.0)
        ]
        df = spark.createDataFrame(data, ["id_transacao", "cliente_nome", "cliente_cpf", "cartao", "valor"])

    # 2. DATA QUALITY
    print("\n--- 🕵️ Executando Testes de Qualidade ---")
    qualidade_ok = dq.check_positive_values(df, ["valor"])
    
    if not qualidade_ok:
        print("❌ ALERTA: Encontrados valores negativos! Filtrando...")
        df = df.filter(col("valor") >= 0)
    else:
        print("✅ Dados Monetários Aprovados.")
    print("------------------------------------------\n")

    # 3. TRANSFORMAÇÃO (LGPD)
    print("🛡️ Aplicando máscaras LGPD...")
    cols = df.columns
    df_silver = df
    
    if "cliente_cpf" in cols:
        df_silver = df_silver.withColumn(
            "cpf_mascarado", 
            regexp_replace(col("cliente_cpf"), r"\d{3}\.\d{3}\.\d{3}", "***.***.***")
        ).drop("cliente_cpf")
        
    if "cartao" in cols:
        df_silver = df_silver.withColumn(
            "cartao_tokenizado",
            regexp_replace(col("cartao"), r"^\d{12}", "**** **** **** ")
        ).drop("cartao")

    df_silver = df_silver.withColumn("data_processamento", current_timestamp())

    # 4. ESCRITA (Silver)
    path_silver = "s3a://silver/transacoes_seguras"
    
    try:
        print(f"💾 Salvando dados tratados em: {path_silver}")
        df_silver.write.mode("overwrite").parquet(path_silver)
        print("✅ Gravação no MinIO concluída!")

        print_tab(df_silver, "SILVER (Dados Tratados/LGPD)")
        
    except Exception as e:
        print(f"❌ Erro ao gravar no MinIO: {e}")

    spark.stop()

if __name__ == "__main__":
    processar_silver()