from pyspark.sql import SparkSession
import os
from dotenv import load_dotenv

# Carrega suas senhas do .env
load_dotenv()

def criar_spark_session():
    """
    Sessão Spark configurada para LEITURA (Visualização).
    BLINDADA contra erros de timeout no Windows (Bug '60s').
    """
    os.environ['OBJC_DISABLE_INITIALIZE_FORK_SAFETY'] = 'YES'
    
    return SparkSession.builder \
        .appName("DataMasters_Leitor_Universal") \
        .master("local[*]") \
        .config("spark.hadoop.fs.s3a.endpoint", os.getenv("MINIO_ENDPOINT")) \
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ROOT_USER")) \
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_ROOT_PASSWORD")) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .config("spark.hadoop.fs.s3a.threads.keepalivetime", "60") \
        .config("spark.hadoop.fs.s3a.connection.establish.timeout", "5000") \
        .config("spark.hadoop.fs.s3a.connection.timeout", "10000") \
        .config("spark.hadoop.fs.s3a.socket.timeout", "10000") \
        .config("spark.hadoop.fs.s3a.attempts.maximum", "1") \
        .config("spark.hadoop.fs.s3a.multipart.purge.age", "86400") \
        .getOrCreate()

def inspecionar(spark, nome_camada, caminho_bucket, formato):
    print(f"\n{'='*20} 🔍 INSPECIONANDO: {nome_camada.upper()} {'='*20}")
    
    try:
        if formato == "json":
            df = spark.read.json(caminho_bucket)
        elif formato == "parquet":
            df = spark.read.parquet(caminho_bucket)
            
        total = df.count()
        print(f"📊 Total de registros encontrados: {total}")
        
        if total > 0:
            print("📋 Estrutura dos dados (Schema):")
            df.printSchema()
            
            print("👀 Amostra dos dados:")
            df.show(10, truncate=False)
        else:
            print("⚠️ A tabela está vazia.")
        
    except Exception as e:
        print(f"⚠️ Erro ao ler a camada {nome_camada}: {e}")

if __name__ == "__main__":
    spark = criar_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    # 1. BRONZE (Dados Brutos - JSON)
    inspecionar(spark, "Bronze", "s3a://bronze/raw/*/*/*/*.json", "json")

    # 2. SILVER (Dados Tratados - Parquet)
    inspecionar(spark, "Silver", "s3a://silver/transacoes_seguras", "parquet")

    # 3. GOLD (Dados Agregados - Parquet)
    inspecionar(spark, "Gold", "s3a://gold/vendas_por_estado", "parquet")

    spark.stop()