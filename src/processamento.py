from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace
import os
import sys
from dotenv import load_dotenv

# Carrega as variáveis do arquivo .env
load_dotenv()

def criar_spark_session():
    """
    Configura a sessão do Spark para acessar o MinIO (S3).
    INCLUI CORREÇÕES PARA WINDOWS:
    1. Bug do '60s' (Timeouts numéricos)
    2. Bug do '24h' (Multipart Purge Age)
    3. Erro de Credenciais (Força SimpleAWSCredentialsProvider)
    """
    # Ajuste de compatibilidade para Windows
    os.environ['OBJC_DISABLE_INITIALIZE_FORK_SAFETY'] = 'YES'

    return SparkSession.builder \
        .appName("DataMasters_Silver_Job") \
        .master("local[*]") \
        .config("spark.hadoop.fs.s3a.endpoint", os.getenv("MINIO_ENDPOINT")) \
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ROOT_USER")) \
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_ROOT_PASSWORD")) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.driver.memory", "2g") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .config("spark.hadoop.fs.s3a.threads.keepalivetime", "60") \
        .config("spark.hadoop.fs.s3a.connection.establish.timeout", "5000") \
        .config("spark.hadoop.fs.s3a.connection.timeout", "10000") \
        .config("spark.hadoop.fs.s3a.socket.timeout", "10000") \
        .config("spark.hadoop.fs.s3a.attempts.maximum", "1") \
        .config("spark.hadoop.fs.s3a.multipart.purge.age", "86400") \
        .getOrCreate()

def processar_silver():
    print("🚀 Iniciando processamento SILVER...")
    spark = criar_spark_session()
    
    # Reduzir logs poluídos no terminal
    spark.sparkContext.setLogLevel("WARN")

    try:
        # 1. LEITURA (Bronze)
        print("📥 Lendo dados da camada BRONZE...")
        df_bronze = spark.read.json("s3a://bronze/raw/*/*/*/*.json")
        
        qtd_registros = df_bronze.count()
        print(f"📊 Total de registros encontrados: {qtd_registros}")

        if qtd_registros == 0:
            print("⚠️ Nenhum dado encontrado na Bronze. Rode a ingestão primeiro!")
            return

        # 2. TRANSFORMAÇÃO (Segurança/LGPD)
        print("🛡️ Aplicando mascaramento de dados sensíveis (PII)...")
        
        # Mascarar CPF: Mantém apenas os 2 últimos dígitos (ex: ***.***.***-99)
        df_silver = df_bronze.withColumn(
            "cpf_mascarado", 
            regexp_replace(col("cliente_cpf"), r"\d{3}\.\d{3}\.\d{3}", "***.***.***")
        )
        
        # Tokenizar Cartão: Mostra apenas os últimos 4 (ex: **** **** **** 1234)
        df_silver = df_silver.withColumn(
            "cartao_tokenizado",
            regexp_replace(col("cartao"), r"^\d{12}", "**** **** **** ")
        )

        # Remover colunas originais (perigosas)
        df_silver = df_silver.drop("cliente_cpf", "cartao")

        # 3. ESCRITA (Silver)
        print("💾 Salvando dados tratados na camada SILVER (Parquet)...")
        
        df_silver.write \
            .mode("overwrite") \
            .parquet("s3a://silver/transacoes_seguras")
            
        print("✅ Processamento concluído com sucesso!")
        
        print("\n--- AMOSTRA DOS DADOS TRATADOS ---")
        df_silver.show(5, truncate=False)

    except Exception as e:
        print(f"❌ Erro crítico no processamento: {e}")
        import traceback
        traceback.print_exc()
    finally:
        spark.stop()

if __name__ == "__main__":
    processar_silver()