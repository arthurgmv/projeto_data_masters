from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count, desc, round
import os
from dotenv import load_dotenv

load_dotenv()

def criar_spark_session():
    """
    Sessão Spark Otimizada para Analytics (Gold).
    Configuração idêntica à Silver para evitar erros no Windows.
    """
    os.environ['OBJC_DISABLE_INITIALIZE_FORK_SAFETY'] = 'YES'

    return SparkSession.builder \
        .appName("DataMasters_Gold_Analytics") \
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

def processar_gold():
    print("🚀 [GOLD] Iniciando agregação de vendas...")
    spark = criar_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    try:
        # 1. LEITURA (Silver - formato Parquet)
        print("📥 Lendo dados tratados da camada SILVER...")
        df_silver = spark.read.parquet("s3a://silver/transacoes_seguras")
        
        # 2. TRANSFORMAÇÃO (Agregação de Negócio)
        print("💰 Calculando Ranking de Vendas por Estado...")
        
        # Agrupa por Estado -> Soma Vendas -> Conta Transações
        df_gold = df_silver.groupBy("estado") \
            .agg(
                sum("valor").alias("total_vendas"),
                count("id_transacao").alias("qtd_transacoes")
            ) \
            .withColumn("total_vendas", round(col("total_vendas"), 2)) \
            .orderBy(desc("total_vendas"))

        # 3. ESCRITA (Salva na Gold)
        print("💾 Salvando relatório na camada GOLD...")
        df_gold.write.mode("overwrite").parquet("s3a://gold/vendas_por_estado")
        
        print("\n--- 🏆 TOP 5 ESTADOS COM MAIS VENDAS ---")
        df_gold.show(5, truncate=False)
        print("✅ Processamento GOLD concluído com sucesso!")

    except Exception as e:
        print(f"❌ Erro no job Gold: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    processar_gold()