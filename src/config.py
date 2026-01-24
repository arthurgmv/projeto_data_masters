import os
from pyspark.sql import SparkSession
from dotenv import load_dotenv

load_dotenv()

class Config:
    IS_DATABRICKS = "DATABRICKS_RUNTIME_VERSION" in os.environ

    @staticmethod
    def get_base_path(camada):

        if Config.IS_DATABRICKS:
            return f"/tmp/data_masters/{camada}"
        else:
            return f"s3a://{camada}"

    @staticmethod
    def get_spark_session(app_name="DataMasters_Job"):
        if Config.IS_DATABRICKS:
            return SparkSession.builder.getOrCreate()
        
        else:
            os.environ['OBJC_DISABLE_INITIALIZE_FORK_SAFETY'] = 'YES'
            
            return SparkSession.builder \
                .appName(app_name) \
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