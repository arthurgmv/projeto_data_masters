import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark():
    print("\n[SETUP] Iniciando Spark Session...")
    
    spark = SparkSession.builder \
        .master("local[1]") \
        .appName("DataMasters_Tests") \
        .config("spark.sql.shuffle.partitions", "1") \
        .config("spark.ui.showConsoleProgress", "false") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    
    yield spark
    
    print("\n[TEARDOWN] Encerrando Spark...")
    spark.stop()