import pytest
import logging
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from src.data_quality import DataQuality

# --- FIXTURES (DADOS FALSOS) ---
@pytest.fixture
def mock_df_com_erro(spark):
    data = [
        ("t1", 100.0),
        ("t2", -50.0),  # <--- O ERRO ESTÁ AQUI!
        ("t3", 20.0)
    ]
    schema = StructType([
        StructField("id", StringType(), True),
        StructField("valor", DoubleType(), True)
    ])
    return spark.createDataFrame(data, schema)

@pytest.fixture
def mock_df_limpo(spark):
    data = [("t1", 100.0), ("t3", 20.0)]
    schema = StructType([
        StructField("id", StringType(), True),
        StructField("valor", DoubleType(), True)
    ])
    return spark.createDataFrame(data, schema)

# --- TESTES UNITÁRIOS ---

def test_deve_detectar_valores_negativos(spark, mock_df_com_erro, caplog):

    dq = DataQuality(spark)

    with caplog.at_level(logging.ERROR):

        dq.check_positive_values(mock_df_com_erro, ["valor"])

    assert "ERRO CRÍTICO" in caplog.text
    print("\n✅ Teste passou: O Log de Erro foi capturado!")

def test_deve_aprovar_dados_limpos(spark, mock_df_limpo, caplog):

    dq = DataQuality(spark)
    
    caplog.clear()
    
    with caplog.at_level(logging.ERROR):
        dq.check_positive_values(mock_df_limpo, ["valor"])
    

    assert "ERRO CRÍTICO" not in caplog.text
    print("\n✅ Teste passou: Nenhum erro foi logado para dados limpos!")