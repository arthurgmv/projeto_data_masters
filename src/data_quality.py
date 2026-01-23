from pyspark.sql import DataFrame
from pyspark.sql.functions import col
import logging

class DataQuality:
    def __init__(self, spark):
        self.spark = spark
        # Configura um logger simples para mostrar mensagens no terminal
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - [DATA QUALITY] - %(message)s', datefmt='%H:%M:%S')
        self.logger = logging.getLogger("DataQuality")

    def check_nulls(self, df: DataFrame, columns: list):
        """Verifica se há valores nulos em colunas críticas"""
        self.logger.info(f"🔍 Verificando Nulos nas colunas: {columns}")
        
        for c in columns:
            null_count = df.filter(col(c).isNull() | (col(c) == "")).count()
            if null_count > 0:
                self.logger.warning(f"⚠️  ALERTA: Coluna '{c}' tem {null_count} registros nulos/vazios!")
            else:
                self.logger.info(f"✅ Coluna '{c}' está íntegra (0 nulos).")
    
    def check_positive_values(self, df: DataFrame, columns: list):
        """Garante que valores numéricos sejam positivos (ex: Vendas)"""
        self.logger.info(f"🔍 Verificando valores negativos: {columns}")
        
        for c in columns:
            negative_count = df.filter(col(c) < 0).count()
            if negative_count > 0:
                self.logger.error(f"🚨 ERRO CRÍTICO: Coluna '{c}' possui {negative_count} valores negativos!")
            else:
                self.logger.info(f"✅ Coluna '{c}' contém apenas valores positivos.")

    def count_rows(self, df: DataFrame, stage_name: str):
        """Métrica de volumetria (Observabilidade)"""
        count = df.count()
        self.logger.info(f"📊 [OBSERVABILITY] Total de linhas em {stage_name}: {count}")
        return count