import json
import time
import os
import logging
import random
from datetime import datetime
from faker import Faker
import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv

# --- CONFIGURAÇÃO: PRODUÇÃO ---
SALVAR_LOCAL = False  # Gravar no Docker!
# ------------------------------

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

# Carrega senhas do .env
load_dotenv()

class IngestaoBronze:
    def __init__(self):
        self.fake = Faker('pt_BR')
        self.bucket = os.getenv('BUCKET_BRONZE', 'bronze')
        self.s3_client = self._conectar_minio()

    def _conectar_minio(self):
        """Conecta ao Data Lake (MinIO)"""
        try:
            return boto3.client(
                's3',
                endpoint_url=os.getenv('MINIO_ENDPOINT'),
                aws_access_key_id=os.getenv('MINIO_ROOT_USER'),
                aws_secret_access_key=os.getenv('MINIO_ROOT_PASSWORD')
            )
        except Exception as e:
            logger.critical(f"Erro ao conectar no MinIO: {e}")
            raise

    def criar_bucket_automatico(self):
        """Cria o balde 'bronze' se ele não existir"""
        try:
            self.s3_client.head_bucket(Bucket=self.bucket)
        except ClientError:
            logger.warning(f"Bucket '{self.bucket}' não encontrado. Criando agora...")
            try:
                self.s3_client.create_bucket(Bucket=self.bucket)
                logger.info(f"✅ Bucket '{self.bucket}' criado com sucesso!")
            except Exception as e:
                logger.error(f"Erro ao criar bucket: {e}")

    def gerar_dados(self, qtd):
        dados = []
        for _ in range(qtd):
            dados.append({
                "id_transacao": self.fake.uuid4(),
                "data_evento": datetime.now().isoformat(),
                "valor": round(random.uniform(10.0, 5000.0), 2),
                "cliente_nome": self.fake.name(),         # SENSÍVEL
                "cliente_cpf": self.fake.cpf(),           # SENSÍVEL
                "cartao": self.fake.credit_card_number(), # SENSÍVEL
                "cidade": self.fake.city(),
                "estado": self.fake.state_abbr()
            })
        return dados

    def executar(self, num_lotes=1):
        logger.info("🚀 Iniciando ingestão no Data Lake...")
        self.criar_bucket_automatico()
        
        for i in range(num_lotes):
            dados = self.gerar_dados(50) # 50 transações por arquivo
            
            # Organiza pastas: raw/ano/mes/dia
            agora = datetime.now()
            caminho = f"raw/ano={agora.year}/mes={agora.month:02d}/dia={agora.day:02d}"
            nome_arquivo = f"{caminho}/lote_{int(time.time())}_{i}.json"

            try:
                corpo_json = json.dumps(dados, ensure_ascii=False)
                self.s3_client.put_object(
                    Bucket=self.bucket,
                    Key=nome_arquivo,
                    Body=corpo_json
                )
                logger.info(f"💾 Arquivo salvo: {nome_arquivo}")
            except Exception as e:
                logger.error(f"❌ Erro ao salvar: {e}")
            
            time.sleep(1)

if __name__ == "__main__":
    # Gera 3 arquivos de teste
    pipeline = IngestaoBronze()
    pipeline.executar(num_lotes=3)