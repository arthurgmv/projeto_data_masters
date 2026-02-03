@echo off
chcp 65001 > nul
echo ========================================================
echo 🐳 1. INICIANDO INFRAESTRUTURA (DOCKER)
echo ========================================================
docker-compose up -d

echo.
echo ⏳ Aguardando serviços iniciarem (10s)...
timeout /t 10 >nul

echo.
echo ========================================================
echo 📦 2. INSTALANDO DEPENDENCIAS (NO CLUSTER)
echo ========================================================
docker exec spark_master pip install boto3 python-dotenv pytest faker colorama pyspark

echo.
echo ========================================================
echo 🧪 3. EXECUTANDO TESTES DE QUALIDADE
echo ========================================================
docker exec -t spark_master pytest -v /app/tests/

echo.
echo ========================================================
echo 🚀 4. INICIANDO PIPELINE DE DADOS (ORQUESTRADOR)
echo ========================================================
docker exec -t spark_master python3 src/pipeline.py

echo.
echo ========================================================
echo ✅ PROCESSO FINALIZADO!
echo ========================================================
pause