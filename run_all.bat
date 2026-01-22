@echo off
echo ==========================================
echo 🐳 1. INICIANDO INFRAESTRUTURA (DOCKER)
echo ==========================================
docker-compose up -d
echo.
echo ⏳ Aguardando 5 segundos para o MinIO subir...
timeout /t 5 >nul

echo.
echo ==========================================
echo 🥉 2. EXECUTANDO CAMADA BRONZE (INGESTAO)
echo ==========================================
py src/ingestao.py

echo.
echo ==========================================
echo 🥈 3. EXECUTANDO CAMADA SILVER (PROCESSAMENTO)
echo ==========================================
py src/processamento.py

echo.
echo ==========================================
echo 🥇 4. EXECUTANDO CAMADA GOLD (INTELIGENCIA)
echo ==========================================
py src/gold.py

echo.
echo ==========================================
echo 🔍 5. AUDITORIA FINAL (LEITURA)
echo ==========================================
py src/leitor.py

echo.
echo ==========================================
echo ✅ PIPELINE FINALIZADO COM SUCESSO!
echo ==========================================
pause