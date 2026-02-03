import subprocess
import time
import logging
import sys
import os
from colorama import init, Fore, Style

# 🔧 ALTERAÇÃO 1: Adicionamos strip=False. 
# Isso força o colorama a emitir códigos ANSI mesmo se detectar que não está num TTY (Terminal).
init(autoreset=True, strip=False)

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger()

def print_header(text):
    print(f"\n{Fore.CYAN}{Style.BRIGHT}{'='*60}")
    print(f"{Fore.CYAN}{Style.BRIGHT} {text}")
    print(f"{Fore.CYAN}{Style.BRIGHT}{'='*60}{Style.RESET_ALL}")

def run_task(script_name, step_name):
    print(f"\n{Fore.YELLOW}▶️  Iniciando etapa: {Style.BRIGHT}{step_name}{Style.RESET_ALL} ({script_name})")
    print(f"{Fore.BLACK}{Style.BRIGHT}{'-'*60}")
    
    start_time = time.time()
    script_path = os.path.join("src", script_name)
    
    my_env = os.environ.copy()
    my_env["FORCE_COLOR"] = "1"       # Padrão moderno
    my_env["CLICOLOR_FORCE"] = "1"    # Padrão Unix/Linux
    my_env["PYTHONUNBUFFERED"] = "1"  # Garante que os logs saiam em tempo real
    
    try:
        subprocess.run([sys.executable, script_path], check=True, env=my_env)
        
        duration = time.time() - start_time
        
        print(f"{Fore.BLACK}{Style.BRIGHT}{'-'*60}")
        logger.info(f"{Fore.GREEN}{Style.BRIGHT}✅ SUCESSO: {step_name} finalizado em {duration:.2f}s{Style.RESET_ALL}")
        return True
        
    except subprocess.CalledProcessError:
        print(f"{Fore.BLACK}{Style.BRIGHT}{'-'*60}")
        logger.error(f"{Fore.RED}{Style.BRIGHT}❌ FALHA CRÍTICA: Erro na etapa {step_name}. Pipeline abortado.{Style.RESET_ALL}")
        return False
    except FileNotFoundError:
        logger.error(f"{Fore.RED}❌ Erro: O arquivo '{script_path}' não foi encontrado.{Style.RESET_ALL}")
        return False

def main():
    print_header("🚀 INICIANDO ORQUESTRADOR DATA MASTERS")
    
    # Passo 1: Ingestão (Bronze)
    if not run_task("ingestao.py", "1. BRONZE INGESTION"):
        sys.exit(1)

    # Passo 2: Processamento (Silver)
    if not run_task("processamento.py", "2. SILVER TRANSFORMATION (LGPD)"):
        sys.exit(1)

    # Passo 3: Agregação (Gold)
    if os.path.exists("src/gold.py"):
        if not run_task("gold.py", "3. GOLD AGGREGATION (KPIs)"):
            sys.exit(1)

    # Passo 4: Auditoria
    if os.path.exists("src/leitor.py"):
        run_task("leitor.py", "4. FINAL AUDIT & QUALITY CHECK")

    print(f"\n{Fore.GREEN}{Style.BRIGHT}{'='*60}")
    print(f"{Fore.GREEN}{Style.BRIGHT}🏆  PIPELINE EXECUTADO COM SUCESSO COMPLETO  🏆")
    print(f"{Fore.GREEN}{Style.BRIGHT}{'='*60}{Style.RESET_ALL}\n")

if __name__ == "__main__":
    main()