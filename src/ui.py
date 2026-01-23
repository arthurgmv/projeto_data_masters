from colorama import init, Fore, Style

# Inicializa o colorama
init(autoreset=True)

def print_tab(df, title):

    print(f"\n{Fore.MAGENTA}{Style.BRIGHT}┌{'─'*78}┐")
    print(f"{Fore.MAGENTA}{Style.BRIGHT}│ 📊 AMOSTRA DE DADOS: {title.ljust(59)} │")
    print(f"{Fore.MAGENTA}{Style.BRIGHT}└{'─'*78}┘{Style.RESET_ALL}")
    
    df.show(5, truncate=False)
    
    print(f"{Fore.MAGENTA}{Style.DIM}{'-'*80}{Style.RESET_ALL}\n")