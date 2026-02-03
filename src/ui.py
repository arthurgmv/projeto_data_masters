from colorama import init, Fore, Style

init(autoreset=True, strip=False)

def print_tab(df, title):
    print(f"\n{Fore.MAGENTA}{Style.BRIGHT}┌{'─'*78}┐")
    print(f"{Fore.MAGENTA}{Style.BRIGHT}│ 📊 AMOSTRA DE DADOS: {title.center(56)} │") 
    print(f"{Fore.MAGENTA}{Style.BRIGHT}└{'─'*78}┘{Style.RESET_ALL}")
    
    df.show(5, truncate=False)
    
    print(f"{Fore.MAGENTA}{Style.DIM}{'-'*80}{Style.RESET_ALL}\n")