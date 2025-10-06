# ==============================================================================
# DATA COLLECTOR BOT - v1.0
#
# Coleta dados de memecoins recém-lançadas e salva em um banco de dados
# PostgreSQL para análise futura. Focado em moedas de vida curta.
# ==============================================================================

import os
import requests
import time
import psycopg2
from datetime import datetime, timedelta

# --- 1. CONFIGURAÇÕES E VARIÁVEIS DE AMBIENTE ---

# A URL do banco de dados é injetada pelo Koyeb
DATABASE_URL = os.environ.get('DATABASE_URL')
# Chaves de API (injete-as como variáveis de ambiente no seu deploy)
GOPLUS_API_KEY = os.environ.get('GOPLUS_API_KEY')
# Use a chave do Solscan (via Helius/QuickNode) ou Etherscan/Basescan
BLOCKCHAIN_EXPLORER_API_KEY = os.environ.get('BLOCKCHAIN_EXPLORER_API_KEY') 
# A blockchain alvo para a descoberta de novos pares
TARGET_CHAIN = 'solana' # ou 'base', 'ethereum', etc.

# Regras de negócio
MAX_PAIR_AGE_HOURS = 4  # Idade máxima em horas para um par ser considerado "novo"
DEATH_LIQUIDITY_THRESHOLD_USD = 2000 # Liquidez mínima para ser considerado "vivo"
DEATH_VOLUME_THRESHOLD_USD = 1000 # Volume mínimo em 1h para ser considerado "vivo"

def get_db_connection():
    """Retorna uma conexão com o banco de dados PostgreSQL."""
    if not DATABASE_URL:
        raise ValueError("DATABASE_URL não foi configurada como variável de ambiente.")
    return psycopg2.connect(DATABASE_URL)

def setup_database():
    """Cria as tabelas no PostgreSQL se não existirem."""
    print("🔧 Configurando o banco de dados PostgreSQL...")
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS tokens (
            id SERIAL PRIMARY KEY,
            token_address TEXT UNIQUE NOT NULL,
            pair_address TEXT,
            chain TEXT,
            symbol TEXT,
            discovered_at TIMESTAMPTZ,
            initial_holder_count INTEGER,
            is_honeypot BOOLEAN,
            buy_tax REAL,
            sell_tax REAL,
            status TEXT DEFAULT 'monitoring',
            death_at TIMESTAMPTZ,
            death_reason TEXT
        );
    ''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS market_data (
            id SERIAL PRIMARY KEY,
            token_id INTEGER REFERENCES tokens(id),
            timestamp TIMESTAMPTZ NOT NULL,
            price_usd NUMERIC,
            liquidity_usd NUMERIC,
            volume_h1 NUMERIC,
            buys_h1 INTEGER,
            sells_h1 INTEGER
        );
    ''')
    conn.commit()
    cursor.close()
    conn.close()
    print("✅ Banco de dados pronto.")

# --- 2. FONTES DE DADOS (APIs) ---

def get_security_data(token_address, chain_id='solana_mainnet'):
    """Busca dados de segurança na GoPlus Security API."""
    if not GOPLUS_API_KEY: return None
    url = f"https://api.gopluslabs.io/api/v1/token_security/{chain_id}?contract_addresses={token_address}"
    headers = {'X-API-KEY': GOPLUS_API_KEY}
    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        return response.json().get('result', {}).get(token_address.lower())
    except requests.RequestException as e:
        print(f"  - Erro na API GoPlus: {e}")
        return None

def get_holder_count(token_address):
    """Busca o número de holders. (Exemplo para Solana via API pública, idealmente use uma com chave)"""
    # NOTA: Adapte esta URL para a API que você escolheu (Helius, QuickNode, Etherscan, etc.)
    # Este é um exemplo genérico que pode não funcionar para todas as redes.
    try:
        # Exemplo para Solana com API pública do Solscan (pode ser instável)
        if TARGET_CHAIN == 'solana':
            url = f"https://public-api.solscan.io/token/holders?tokenAddress={token_address}&offset=0&limit=0"
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            return response.json().get('total')
        # Adicione aqui a lógica para outras chains (ex: Etherscan)
        return 0
    except requests.RequestException as e:
        print(f"  - Erro ao buscar holders: {e}")
        return 0

# --- 3. LÓGICA DO BOT ---

def discover_and_profile_new_pairs():
    """Busca, perfila e salva novos pares no banco de dados."""
    print(f"\n🔎 Procurando novos pares na rede {TARGET_CHAIN}...")
    try:
        response = requests.get(f"https://api.dexscreener.com/latest/dex/search?q=new", timeout=15)
        response.raise_for_status()
        pairs = response.json().get('pairs', [])
        
        conn = get_db_connection()
        cursor = conn.cursor()

        for pair in pairs:
            if pair.get('chainId') != TARGET_CHAIN:
                continue
            
            pair_created_at = datetime.fromtimestamp(pair.get('pairCreatedAt', 0) / 1000)
            if datetime.utcnow() - pair_created_at > timedelta(hours=MAX_PAIR_AGE_HOURS):
                continue
            
            token_address = pair.get('baseToken', {}).get('address')
            if not token_address: continue

            cursor.execute("SELECT id FROM tokens WHERE token_address = %s", (token_address,))
            if cursor.fetchone() is None:
                print(f"✨ Descoberto: {pair['baseToken']['symbol']} ({pair['pairAddress'][:6]}...)")
                
                # Coleta de dados estáticos na descoberta
                security_data = get_security_data(token_address)
                time.sleep(1) # Respeita o rate limit da GoPlus
                holder_count = get_holder_count(token_address)
                time.sleep(1) # Respeita o rate limit do Explorer
                
                is_honeypot = bool(int(security_data.get('is_honeypot', 0))) if security_data else None
                buy_tax = float(security_data.get('buy_tax', 0)) if security_data else None
                sell_tax = float(security_data.get('sell_tax', 0)) if security_data else None

                cursor.execute(
                    """
                    INSERT INTO tokens 
                    (token_address, pair_address, chain, symbol, discovered_at, initial_holder_count, is_honeypot, buy_tax, sell_tax) 
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (token_address, pair['pairAddress'], pair['chainId'], pair['baseToken']['symbol'], datetime.utcnow(), holder_count, is_honeypot, buy_tax, sell_tax)
                )
                conn.commit()
    except Exception as e:
        print(f"Erro na fase de descoberta: {e}")
    finally:
        if 'conn' in locals():
            cursor.close()
            conn.close()

def collect_and_analyze_data():
    """Coleta dados para tokens ativos e verifica sua 'morte'."""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT id, pair_address, symbol FROM tokens WHERE status = 'monitoring'")
    tokens_to_monitor = cursor.fetchall()

    if not tokens_to_monitor:
        print("📊 Nenhum token ativo para monitorar.")
        return
        
    print(f"\n📊 Coletando dados para {len(tokens_to_monitor)} token(s) ativo(s)...")
    for token_id, pair_address, symbol in tokens_to_monitor:
        try:
            url = f"https://api.dexscreener.com/latest/dex/pairs/{TARGET_CHAIN}/{pair_address}"
            response = requests.get(url, timeout=10)
            data = response.json().get('pair')

            if not data: continue

            price_usd = float(data.get('priceUsd', 0))
            liquidity_usd = float(data.get('liquidity', {}).get('usd', 0))
            volume_h1 = float(data.get('volume', {}).get('h1', 0))
            buys_h1 = int(data.get('txns', {}).get('h1', {}).get('buys', 0))
            sells_h1 = int(data.get('txns', {}).get('h1', {}).get('sells', 0))
            
            cursor.execute(
                "INSERT INTO market_data (token_id, timestamp, price_usd, liquidity_usd, volume_h1, buys_h1, sells_h1) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                (token_id, datetime.utcnow(), price_usd, liquidity_usd, volume_h1, buys_h1, sells_h1)
            )
            print(f"  -> {symbol}: Preço ${price_usd:.8f}, Liq ${liquidity_usd:,.0f}")
            
            death_reason = None
            if liquidity_usd > 1 and liquidity_usd < DEATH_LIQUIDITY_THRESHOLD_USD:
                death_reason = f"liquidity_collapse"
            elif volume_h1 < DEATH_VOLUME_THRESHOLD_USD and liquidity_usd > 1:
                death_reason = f"low_volume"

            if death_reason:
                cursor.execute(
                    "UPDATE tokens SET status = 'dead', death_at = %s, death_reason = %s WHERE id = %s",
                    (datetime.utcnow(), death_reason, token_id)
                )
                print(f"  💀 {symbol} foi marcado como 'morto'. Motivo: {death_reason}")
            
            conn.commit()
            time.sleep(1) # Pausa entre as chamadas da DexScreener
        except Exception as e:
            print(f"Erro ao processar {symbol}: {e}")
            conn.rollback()
    
    cursor.close()
    conn.close()

# --- LOOP PRINCIPAL ---

if __name__ == "__main__":
    if not all([DATABASE_URL, GOPLUS_API_KEY, BLOCKCHAIN_EXPLORER_API_KEY]):
        print("❌ ERRO: Verifique se as variáveis de ambiente DATABASE_URL, GOPLUS_API_KEY, e BLOCKCHAIN_EXPLORER_API_KEY estão configuradas.")
    else:
        setup_database()
        while True:
            try:
                discover_and_profile_new_pairs()
                collect_and_analyze_data()
                print(f"\n--- Ciclo completo. Próxima verificação em 15 minutos --- ({datetime.now().strftime('%H:%M:%S')})")
                time.sleep(900)
            except KeyboardInterrupt:
                print("\n🛑 Bot interrompido.")
                break
            except Exception as e:
                print(f"❌ Erro fatal no loop principal: {e}")
                time.sleep(60)
