# ==============================================================================
# DATA COLLECTOR BOT - v1.2 FINAL ROBUSTO
#
# Coleta dados de memecoins recém-lançadas na rede Solana e salva em um
# banco de dados PostgreSQL. Otimizado para resiliência e deploy no Koyeb.
# ==============================================================================

import os
import requests
import time
import psycopg2
import traceback # Importado para logs de erro mais detalhados
from datetime import datetime, timedelta

# --- 1. CONFIGURAÇÕES E VARIÁVEIS DE AMBIENTE ---

# Variáveis de ambiente que serão configuradas no painel do Koyeb
DATABASE_URL = os.environ.get('DATABASE_URL')
GOPLUS_API_KEY = os.environ.get('GOPLUS_API_KEY')
RPC_URL = os.environ.get('RPC_URL') # Usando a variável padrão para a URL da Helius

# A blockchain alvo para a descoberta de novos pares
TARGET_CHAIN = 'solana'
# Chain ID correspondente na GoPlus Security API
GOPLUS_CHAIN_ID = 'solana_mainnet' 

# Regras de negócio
MAX_PAIR_AGE_HOURS = 4  # Idade máxima em horas para um par ser considerado "novo"
DEATH_LIQUIDITY_THRESHOLD_USD = 2000 # Liquidez mínima para ser considerado "vivo"
DEATH_VOLUME_THRESHOLD_USD = 1000 # Volume mínimo em 1h para ser considerado "vivo"

# --- 2. BANCO DE DADOS (PostgreSQL) ---

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

# --- 3. FONTES DE DADOS (APIs) ---

def get_security_data(token_address):
    """Busca dados de segurança na GoPlus Security API de forma mais robusta."""
    if not GOPLUS_API_KEY: 
        return None
        
    url = f"https://api.gopluslabs.io/api/v1/token_security/{GOPLUS_CHAIN_ID}?contract_addresses={token_address}"
    headers = {'X-API-KEY': GOPLUS_API_KEY}
    
    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        
        # --- LINHA CORRIGIDA ---
        # Primeiro, pegamos o dicionário 'result' de forma segura.
        result_dict = response.json().get('result')
        
        # Depois, verificamos se ele não é nulo antes de usá-lo.
        if result_dict:
            return result_dict.get(token_address.lower())
        
        # Se 'result' for nulo ou não existir, retornamos None.
        return None
        
    except requests.RequestException as e:
        print(f"  - Erro na API GoPlus: {e}")
        return None
        
def get_holder_count_from_helius(token_address):
    """Busca o número de holders usando a Digital Asset API da Helius."""
    if not RPC_URL:
        print("  - URL RPC da Helius não configurada na variável RPC_URL.")
        return 0
    
    try:
        headers = {'Content-Type': 'application/json'}
        payload = {
            "jsonrpc": "2.0", "id": "helius-data-collector",
            "method": "getAsset", "params": {"id": token_address}
        }
        response = requests.post(RPC_URL, headers=headers, json=payload, timeout=15)
        response.raise_for_status()
        data = response.json()
        holder_count = data.get('result', {}).get('ownership', {}).get('owner_count', 0)
        return holder_count
    except requests.RequestException as e:
        print(f"  - Erro na API Helius ao buscar holders: {e}")
        return 0
    except (KeyError, TypeError) as e:
        print(f"  - Erro ao processar resposta da Helius: {e}")
        return 0

# --- 4. LÓGICA DO BOT ---

def discover_and_profile_new_pairs():
    """Busca, perfila e salva novos pares no banco de dados de forma mais robusta."""
    print(f"\n🔎 Procurando novos pares na rede {TARGET_CHAIN}...")
    try:
        response = requests.get(f"https://api.dexscreener.com/latest/dex/search?q=new", timeout=15)
        response.raise_for_status()
        pairs = response.json().get('pairs', [])
        
        conn = get_db_connection()
        cursor = conn.cursor()

        for pair in pairs:
            if pair.get('chainId') != TARGET_CHAIN: continue
            
            pair_created_at = datetime.fromtimestamp(pair.get('pairCreatedAt', 0) / 1000)
            if datetime.utcnow() - pair_created_at > timedelta(hours=MAX_PAIR_AGE_HOURS): continue
            
            token_address = pair.get('baseToken', {}).get('address')
            if not token_address: continue

            cursor.execute("SELECT id FROM tokens WHERE token_address = %s", (token_address,))
            if cursor.fetchone() is None:
                print(f"✨ Descoberto: {pair['baseToken']['symbol']} ({pair['pairAddress'][:6]}...)")
                
                # Bloco de coleta e processamento de dados robusto
                security_data = get_security_data(token_address)
                time.sleep(1) 
                
                if security_data:
                    is_honeypot = bool(int(security_data.get('is_honeypot', 0)))
                    buy_tax = float(security_data.get('buy_tax', 0))
                    sell_tax = float(security_data.get('sell_tax', 0))
                else:
                    print(f"  - Aviso: Dados de segurança para {pair['baseToken']['symbol']} não encontrados. Salvando como nulo.")
                    is_honeypot = None
                    buy_tax = None
                    sell_tax = None

                holder_count = get_holder_count_from_helius(token_address)
                time.sleep(1) 
                
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
        traceback.print_exc()
    finally:
        if 'conn' in locals() and not conn.closed:
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
            if liquidity_usd > 1 and liquidity_usd < DEATH_LIQUIDITY_THRESHOLD_USD: death_reason = f"liquidity_collapse"
            elif volume_h1 < DEATH_VOLUME_THRESHOLD_USD and liquidity_usd > 1: death_reason = f"low_volume"

            if death_reason:
                cursor.execute(
                    "UPDATE tokens SET status = 'dead', death_at = %s, death_reason = %s WHERE id = %s",
                    (datetime.utcnow(), death_reason, token_id)
                )
                print(f"  💀 {symbol} foi marcado como 'morto'. Motivo: {death_reason}")
            
            conn.commit()
            time.sleep(1)
        except Exception as e:
            print(f"Erro ao processar {symbol}: {e}")
            conn.rollback()
    
    cursor.close()
    conn.close()

# --- 5. LOOP PRINCIPAL ---

if __name__ == "__main__":
    if not all([DATABASE_URL, GOPLUS_API_KEY, RPC_URL]):
        print("❌ ERRO: Verifique se as variáveis de ambiente DATABASE_URL, GOPLUS_API_KEY e RPC_URL estão configuradas.")
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
                traceback.print_exc()
                print("Reiniciando em 60 segundos...")
                time.sleep(60)
