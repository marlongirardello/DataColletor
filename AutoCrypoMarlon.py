# ==============================================================================
# DATA COLLECTOR BOT - v2.0 FINAL E OTIMIZADO
#
# Utiliza o método getProgramAccounts para uma contagem de holders robusta
# e garantida de funcionar com endpoints RPC gratuitos.
# ==============================================================================

import os
import requests
import time
import psycopg2
import traceback
from datetime import datetime, timedelta
from flask import Flask
from threading import Thread

# --- 1. CONFIGURAÇÕES E VARIÁVEIS DE AMBIENTE ---
DATABASE_URL = os.environ.get('DATABASE_URL')
GOPLUS_API_KEY = os.environ.get('GOPLUS_API_KEY')
RPC_URL = os.environ.get('RPC_URL') # Sua URL da QuickNode

TARGET_CHAIN = 'solana'
GOPLUS_CHAIN_ID = 'solana_mainnet' 

MAX_PAIR_AGE_HOURS = 4
DEATH_LIQUIDITY_THRESHOLD_USD = 2000
DEATH_VOLUME_THRESHOLD_USD = 1000

# --- 2. SERVIDOR WEB PARA HEALTH CHECK ---
app = Flask(__name__)
@app.route('/')
def health_check():
    return "Data collector is alive and running.", 200

def run_web_server():
    port = int(os.environ.get("PORT", 8000))
    app.run(host='0.0.0.0', port=port)

# --- FUNÇÃO DE TESTE DE API (Opcional, mas bom manter) ---
def test_api_keys():
    # Esta função pode ser mantida como está, pois já validou a GoPlus
    pass 

# --- 3. BANCO DE DADOS (PostgreSQL) ---
# (As funções de banco de dados permanecem as mesmas)
def get_db_connection():
    if not DATABASE_URL: raise ValueError("DATABASE_URL não configurada.")
    return psycopg2.connect(DATABASE_URL)

def setup_database():
    print("🔧 Configurando o banco de dados PostgreSQL...")
    conn = get_db_connection()
    cursor = conn.cursor()
    # (Comandos CREATE TABLE permanecem os mesmos)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS tokens (id SERIAL PRIMARY KEY, token_address TEXT UNIQUE NOT NULL, pair_address TEXT, chain TEXT, symbol TEXT, discovered_at TIMESTAMPTZ, initial_holder_count INTEGER, is_honeypot BOOLEAN, buy_tax REAL, sell_tax REAL, status TEXT DEFAULT 'monitoring', death_at TIMESTAMPTZ, death_reason TEXT);
    ''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS market_data (id SERIAL PRIMARY KEY, token_id INTEGER REFERENCES tokens(id), timestamp TIMESTAMPTZ NOT NULL, price_usd NUMERIC, liquidity_usd NUMERIC, volume_h1 NUMERIC, buys_h1 INTEGER, sells_h1 INTEGER);
    ''')
    conn.commit()
    cursor.close()
    conn.close()
    print("✅ Banco de dados pronto.")

# --- 4. FONTES DE DADOS (APIs) ---

def get_security_data(token_address):
    # (Esta função permanece a mesma)
    if not GOPLUS_API_KEY: return None
    url = f"https://api.gopluslabs.io/api/v1/token_security/{GOPLUS_CHAIN_ID}?contract_addresses={token_address}"
    headers = {'X-API-KEY': GOPLUS_API_KEY}
    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        result_dict = response.json().get('result')
        if result_dict: return result_dict.get(token_address)
        return None
    except requests.RequestException as e:
        print(f"  - Erro na API GoPlus: {e}")
        return None

# --- NOVA FUNÇÃO DE CONTAGEM DE HOLDERS ---
def get_holder_count(token_address):
    """Busca o número de holders usando o método universal e otimizado getProgramAccounts."""
    if not RPC_URL:
        print("  - URL RPC não configurada.")
        return 0
    
    try:
        headers = {'Content-Type': 'application/json'}
        payload = {
            "jsonrpc": "2.0", "id": 1, "method": "getProgramAccounts",
            "params": [
                "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",
                {"encoding": "base64", "filters": [{"dataSize": 165}, {"memcmp": {"offset": 0, "bytes": token_address}}], "withContext": False}
            ]
        }
        response = requests.post(RPC_URL, headers=headers, json=payload, timeout=60)
        response.raise_for_status()
        data = response.json()
        
        if 'result' in data and isinstance(data['result'], list):
            return len(data['result'])
        else:
            print(f"  - Resposta inesperada da RPC para holders do token {token_address[:10]}...")
            return 0
    except Exception as e:
        print(f"  - Erro na chamada RPC para getProgramAccounts: {e}")
        return 0

# --- 5. LÓGICA DO BOT ---

def discover_and_profile_new_pairs():
    """Busca novos pares e os perfila usando as funções otimizadas."""
    print(f"\n🔎 Procurando novos pares na rede {TARGET_CHAIN} via Geckoterminal...")
    try:
        response = requests.get(f"https://api.geckoterminal.com/api/v2/networks/{TARGET_CHAIN}/new_pools", timeout=15)
        response.raise_for_status()
        pools_data = response.json().get('data', [])
        
        if not pools_data:
            print("  - Nenhum pool novo retornado pela Geckoterminal.")
            return

        conn = get_db_connection()
        cursor = conn.cursor()

        for pool in pools_data:
            attributes = pool.get('attributes', {})
            relationships = pool.get('relationships', {})
            pair_address = attributes.get('address')
            base_token_data = relationships.get('base_token', {}).get('data', {})
            token_id_string = base_token_data.get('id')
            
            if not all([pair_address, token_id_string]): continue

            token_address = token_id_string.split('_')[-1]
            symbol = attributes.get('name', 'N/A').split(' / ')[0]
            
            cursor.execute("SELECT id FROM tokens WHERE token_address = %s", (token_address,))
            if cursor.fetchone() is None:
                print(f"✨ Descoberto via Geckoterminal: {symbol} ({pair_address[:6]}...)")
                
                security_data = get_security_data(token_address)
                time.sleep(1)
                
                if security_data:
                    is_honeypot = bool(int(security_data.get('is_honeypot', 0)))
                    buy_tax = float(security_data.get('buy_tax', 0))
                    sell_tax = float(security_data.get('sell_tax', 0))
                else:
                    print(f"  - Aviso: Dados de segurança para {symbol} não encontrados.")
                    is_honeypot = None; buy_tax = None; sell_tax = None

                # --- CHAMANDO A NOVA FUNÇÃO CORRETA ---
                holder_count = get_holder_count(token_address)
                print(f"  - Contagem de Holders: {holder_count}")
                time.sleep(1)
                
                cursor.execute(
                    "INSERT INTO tokens (token_address, pair_address, chain, symbol, discovered_at, initial_holder_count, is_honeypot, buy_tax, sell_tax) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
                    (token_address, pair_address, TARGET_CHAIN, symbol, datetime.utcnow(), holder_count, is_honeypot, buy_tax, sell_tax)
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
    # (Esta função permanece a mesma)
    conn = get_db_connection(); cursor = conn.cursor()
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
            price_usd = float(data.get('priceUsd', 0)); liquidity_usd = float(data.get('liquidity', {}).get('usd', 0)); volume_h1 = float(data.get('volume', {}).get('h1', 0)); buys_h1 = int(data.get('txns', {}).get('h1', {}).get('buys', 0)); sells_h1 = int(data.get('txns', {}).get('h1', {}).get('sells', 0))
            cursor.execute("INSERT INTO market_data (token_id, timestamp, price_usd, liquidity_usd, volume_h1, buys_h1, sells_h1) VALUES (%s, %s, %s, %s, %s, %s, %s)", (token_id, datetime.utcnow(), price_usd, liquidity_usd, volume_h1, buys_h1, sells_h1))
            print(f"  -> {symbol}: Preço ${price_usd:.8f}, Liq ${liquidity_usd:,.0f}")
            death_reason = None
            if liquidity_usd > 1 and liquidity_usd < DEATH_LIQUIDITY_THRESHOLD_USD: death_reason = f"liquidity_collapse"
            elif volume_h1 < DEATH_VOLUME_THRESHOLD_USD and liquidity_usd > 1: death_reason = f"low_volume"
            if death_reason:
                cursor.execute("UPDATE tokens SET status = 'dead', death_at = %s, death_reason = %s WHERE id = %s", (datetime.utcnow(), death_reason, token_id))
                print(f"  💀 {symbol} foi marcado como 'morto'. Motivo: {death_reason}")
            conn.commit(); time.sleep(1)
        except Exception as e:
            print(f"Erro ao processar {symbol}: {e}"); conn.rollback()
    cursor.close(); conn.close()

# --- 6. INICIALIZAÇÃO ---

if __name__ == "__main__":
    if not all([DATABASE_URL, RPC_URL]): # Simplificado, já que a GoPlus não está funcionando para nós
        print("❌ ERRO FATAL: Verifique se as variáveis de ambiente DATABASE_URL e RPC_URL estão configuradas.")
    else:
        health_check_thread = Thread(target=run_web_server, daemon=True)
        health_check_thread.start()
        main_bot_logic()
