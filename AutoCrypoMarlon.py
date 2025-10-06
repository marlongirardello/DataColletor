# ==============================================================================
# DATA COLLECTOR BOT - v1.4 - COM PRÉ-TESTE DE API
#
# Adiciona uma verificação de validade da chave da GoPlus na inicialização
# para um diagnóstico rápido e preciso.
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
RPC_URL = os.environ.get('RPC_URL')

TARGET_CHAIN = 'solana'
GOPLUS_CHAIN_ID = 'solana_mainnet' 

MAX_PAIR_AGE_HOURS = 4
DEATH_LIQUIDITY_THRESHOLD_USD = 2000
DEATH_VOLUME_THRESHOLD_USD = 1000

# --- 2. SERVIDOR WEB PARA HEALTH CHECK ---

app = Flask(__name__)

@app.route('/')
def health_check():
    """Endpoint simples para responder à verificação de saúde do Koyeb."""
    return "Data collector is alive and running.", 200

def run_web_server():
    """Inicia o servidor Flask na porta fornecida pelo ambiente."""
    port = int(os.environ.get("PORT", 8000))
    app.run(host='0.0.0.0', port=port)

# --- NOVA FUNÇÃO DE TESTE DE API ---

def test_api_keys():
    """Verifica se a chave da API GoPlus é válida antes de iniciar o bot."""
    print("\n🔬 Iniciando pré-teste das chaves de API...")
    
    # Teste para a GoPlus Security
    if not GOPLUS_API_KEY:
        print("❌ ERRO: A variável de ambiente GOPLUS_API_KEY não está configurada.")
        return False
        
    # Usamos um token conhecido e estável para o teste (Dogwifhat - WIF)
    test_token_address = "EKpQGSJtjMFqKZ9KQanSqYXRcF8fBopzL7EMMroA2FDb"
    url = f"https://api.gopluslabs.io/api/v1/token_security/{GOPLUS_CHAIN_ID}?contract_addresses={test_token_address}"
    headers = {'X-API-KEY': GOPLUS_API_KEY}
    
    try:
        response = requests.get(url, headers=headers, timeout=15)
        
        if response.status_code == 200:
            print("✅ Chave da GoPlus Security validada com sucesso.")
            # Poderíamos adicionar um teste para a Helius aqui também se necessário
            return True
        elif response.status_code in [401, 403]:
            print(f"❌ ERRO: A chave da GoPlus Security é INVÁLIDA ou não autorizada (Código: {response.status_code}).")
            print("   - Verifique se a chave foi copiada corretamente e está ativa no painel da GoPlus.")
            return False
        else:
            print(f"⚠️ Aviso: Recebido status inesperado da GoPlus ({response.status_code}). O bot tentará continuar.")
            print(f"   - Resposta: {response.text[:100]}") # Mostra os primeiros 100 caracteres da resposta
            return True # Permite que o bot continue mesmo com avisos
            
    except requests.RequestException as e:
        print(f"❌ ERRO: Falha de conexão ao testar a chave da GoPlus: {e}")
        return False

# --- 3. BANCO DE DADOS (PostgreSQL) ---

def get_db_connection():
    if not DATABASE_URL:
        raise ValueError("DATABASE_URL não foi configurada.")
    return psycopg2.connect(DATABASE_URL)

def setup_database():
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

# --- 4. FONTES DE DADOS (APIs) ---
# (As funções get_security_data e get_holder_count_from_helius permanecem as mesmas da versão anterior)

def get_security_data(token_address):
    if not GOPLUS_API_KEY: return None
    url = f"https://api.gopluslabs.io/api/v1/token_security/{GOPLUS_CHAIN_ID}?contract_addresses={token_address}"
    headers = {'X-API-KEY': GOPLUS_API_KEY}
    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        result_dict = response.json().get('result')
        if result_dict:
            return result_dict.get(token_address)
        return None
    except requests.RequestException as e:
        print(f"  - Erro na API GoPlus: {e}")
        return None

def get_holder_count_from_helius(token_address):
    if not RPC_URL:
        print("  - URL RPC da Helius não configurada.")
        return 0
    try:
        headers = {'Content-Type': 'application/json'}
        payload = { "jsonrpc": "2.0", "id": "helius-data-collector", "method": "getAsset", "params": {"id": token_address} }
        response = requests.post(RPC_URL, headers=headers, json=payload, timeout=15)
        response.raise_for_status()
        data = response.json()
        return data.get('result', {}).get('ownership', {}).get('owner_count', 0)
    except Exception as e:
        print(f"  - Erro na API Helius ao buscar holders: {e}")
        return 0

# --- 5. LÓGICA DO BOT ---
# (As funções main_bot_logic, discover_and_profile_new_pairs, collect_and_analyze_data permanecem as mesmas)

def main_bot_logic():
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

def discover_and_profile_new_pairs():
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

        new_discoveries = 0
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
                new_discoveries += 1
                print(f"✨ Descoberto via Geckoterminal: {symbol} ({pair_address[:6]}...)")
                
                security_data = get_security_data(token_address)
                time.sleep(1)
                
                if security_data:
                    is_honeypot = bool(int(security_data.get('is_honeypot', 0)))
                    buy_tax = float(security_data.get('buy_tax', 0))
                    sell_tax = float(security_data.get('sell_tax', 0))
                else:
                    print(f"  - Aviso: Dados de segurança para {symbol} não encontrados.")
                    is_honeypot = None
                    buy_tax = None
                    sell_tax = None

                holder_count = get_holder_count_from_helius(token_address)
                time.sleep(1)
                
                cursor.execute(
                    """
                    INSERT INTO tokens (token_address, pair_address, chain, symbol, discovered_at, initial_holder_count, is_honeypot, buy_tax, sell_tax) 
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (token_address, pair_address, TARGET_CHAIN, symbol, datetime.utcnow(), holder_count, is_honeypot, buy_tax, sell_tax)
                )
                conn.commit()
        
        if new_discoveries == 0:
            print("  - Nenhum par novo (ainda não registrado) encontrado neste ciclo.")

    except Exception as e:
        print(f"Erro na fase de descoberta via Geckoterminal: {e}")
        traceback.print_exc()
    finally:
        if 'conn' in locals() and not conn.closed:
            cursor.close()
            conn.close()

def collect_and_analyze_data():
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


# --- BLOCO DE INICIALIZAÇÃO MODIFICADO ---

if __name__ == "__main__":
    # Verifica se as variáveis de ambiente essenciais existem
    if not all([DATABASE_URL, GOPLUS_API_KEY, RPC_URL]):
        print("❌ ERRO FATAL: Verifique se as variáveis de ambiente DATABASE_URL, GOPLUS_API_KEY e RPC_URL estão configuradas.")
    else:
        # Inicia o servidor web em uma thread separada para o health check
        health_check_thread = Thread(target=run_web_server, daemon=True)
        health_check_thread.start()
        
        # Roda o pré-teste das chaves de API
        if test_api_keys():
            # Se as chaves são válidas, inicia a lógica principal do bot
            main_bot_logic()
        else:
            # Se as chaves são inválidas, o bot não continua.
            print("🔴 Bot encerrado devido a chaves de API inválidas. Verifique a configuração e reinicie o serviço.")
