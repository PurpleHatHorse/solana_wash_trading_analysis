"""
Unified Token Analysis - Live API Version
"""

import sys
import os
from pathlib import Path

# Setup paths
ROOT_DIR = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT_DIR / 'scripts'))
sys.path.insert(0, str(ROOT_DIR / 'analysis'))

# Load .env FIRST
from dotenv import load_dotenv
load_dotenv(dotenv_path=ROOT_DIR / '.env')

# Get config values
API_KEY = os.getenv('ARKHAM_API_KEY', '')
BASE_URL = os.getenv('ARKHAM_BASE_URL', 'https://api.arkm.com')
TOKENS = os.getenv('TOKENS', '').split(',')
TOKENS = [t.strip() for t in TOKENS if t.strip()]
CHAIN = os.getenv('CHAIN', 'solana')
TIME_WINDOW = os.getenv('TIME_WINDOW', '7d')
ENDPOINTS = os.getenv('API_ENDPOINTS', 'transfers,intelligence,counterparties').split(',')
ENDPOINTS = [e.strip() for e in ENDPOINTS]

import pandas as pd
from datetime import datetime

# Import modules - they now get config from environment
from data_fetcher import LiveDataFetcher
from wash_trading_detector import WashTradingDetector  
from bot_detector import BotDetector
from combined_analyzer import CombinedAnalyzer


def analyze_token(token_address: str):
    """Analyze a single token"""
    
    fetcher = LiveDataFetcher(API_KEY)
    token_name = fetcher._get_token_display_name(token_address)
    
    print("\n" + "="*70)
    print(f"ANALYZING: {token_name}")
    print("="*70)
    
    # Step 1: Fetch data
    print("\n[1/4] FETCHING LIVE DATA")
    user_flows = fetcher.fetch_and_process_token(token_address)
    
    if user_flows is None or len(user_flows) == 0:
        print(f"✗ No data for {token_name}")
        return None
    
    # Step 2: Wash trading
    print("\n[2/4] WASH TRADING DETECTION")
    wash_detector = WashTradingDetector(user_flows)
    wash_detector.run_all_analyses()
    
    # Step 3: Bot detection
    print("\n[3/4] BOT DETECTION")
    bot_detector = BotDetector(
        api_key=API_KEY,
        user_flows=user_flows,
        base_url=BASE_URL,
        cache_dir="data/cache",
        endpoints=ENDPOINTS,
        time_window=TIME_WINDOW,
        max_workers=3
    )
    bot_detector.classify_wallets(min_transactions=5)
    
    # Step 4: Combined
    print("\n[4/4] COMBINED ANALYSIS")
    combined = CombinedAnalyzer(wash_detector, bot_detector, token_name)
    combined.create_combined_analysis()
    
    # Save
    os.makedirs("outputs/combined_analysis", exist_ok=True)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    combined.save_results("outputs/combined_analysis", token_name)
    
    print("\n" + combined.generate_report())
    
    return combined


def main():
    print("\n" + "="*70)
    print("LIVE API TOKEN ANALYSIS")
    print("="*70 + "\n")
    
    print(f"API Key: {'✓' if API_KEY else '✗'}")
    print(f"Tokens: {len(TOKENS)}")
    print(f"Endpoints: {', '.join(ENDPOINTS)}\n")
    
    for i, token in enumerate(TOKENS, 1):
        print(f"{i}. {token}")
    
    response = input("\nProceed? (y/n): ")
    if response.lower() != 'y':
        return
    
    results = {}
    for token in TOKENS:
        try:
            results[token] = analyze_token(token)
        except Exception as e:
            print(f"\n✗ Error: {e}")
            import traceback
            traceback.print_exc()
    
    print("\n✅ Done!")


if __name__ == "__main__":
    main()