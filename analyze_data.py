"""
Unified Token Analysis - Live API Version
"""

import sys
import os
from pathlib import Path

# Setup paths
# target_directory_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'scripts'))
# sys.path.insert(0, target_directory_path)

# Import Config (this will auto-load .env)
try:
    from config import config
except ImportError:
    # If config isn't in python path, add current dir
    sys.path.insert(0, str(Path(__file__).parent))
    from config import config

# Validate config before proceeding
if not config.validate():
    print("Exiting due to configuration errors.")
    sys.exit(1)

import pandas as pd
from datetime import datetime

# Import modules
from data_fetcher import LiveDataFetcher
from wash_trading_detector import WashTradingDetector
from bot_detector import BotDetector
from combined_analyzer import CombinedAnalyzer
from holder_analyzer import HolderAnalyzer

def analyze_token(token_address: str):
    """Analyze a single token"""

    # 1. Instantiate Fetcher (Uses config.ARKHAM_API_KEY by default)
    fetcher = LiveDataFetcher()
    token_name = fetcher._get_token_display_name(token_address)

    print("\n" + "="*70)
    print(f"ANALYZING: {token_name}")
    print("="*70)

    # Step 1: Fetch data
    print("\n[1/5] FETCHING LIVE DATA")
    user_flows = fetcher.fetch_and_process_token(token_address)

    if user_flows is None or len(user_flows) == 0:
        print(f"✗ No data for {token_name}")
        return None

    # Step 2: Wash trading
    print("\n[2/5] WASH TRADING DETECTION")
    wash_detector = WashTradingDetector(user_flows)
    wash_detector.run_all_analyses()

    # Step 3: Bot detection
    print("\n[3/5] BOT DETECTION")

    # UNIFIED: We no longer pass manual args. BotDetector grabs them from config.
    bot_detector = BotDetector(user_flows=user_flows)

    bot_detector.classify_wallets(
        min_transactions=config.MIN_TRANSACTIONS,
        sample_size=config.SAMPLE_SIZE
    )

    # Step 4: Holder & Risk Analysis
    print("\n[4/5] HOLDER RISK ANALYSIS")
    holder_analyzer = HolderAnalyzer(fetcher, token_address, config.CHAIN)
    holder_concentration_metrics, top_holders_list = holder_analyzer.run_analysis()

    # Step 5: Combined
    print("\n[5/5] COMBINED ANALYSIS")
    combined = CombinedAnalyzer(wash_detector, bot_detector, token_name)
    combined.create_combined_analysis(holder_concentration_metrics, top_holders_list)

    # Save
    os.makedirs(f"{config.OUTPUT_DIR}/combined_analysis", exist_ok=True)
    combined.save_results(f"{config.OUTPUT_DIR}/combined_analysis", token_name)
    print("\n" + combined.generate_report())


    return combined


def main():
    print("\n" + "="*70)
    print("LIVE API TOKEN ANALYSIS")
    print("="*70 + "\n")

    config.print_config()

    for i, token in enumerate(config.TOKENS, 1):
        print(f"{i}. {token}")

    response = input("\nProceed? (y/n): ")
    if response.lower() != 'y':
        return

    results = {}
    for token in config.TOKENS:
        try:
            results[token] = analyze_token(token)
        except Exception as e:
            print(f"\n✗ Error: {e}")
            import traceback
            traceback.print_exc()

    print("\n✅ Done!")


if __name__ == "__main__":
    main()
