"""
Main data collection script
Fetches transfer data from Arkham API and saves to JSON
"""

import os
import sys
from datetime import datetime
from dotenv import load_dotenv
from arkham_client import ArkhamAPIClient

load_dotenv()


def main():
    """Execute data collection workflow"""
    
    # Load configuration
    token = os.getenv('TARGET_TOKEN')
    token_symbol = os.getenv('TOKEN_SYMBOL', 'TOKEN')
    chain = os.getenv('CHAIN', 'solana')
    time_range = os.getenv('TIME_RANGE', '30d')
    min_usd = float(os.getenv('MIN_USD_VALUE', '0'))
    max_transfers = int(os.getenv('MAX_TRANSFERS', '5000'))
    
    if not token:
        print("✗ ERROR: TARGET_TOKEN not set in .env file")
        sys.exit(1)
    
    print("\n" + "="*70)
    print("SOLANA WASH TRADING ANALYSIS")
    print("Data Collection Module")
    print("="*70)
    print(f"\nConfiguration:")
    print(f"  Token: {token}")
    print(f"  Symbol: {token_symbol}")
    print(f"  Chain: {chain}")
    print(f"  Time Range: {time_range}")
    print(f"  Min USD Value: ${min_usd:,.2f}")
    print(f"  Max Transfers: {max_transfers:,}")
    print("="*70)
    
    # Initialize API client
    try:
        client = ArkhamAPIClient()
    except ValueError as e:
        print(f"\n✗ ERROR: {e}")
        print("Please set ARKHAM_API_KEY in your .env file")
        sys.exit(1)
    
    # Collect transfers
    print("\nStarting data collection...")
    
    transfers = client.get_all_transfers(
        chains=chain,
        tokens=token,
        timeLast=time_range,
        usdGte=min_usd if min_usd > 0 else None,
        max_results=max_transfers
    )
    
    if not transfers:
        print("\n✗ No transfers collected. Possible reasons:")
        print("  - Token has no activity in the specified time range")
        print("  - API key is invalid")
        print("  - Network connectivity issues")
        print("  - Token address is incorrect")
        sys.exit(1)
    
    # Generate filename with timestamp
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f"{token_symbol}_{chain}_{time_range}_{timestamp}.json"
    
    # Save data
    client.save_raw_data(transfers, filename)
    
    # Print summary
    print("\n" + "="*70)
    print("✓ DATA COLLECTION SUCCESSFUL")
    print("="*70)
    print(f"\nNext steps:")
    print(f"  1. Process the data:")
    print(f"     python scripts/process_data.py {filename}")
    print(f"\n  2. Run wash trading analysis:")
    print(f"     python analysis/detect_wash_trading.py")
    print("\n" + "="*70 + "\n")


if __name__ == "__main__":
    main()
