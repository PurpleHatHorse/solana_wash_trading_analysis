"""
Live Data Fetcher
Fetches transfer data from Arkham API for specified tokens
"""

import pandas as pd
import requests
import time
import os
from typing import Optional, Dict, Any
from tqdm import tqdm
from datetime import datetime

# Import config (caller should have already set up paths and loaded .env)
try:
    from config import config
except ImportError:
    import sys
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).parent))
    from config import config


class LiveDataFetcher:
    """Fetch live transfer data from Arkham API"""
    
    def __init__(self, api_key: str = None):
        # Unify: Use passed key or fall back to config
        self.api_key = api_key or config.ARKHAM_API_KEY
        self.base_url = config.ARKHAM_BASE_URL
        self.headers = {"API-Key": self.api_key}
    
    def _enforce_rate_limit(self):
        """Enforce 1 req/sec for /transfers endpoint"""
        if config.ENFORCE_RATE_LIMITS:
            time.sleep(1.0)
    
    def _get_token_display_name(self, contract_address: str) -> str:
        """Get friendly name for token, fallback to shortened address"""
        url = f"{self.base_url}/intelligence/address/{contract_address}"
        params = {"chain": config.CHAIN}

        try:
            response = requests.get(url, headers=self.headers, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
            print (data)
            name = data['arkhamLabel']['name']
            start = name.index("(") + 1
            end = name.index(")")
            # Extract the substring
            token_name = name[start:end]
            print(f"✓ Successfully fetched token name")
            return token_name.upper()
        except requests.exceptions.RequestException as e:
            print(f"✗ Error fetching token name: {e}")
            return f"{contract_address[:4]}...{contract_address[-4:]}"

    def fetch_token_transfers(self, 
                             token_address: str,
                             chain: str = "solana",
                             time_last: str = "7d",
                             limit: int = 1000) -> Optional[pd.DataFrame]:
        """
        Fetch transfers for a specific token using contract address
        """
        display_name = self._get_token_display_name(token_address)
        
        print(f"\n{'='*70}")
        print(f"FETCHING TRANSFERS: {display_name}")
        print(f"Contract: {token_address}")
        print(f"Chain: {chain} | Window: {time_last}")
        print(f"{'='*70}\n")
        
        all_transfers = []
        offset = 0
        batch_size = 100
        consecutive_errors = 0
        max_consecutive_errors = 3
        
        with tqdm(desc=f"Fetching {display_name}", unit="batch") as pbar:
            while len(all_transfers) < limit:
                self._enforce_rate_limit()
                
                params = {
                    'tokens': token_address,
                    'chains': chain,
                    'timeLast': time_last,
                    'limit': batch_size,
                    'offset': offset,
                    'sortKey': 'time',
                    'sortDir': 'desc'
                }
                
                try:
                    response = requests.get(
                        f"{self.base_url}/transfers",
                        headers=self.headers,
                        params=params,
                        timeout=30
                    )
                    
                    if response.status_code == 404:
                        print(f"\n  ✗ Token not found: {token_address}")
                        return None
                    
                    if response.status_code == 401:
                        print(f"\n  ✗ Authentication failed")
                        return None
                    
                    if response.status_code == 429:
                        wait_time = 5
                        print(f"\n  ⚠ Rate limited, waiting {wait_time}s...")
                        time.sleep(wait_time)
                        continue
                    
                    if response.status_code == 400:
                        print(f"\n  ✗ Bad request (400)")
                        return None
                    
                    response.raise_for_status()
                    
                    try:
                        data = response.json()
                    except Exception as e:
                        print(f"\n  ✗ Failed to parse JSON: {e}")
                        consecutive_errors += 1
                        if consecutive_errors >= max_consecutive_errors:
                            return None
                        continue
                    
                    if data is None or not isinstance(data, dict):
                        consecutive_errors += 1
                        if consecutive_errors >= max_consecutive_errors:
                            return None
                        continue
                    
                    if 'transfers' not in data:
                        print(f"\n  ✗ No 'transfers' key in response")
                        if 'error' in data:
                            print(f"  Error: {data['error']}")
                        consecutive_errors += 1
                        if consecutive_errors >= max_consecutive_errors:
                            return None
                        continue
                    
                    transfers = data.get('transfers', [])
                    
                    if not transfers or len(transfers) == 0:
                        if offset == 0:
                            print(f"\n  ⚠ No transfers found for {display_name}")
                            return None
                        else:
                            break
                    
                    consecutive_errors = 0
                    all_transfers.extend(transfers)
                    
                    pbar.update(1)
                    pbar.set_postfix({
                        'transfers': len(all_transfers),
                        'batch': len(transfers)
                    })
                    
                    offset += batch_size
                    
                    if len(transfers) < batch_size:
                        break
                
                except requests.exceptions.Timeout:
                    print(f"\n  ⚠ Request timeout, retrying...")
                    consecutive_errors += 1
                    if consecutive_errors >= max_consecutive_errors:
                        return None
                    time.sleep(2)
                    continue
                
                except requests.exceptions.RequestException as e:
                    print(f"\n  ✗ Request error: {e}")
                    consecutive_errors += 1
                    if consecutive_errors >= max_consecutive_errors:
                        return None
                    time.sleep(2)
                    continue
        
        if not all_transfers:
            return None
        
        df = pd.DataFrame(all_transfers)
        
        print(f"\n✓ Successfully fetched {len(df):,} transfers for {display_name}")
        
        if 'blockTimestamp' in df.columns:
            df_temp = df.copy()
            df_temp['blockTimestamp'] = pd.to_datetime(df_temp['blockTimestamp'])
            print(f"✓ Date range: {df_temp['blockTimestamp'].min()} to {df_temp['blockTimestamp'].max()}")
        
        if 'fromAddress' in df.columns and 'toAddress' in df.columns:
            unique_from = df['fromAddress'].apply(lambda x: x.get('address') if isinstance(x, dict) else x).nunique()
            unique_to = df['toAddress'].apply(lambda x: x.get('address') if isinstance(x, dict) else x).nunique()
            print(f"✓ Unique addresses: {unique_from + unique_to:,} (from: {unique_from:,}, to: {unique_to:,})")
        
        return df
    
    def process_transfers_to_user_flows(self, df: pd.DataFrame, token_address: str) -> pd.DataFrame:
        """Process raw transfers into user flows (remove intermediaries)"""
        
        display_name = self._get_token_display_name(token_address)
        
        print(f"\n{'='*70}")
        print(f"PROCESSING TRANSFERS TO USER FLOWS: {display_name}")
        print(f"{'='*70}\n")
        
        df['timestamp'] = pd.to_datetime(df['blockTimestamp'])
        
        intermediaries = ['dex', 'cex', 'dex_aggregator', 'bridge', 'mixer']
        
        def get_entity_type(address_obj):
            if isinstance(address_obj, dict) and 'arkhamEntity' in address_obj:
                entity = address_obj['arkhamEntity']
                if isinstance(entity, dict):
                    return entity.get('type')
            return None
        
        df['from_entity_type'] = df['fromAddress'].apply(get_entity_type)
        df['to_entity_type'] = df['toAddress'].apply(get_entity_type)
        
        df['from_is_intermediary'] = df['from_entity_type'].isin(intermediaries)
        df['to_is_intermediary'] = df['to_entity_type'].isin(intermediaries)
        
        def get_address(address_obj):
            if isinstance(address_obj, dict):
                return address_obj.get('address')
            return address_obj
        
        df['from_address'] = df['fromAddress'].apply(get_address)
        df['to_address'] = df['toAddress'].apply(get_address)
        
        print(f"Intermediary transfers: {(df['from_is_intermediary'] | df['to_is_intermediary']).sum():,}")
        print(f"User-to-user transfers: {(~df['from_is_intermediary'] & ~df['to_is_intermediary']).sum():,}")
        
        user_flows = []
        
        for tx_hash, tx_transfers in tqdm(df.groupby('transactionHash'), desc="Extracting flows"):
            tx_transfers = tx_transfers.sort_values('blockNumber')
            
            start_wallet = None
            start_entity_type = None
            for _, row in tx_transfers.iterrows():
                if not row['from_is_intermediary']:
                    start_wallet = row['from_address']
                    start_entity_type = row['from_entity_type']
                    break
            
            end_wallet = None
            end_entity_type = None
            for _, row in tx_transfers.iloc[::-1].iterrows():
                if not row['to_is_intermediary']:
                    end_wallet = row['to_address']
                    end_entity_type = row['to_entity_type']
                    break
            
            if start_wallet and end_wallet:
                user_flows.append({
                    'transaction_hash': tx_hash,
                    'start_wallet': start_wallet,
                    'end_wallet': end_wallet,
                    'start_entity_type': start_entity_type,
                    'end_entity_type': end_entity_type,
                    'timestamp': tx_transfers['timestamp'].iloc[0],
                    'block_number': tx_transfers['blockNumber'].iloc[0],
                    'usd_value': tx_transfers.get('historicalUSD', pd.Series([0])).sum(),
                    'token_symbol': display_name,
                    'token_address': token_address,
                    'chain': tx_transfers['chain'].iloc[0] if 'chain' in tx_transfers else 'solana',
                    'hop_count': len(tx_transfers),
                    'is_self_transfer': start_wallet == end_wallet
                })
        
        user_flows_df = pd.DataFrame(user_flows)
        
        if len(user_flows_df) == 0:
            print(f"\n⚠ Warning: No user-to-user flows found!")
            return user_flows_df
        
        print(f"\n✓ Extracted {len(user_flows_df):,} user-to-user flows")
        print(f"✓ From {df['transactionHash'].nunique():,} transactions")
        print(f"✓ Unique wallets: {user_flows_df['start_wallet'].nunique() + user_flows_df['end_wallet'].nunique():,}")
        
        self_transfers = user_flows_df['is_self_transfer'].sum()
        if self_transfers > 0:
            print(f"⚠ Self-transfers detected: {self_transfers}")
        
        return user_flows_df

    def fetch_token_holders(self, token_address: str, chain: str = "solana") -> Optional[Dict[str, Any]]:
        """
        Fetches token holder data from the Arkham API.
        """
        display_name = self._get_token_display_name(token_address)
        
        print(f"\n[Fetching Holders] {display_name} ({chain})...")
        
        self._enforce_rate_limit()
        
        url = f"{self.base_url}/token/holders/{chain}/{token_address}"
        params = {"groupByEntity": "true"}

        try:
            response = requests.get(url, headers=self.headers, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
            print(f"✓ Successfully fetched top holders")
            return data
        except requests.exceptions.RequestException as e:
            print(f"✗ Error fetching holders: {e}")
            return None

    def fetch_wallet_balance(self, wallet_address: str, chain: str = "solana") -> Dict[str, Any]:
        """
        Fetches the portfolio/balance of a specific wallet address.
        Used for AMM analysis.
        """
        self._enforce_rate_limit()
        
        url = f"{self.base_url}/balances/address/{wallet_address}"
        params = {"chains": chain}

        try:
            response = requests.get(url, headers=self.headers, params=params, timeout=15)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            # Silent fail for individual wallet lookups to not spam console
            return {}

    def fetch_and_process_token(self, token_address: str) -> Optional[pd.DataFrame]:
        """Fetch and process data for a single token"""
        
        display_name = self._get_token_display_name(token_address)
        
        transfers_df = self.fetch_token_transfers(
            token_address=token_address,
            chain=config.CHAIN,
            time_last=config.TIME_WINDOW,
            limit=10000
        )
        
        if transfers_df is None or len(transfers_df) == 0:
            print(f"\n✗ No data available for {display_name}")
            return None
        
        user_flows = self.process_transfers_to_user_flows(transfers_df, token_address)
        
        if user_flows is None or len(user_flows) == 0:
            print(f"\n✗ No user flows extracted for {display_name}")
            return None
        
        os.makedirs(f"{config.OUTPUT_DIR}/transfer_data", exist_ok=True)
        os.makedirs(f"{config.OUTPUT_DIR}/user_flows", exist_ok=True)
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        safe_name = display_name.replace('/', '_').replace('\\', '_')
        
        transfers_file = f"{config.OUTPUT_DIR}/transfer_data/{safe_name}_{config.CHAIN}_{timestamp}.csv"
        transfers_df.to_csv(transfers_file, index=False)
        print(f"✓ Saved raw transfers: {transfers_file}")
        
        flows_file = f"{config.OUTPUT_DIR}/user_flows/{safe_name}_{config.CHAIN}_{timestamp}.csv"
        user_flows.to_csv(flows_file, index=False)
        print(f"✓ Saved user flows: {flows_file}")
        
        return user_flows
