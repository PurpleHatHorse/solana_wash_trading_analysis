"""
Wash Trading Detection
Detects suspicious trading patterns
"""

import sys
from pathlib import Path

# Add root directory to path
ROOT_DIR = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT_DIR))

import pandas as pd
import numpy as np
from typing import Dict, Set
from tqdm import tqdm
import networkx as nx
from datetime import timedelta


class WashTradingDetector:
    """Detect wash trading patterns in user flows"""
    
    def __init__(self, user_flows: pd.DataFrame):
        self.user_flows = user_flows
        self.suspicious_wallets: Set[str] = set()
        self.results = {}
    
    def detect_self_transfers(self) -> pd.DataFrame:
        """Detect wallets sending to themselves"""
        
        print("\n[1/6] Detecting Self-Transfers...")
        
        self_transfers = self.user_flows[
            self.user_flows['is_self_transfer'] == True
        ].copy()
        
        if len(self_transfers) == 0:
            print("  ✓ No self-transfers detected")
            return pd.DataFrame()
        
        summary = self_transfers.groupby('start_wallet').agg({
            'transaction_hash': 'count',
            'usd_value': ['sum', 'mean']
        }).reset_index()
        
        summary.columns = ['wallet', 'transfer_count', 'total_volume_usd', 'avg_volume_usd']
        summary = summary.sort_values('transfer_count', ascending=False)
        
        self.suspicious_wallets.update(summary['wallet'])
        
        print(f"  🚨 Found {len(self_transfers)} self-transfer transactions")
        print(f"  🚨 Involving {len(summary)} unique wallets")
        
        return summary
    
    def detect_rapid_roundtrips(self, time_threshold_hours: int = 24) -> pd.DataFrame:
        """Detect rapid round-trip trades"""
        
        print(f"\n[2/6] Detecting Rapid Round-Trips (<{time_threshold_hours}h)...")
        
        roundtrips = []
        flows = self.user_flows.copy()
        
        flows['wallet_pair'] = flows.apply(
            lambda row: tuple(sorted([row['start_wallet'], row['end_wallet']])),
            axis=1
        )
        
        for pair, group in tqdm(flows.groupby('wallet_pair'), desc="  Checking pairs", leave=False):
            if len(group) < 2:
                continue
            
            wallet_a, wallet_b = pair
            a_to_b = group[group['start_wallet'] == wallet_a].sort_values('timestamp')
            b_to_a = group[group['start_wallet'] == wallet_b].sort_values('timestamp')
            
            if len(a_to_b) == 0 or len(b_to_a) == 0:
                continue
            
            roundtrip_count = 0
            for _, out_transfer in a_to_b.iterrows():
                time_window_end = out_transfer['timestamp'] + timedelta(hours=time_threshold_hours)
                matching_returns = b_to_a[
                    (b_to_a['timestamp'] > out_transfer['timestamp']) &
                    (b_to_a['timestamp'] <= time_window_end)
                ]
                roundtrip_count += len(matching_returns)
            
            if roundtrip_count >= 2:
                roundtrips.append({
                    'wallet_a': wallet_a,
                    'wallet_b': wallet_b,
                    'roundtrip_count': roundtrip_count,
                    'total_transactions': len(group)
                })
        
        result_df = pd.DataFrame(roundtrips)
        
        if not result_df.empty:
            self.suspicious_wallets.update(result_df['wallet_a'])
            self.suspicious_wallets.update(result_df['wallet_b'])
            print(f"  🚨 Found {len(result_df)} wallet pairs with round-trips")
        else:
            print("  ✓ No rapid round-trips detected")
        
        return result_df
    
    def detect_high_frequency_pairs(self, min_transactions: int = 10) -> pd.DataFrame:
        """Detect high-frequency trading pairs"""
        
        print(f"\n[3/6] Detecting High-Frequency Pairs (>={min_transactions} txs)...")
        
        flows = self.user_flows.copy()
        flows['wallet_pair'] = flows.apply(
            lambda row: tuple(sorted([row['start_wallet'], row['end_wallet']])),
            axis=1
        )
        
        pair_counts = flows.groupby('wallet_pair').size()
        high_freq_pairs = pair_counts[pair_counts >= min_transactions]
        
        if len(high_freq_pairs) == 0:
            print("  ✓ No high-frequency pairs detected")
            return pd.DataFrame()
        
        results = []
        for pair, count in high_freq_pairs.items():
            results.append({
                'wallet_a': pair[0],
                'wallet_b': pair[1],
                'transaction_count': count
            })
        
        result_df = pd.DataFrame(results).sort_values('transaction_count', ascending=False)
        
        self.suspicious_wallets.update(result_df['wallet_a'])
        self.suspicious_wallets.update(result_df['wallet_b'])
        
        print(f"  🚨 Found {len(result_df)} high-frequency pairs")
        
        return result_df
    
    def detect_circular_patterns(self, max_cycle_length: int = 4) -> pd.DataFrame:
        """Detect circular trading patterns"""
        
        print(f"\n[4/6] Detecting Circular Patterns (max {max_cycle_length} hops)...")
        
        G = nx.DiGraph()
        
        for _, row in self.user_flows.iterrows():
            if G.has_edge(row['start_wallet'], row['end_wallet']):
                G[row['start_wallet']][row['end_wallet']]['weight'] += row['usd_value'] or 0
            else:
                G.add_edge(row['start_wallet'], row['end_wallet'], weight=row['usd_value'] or 0)
        
        cycles_found = []
        
        try:
            for cycle in list(nx.simple_cycles(G))[:1000]:  # Limit to 1000
                if 2 <= len(cycle) <= max_cycle_length:
                    cycles_found.append({
                        'cycle_length': len(cycle),
                        'wallets': cycle
                    })
                    self.suspicious_wallets.update(cycle)
        except:
            pass
        
        result_df = pd.DataFrame(cycles_found)
        
        if not result_df.empty:
            print(f"  🚨 Found {len(result_df)} circular patterns")
        else:
            print("  ✓ No circular patterns detected")
        
        return result_df
    
    def analyze_volume_concentration(self, top_n: int = 20) -> pd.DataFrame:
        """Analyze volume concentration"""
        
        print(f"\n[5/6] Analyzing Volume Concentration...")
        
        start_volume = self.user_flows.groupby('start_wallet')['usd_value'].sum()
        end_volume = self.user_flows.groupby('end_wallet')['usd_value'].sum()
        
        total_volume = start_volume.add(end_volume, fill_value=0).sort_values(ascending=False)
        top_wallets = total_volume.head(top_n)
        
        result_df = pd.DataFrame({
            'wallet': top_wallets.index,
            'total_volume_usd': top_wallets.values
        }).reset_index(drop=True)
        
        print(f"  📊 Top 10 wallets: {(top_wallets.head(10).sum() / total_volume.sum() * 100):.1f}% of volume")
        
        return result_df
    
    def detect_temporal_clustering(self, time_window_minutes: int = 5) -> pd.DataFrame:
        """Detect temporal clustering"""
        
        print(f"\n[6/6] Detecting Temporal Clustering...")
        
        flows = self.user_flows.copy()
        flows['time_window'] = flows['timestamp'].dt.floor(f'{time_window_minutes}min')
        
        clusters = flows.groupby('time_window').size()
        significant_clusters = clusters[clusters >= 3]
        
        result_df = pd.DataFrame({
            'timestamp': significant_clusters.index,
            'transaction_count': significant_clusters.values
        })
        
        if not result_df.empty:
            print(f"  🚨 Found {len(result_df)} clustered time windows")
        else:
            print("  ✓ No temporal clustering detected")
        
        return result_df
    
    def run_all_analyses(self) -> Dict:
        """Run all wash trading detection methods"""
        
        print(f"\n{'='*70}")
        print("WASH TRADING DETECTION")
        print(f"{'='*70}")
        
        self.results['self_transfers'] = self.detect_self_transfers()
        self.results['rapid_roundtrips'] = self.detect_rapid_roundtrips()
        self.results['high_frequency_pairs'] = self.detect_high_frequency_pairs()
        self.results['circular_patterns'] = self.detect_circular_patterns()
        self.results['volume_concentration'] = self.analyze_volume_concentration()
        self.results['temporal_clustering'] = self.detect_temporal_clustering()
        
        print(f"\n🚨 TOTAL SUSPICIOUS WALLETS: {len(self.suspicious_wallets)}")
        
        return self.results
