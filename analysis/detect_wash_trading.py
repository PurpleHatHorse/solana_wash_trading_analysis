"""
Transaction-Based Wash Trading Detection
Extracts start and end user wallets, excludes all intermediaries
"""

import pandas as pd
import numpy as np
import sys
import os
from datetime import timedelta
from collections import defaultdict
import networkx as nx
from typing import Dict, List, Tuple
from tqdm import tqdm


class TransactionBasedWashTradingDetector:
    """
    Wash trading detector using transaction-hash based analysis
    Extracts only start and end user wallets, excludes all intermediaries
    """
    
    # Entity types to exclude (intermediaries)
    INTERMEDIARIES = ['dex', 'cex', 'dex_aggregator', 'bridge', 'mixer']
    
    def __init__(self, csv_filename: str):
        """Initialize detector with processed transfer data"""
        
        filepath = f"data/processed/{csv_filename}"
        
        if not os.path.exists(filepath):
            raise FileNotFoundError(f"File not found: {filepath}")
        
        print(f"\nLoading processed data: {filepath}")
        
        # Load necessary columns
        self.df = pd.read_csv(filepath)
        self.df['timestamp'] = pd.to_datetime(self.df['timestamp'])
        
        # Remove rows with null addresses
        self.df = self.df.dropna(subset=['from_address', 'to_address', 'transaction_hash'])
        
        print(f"✓ Loaded {len(self.df):,} transfers")
        print(f"✓ Unique transactions: {self.df['transaction_hash'].nunique():,}")
        
        self.user_flows = None
        self.results = {}
        self.suspicious_addresses = set()
    
    def extract_user_flows(self) -> pd.DataFrame:
        """
        Extract start→end user wallet flows from each transaction
        Removes all intermediary hops (DEX, CEX, bridges, etc.)
        """
        
        print("\n" + "="*70)
        print("EXTRACTING USER WALLET FLOWS")
        print("="*70 + "\n")
        
        print("[Step 1/2] Identifying intermediaries...")
        
        # Mark intermediaries
        self.df['from_is_intermediary'] = self.df['from_entity_type'].isin(self.INTERMEDIARIES)
        self.df['to_is_intermediary'] = self.df['to_entity_type'].isin(self.INTERMEDIARIES)
        
        intermediary_count = (
            self.df['from_is_intermediary'].sum() + 
            self.df['to_is_intermediary'].sum()
        )
        print(f"   ✓ Found {intermediary_count:,} intermediary addresses in transfers")
        
        print("\n[Step 2/2] Extracting start and end wallets per transaction...")
        
        user_flows = []
        
        # Group by transaction hash
        tx_groups = self.df.groupby('transaction_hash')
        
        for tx_hash, tx_transfers in tqdm(tx_groups, desc="   Processing transactions"):
            
            # Sort by log_index to get correct order
            tx_transfers = tx_transfers.sort_values('log_index')
            
            # Find START wallet (first non-intermediary sender)
            start_wallet = None
            start_entity_type = None
            for _, row in tx_transfers.iterrows():
                if not row['from_is_intermediary']:
                    start_wallet = row['from_address']
                    start_entity_type = row['from_entity_type']
                    break
            
            # Find END wallet (last non-intermediary receiver)
            end_wallet = None
            end_entity_type = None
            for _, row in tx_transfers.iloc[::-1].iterrows():  # reverse order
                if not row['to_is_intermediary']:
                    end_wallet = row['to_address']
                    end_entity_type = row['to_entity_type']
                    break
            
            # Only add if we found both start AND end user wallets
            if start_wallet and end_wallet:
                user_flows.append({
                    'transaction_hash': tx_hash,
                    'start_wallet': start_wallet,
                    'end_wallet': end_wallet,
                    'start_entity_type': start_entity_type,
                    'end_entity_type': end_entity_type,
                    'timestamp': tx_transfers['timestamp'].iloc[0],
                    'block_number': tx_transfers['block_number'].iloc[0],
                    'usd_value': tx_transfers['usd_value'].sum(),  # total value in tx
                    'token_symbol': tx_transfers['token_symbol'].iloc[0],
                    'hop_count': len(tx_transfers),  # number of transfers in tx
                    'is_self_transfer': start_wallet == end_wallet
                })
        
        self.user_flows = pd.DataFrame(user_flows)
        
        print(f"\n   ✓ Extracted {len(self.user_flows):,} user-to-user flows")
        print(f"   ✓ From {self.df['transaction_hash'].nunique():,} transactions")
        print(f"   ✓ Filtered out {self.df['transaction_hash'].nunique() - len(self.user_flows):,} transactions")
        print(f"       (transactions with only intermediary addresses)")
        
        if len(self.user_flows) > 0:
            self_transfers = self.user_flows['is_self_transfer'].sum()
            print(f"\n   🚨 Found {self_transfers} self-transfers (same start and end wallet)")
        
        return self.user_flows
    
    def run_all_analyses(self) -> Dict:
        """Execute all wash trading detection algorithms"""
        
        if self.user_flows is None:
            self.extract_user_flows()
        
        if len(self.user_flows) == 0:
            print("\n⚠ No user-to-user flows found. Cannot perform analysis.")
            return {}
        
        print("\n" + "="*70)
        print("WASH TRADING PATTERN DETECTION")
        print("="*70 + "\n")
        
        # Run all detection methods
        self.results['self_transfers'] = self.detect_self_transfers()
        self.results['rapid_roundtrips'] = self.detect_rapid_roundtrips()
        self.results['high_frequency_pairs'] = self.detect_high_frequency_pairs()
        self.results['circular_patterns'] = self.detect_circular_patterns()
        self.results['volume_concentration'] = self.analyze_volume_concentration()
        self.results['temporal_clustering'] = self.detect_temporal_clustering()
        
        # Compile suspicious addresses
        self._compile_suspicious_addresses()
        
        return self.results
    
    def detect_self_transfers(self) -> pd.DataFrame:
        """Detect transactions where start_wallet == end_wallet"""
        
        print("[1/6] Detecting Self-Transfers (Same Start & End Wallet)...")
        
        self_transfers = self.user_flows[self.user_flows['is_self_transfer'] == True].copy()
        
        if len(self_transfers) == 0:
            print(f"   ✓ No self-transfers detected")
            return pd.DataFrame()
        
        # Aggregate by wallet
        summary = self_transfers.groupby('start_wallet').agg({
            'transaction_hash': 'count',
            'usd_value': ['sum', 'mean'],
            'hop_count': 'mean'
        }).reset_index()
        
        summary.columns = ['wallet', 'transfer_count', 'total_volume_usd', 'avg_volume_usd', 'avg_hop_count']
        summary = summary.sort_values('transfer_count', ascending=False)
        
        print(f"   🚨 Found {len(self_transfers)} self-transfer transactions")
        print(f"   🚨 Involving {len(summary)} unique wallets")
        print(f"   💰 Total volume: ${summary['total_volume_usd'].sum():,.2f}")
        print(f"   📊 Avg hops per self-transfer: {summary['avg_hop_count'].mean():.1f}")
        
        return summary
    
    def detect_rapid_roundtrips(self, time_threshold_hours: int = 24) -> pd.DataFrame:
        """Detect wallet pairs with rapid round-trip trades"""
        
        print(f"\n[2/6] Detecting Rapid Round-Trips (<{time_threshold_hours}h)...")
        
        roundtrips = []
        
        # Create wallet pairs
        flows = self.user_flows.copy()
        flows['wallet_pair'] = flows.apply(
            lambda row: tuple(sorted([row['start_wallet'], row['end_wallet']])),
            axis=1
        )
        
        # Group by wallet pair
        for pair, group in tqdm(flows.groupby('wallet_pair'), desc="   Checking pairs", leave=False):
            
            if len(group) < 2:
                continue
            
            wallet_a, wallet_b = pair
            
            # Get flows in both directions
            a_to_b = group[group['start_wallet'] == wallet_a].sort_values('timestamp')
            b_to_a = group[group['start_wallet'] == wallet_b].sort_values('timestamp')
            
            if len(a_to_b) == 0 or len(b_to_a) == 0:
                continue
            
            # Find round-trips within time window
            roundtrip_count = 0
            total_volume = 0
            min_time_diff = timedelta(days=999)
            
            for _, out_transfer in a_to_b.iterrows():
                time_window_end = out_transfer['timestamp'] + timedelta(hours=time_threshold_hours)
                
                matching_returns = b_to_a[
                    (b_to_a['timestamp'] > out_transfer['timestamp']) &
                    (b_to_a['timestamp'] <= time_window_end)
                ]
                
                if len(matching_returns) > 0:
                    roundtrip_count += len(matching_returns)
                    total_volume += (out_transfer['usd_value'] or 0) + matching_returns['usd_value'].sum()
                    
                    time_diff = (matching_returns['timestamp'] - out_transfer['timestamp']).min()
                    if time_diff < min_time_diff:
                        min_time_diff = time_diff
            
            if roundtrip_count >= 2:  # At least 2 round-trips
                roundtrips.append({
                    'wallet_a': wallet_a,
                    'wallet_b': wallet_b,
                    'roundtrip_count': roundtrip_count,
                    'total_transactions': len(group),
                    'roundtrip_ratio': roundtrip_count / len(group),
                    'total_volume_usd': total_volume,
                    'fastest_roundtrip_hours': min_time_diff.total_seconds() / 3600
                })
        
        result_df = pd.DataFrame(roundtrips)
        
        if not result_df.empty:
            result_df = result_df.sort_values('roundtrip_count', ascending=False)
            print(f"   🚨 Found {len(result_df)} wallet pairs with round-trips")
            print(f"   🚨 Total roundtrips: {result_df['roundtrip_count'].sum()}")
            print(f"   ⚡ Fastest roundtrip: {result_df['fastest_roundtrip_hours'].min():.2f} hours")
        else:
            print(f"   ✓ No rapid round-trips detected")
        
        return result_df
    
    def detect_high_frequency_pairs(self, min_transactions: int = 10) -> pd.DataFrame:
        """Detect wallet pairs with high transaction frequency"""
        
        print(f"\n[3/6] Detecting High-Frequency Trading Pairs (>={min_transactions} txs)...")
        
        flows = self.user_flows.copy()
        flows['wallet_pair'] = flows.apply(
            lambda row: tuple(sorted([row['start_wallet'], row['end_wallet']])),
            axis=1
        )
        
        # Count transactions per pair
        pair_counts = flows.groupby('wallet_pair').agg({
            'transaction_hash': 'count',
            'usd_value': ['sum', 'mean'],
            'timestamp': ['min', 'max'],
            'start_wallet': lambda x: list(x)[0],
            'end_wallet': lambda x: list(x)[0]
        }).reset_index()
        
        pair_counts.columns = ['wallet_pair', 'transaction_count', 'total_volume', 'avg_volume', 
                               'first_tx', 'last_tx', 'wallet_a', 'wallet_b']
        
        # Filter high-frequency pairs
        high_freq = pair_counts[pair_counts['transaction_count'] >= min_transactions].copy()
        
        if len(high_freq) == 0:
            print(f"   ✓ No high-frequency pairs detected")
            return pd.DataFrame()
        
        # Calculate time metrics
        high_freq['time_span_days'] = (high_freq['last_tx'] - high_freq['first_tx']).dt.total_seconds() / 86400
        high_freq['transactions_per_day'] = high_freq['transaction_count'] / high_freq['time_span_days'].replace(0, 1)
        
        # Check bidirectionality
        def calc_bidirectional_ratio(row):
            pair_flows = flows[flows['wallet_pair'] == row['wallet_pair']]
            a_to_b = len(pair_flows[pair_flows['start_wallet'] == row['wallet_a']])
            b_to_a = len(pair_flows[pair_flows['start_wallet'] == row['wallet_b']])
            if max(a_to_b, b_to_a) == 0:
                return 0
            return min(a_to_b, b_to_a) / max(a_to_b, b_to_a)
        
        high_freq['bidirectional_ratio'] = high_freq.apply(calc_bidirectional_ratio, axis=1)
        
        high_freq = high_freq.drop(['wallet_pair', 'first_tx', 'last_tx'], axis=1)
        high_freq = high_freq.sort_values('transaction_count', ascending=False)
        
        print(f"   🚨 Found {len(high_freq)} high-frequency pairs")
        print(f"   📊 Highest frequency: {high_freq['transaction_count'].max()} transactions")
        print(f"   🔄 Avg bidirectional ratio: {high_freq['bidirectional_ratio'].mean():.2%}")
        
        return high_freq
    
    def detect_circular_patterns(self, max_cycle_length: int = 4) -> pd.DataFrame:
        """Detect circular trading patterns (A→B→C→A)"""
        
        print(f"\n[4/6] Detecting Circular Patterns (max {max_cycle_length} hops)...")
        
        # Build directed graph
        G = nx.DiGraph()
        
        for _, row in self.user_flows.iterrows():
            start = row['start_wallet']
            end = row['end_wallet']
            
            if G.has_edge(start, end):
                G[start][end]['weight'] += row['usd_value'] or 0
                G[start][end]['count'] += 1
            else:
                G.add_edge(start, end, weight=row['usd_value'] or 0, count=1)
        
        print(f"   Graph: {G.number_of_nodes()} nodes, {G.number_of_edges()} edges")
        
        # Find cycles
        cycles_found = []
        cycle_limit = 1000
        
        try:
            for i, cycle in enumerate(nx.simple_cycles(G)):
                if i >= cycle_limit:
                    print(f"   ⚠ Cycle limit reached ({cycle_limit}), stopping search")
                    break
                
                if 2 <= len(cycle) <= max_cycle_length:
                    # Calculate cycle metrics
                    total_volume = 0
                    total_txs = 0
                    
                    for j in range(len(cycle)):
                        from_node = cycle[j]
                        to_node = cycle[(j + 1) % len(cycle)]
                        
                        if G.has_edge(from_node, to_node):
                            total_volume += G[from_node][to_node]['weight']
                            total_txs += G[from_node][to_node]['count']
                    
                    cycles_found.append({
                        'cycle_length': len(cycle),
                        'wallets': cycle,
                        'cycle_path': ' → '.join([w[:8] + '...' for w in cycle] + [cycle[0][:8] + '...']),
                        'total_volume_usd': total_volume,
                        'total_transactions': total_txs
                    })
        except Exception as e:
            print(f"   ⚠ Error detecting cycles: {e}")
        
        result_df = pd.DataFrame(cycles_found)
        
        if not result_df.empty:
            result_df = result_df.sort_values('total_volume_usd', ascending=False)
            print(f"   🚨 Found {len(result_df)} circular patterns")
            print(f"   💰 Total volume in cycles: ${result_df['total_volume_usd'].sum():,.2f}")
        else:
            print(f"   ✓ No circular patterns detected")
        
        return result_df
    
    def analyze_volume_concentration(self, top_n: int = 20) -> pd.DataFrame:
        """Analyze volume concentration among top wallets"""
        
        print(f"\n[5/6] Analyzing Volume Concentration (Top {top_n})...")
        
        # Combine start and end wallet volumes
        start_volume = self.user_flows.groupby('start_wallet')['usd_value'].sum()
        end_volume = self.user_flows.groupby('end_wallet')['usd_value'].sum()
        
        total_volume_by_wallet = start_volume.add(end_volume, fill_value=0)
        total_volume_by_wallet = total_volume_by_wallet.sort_values(ascending=False)
        
        total_volume = total_volume_by_wallet.sum()
        top_wallets = total_volume_by_wallet.head(top_n)
        
        concentration_data = []
        
        for rank, (wallet, volume) in enumerate(top_wallets.items(), 1):
            tx_count = len(self.user_flows[
                (self.user_flows['start_wallet'] == wallet) | 
                (self.user_flows['end_wallet'] == wallet)
            ])
            
            concentration_data.append({
                'rank': rank,
                'wallet': wallet,
                'total_volume_usd': volume,
                'volume_percentage': (volume / total_volume) * 100,
                'transaction_count': tx_count
            })
        
        result_df = pd.DataFrame(concentration_data)
        result_df['cumulative_percentage'] = result_df['volume_percentage'].cumsum()
        
        print(f"   📊 Top 5 wallets: {result_df.head(5)['volume_percentage'].sum():.2f}% of volume")
        print(f"   📊 Top 10 wallets: {result_df.head(10)['volume_percentage'].sum():.2f}% of volume")
        print(f"   📊 Top 20 wallets: {result_df['volume_percentage'].sum():.2f}% of volume")
        
        return result_df
    
    def detect_temporal_clustering(self, time_window_minutes: int = 5) -> pd.DataFrame:
        """Detect temporal clustering of transactions"""
        
        print(f"\n[6/6] Detecting Temporal Clustering ({time_window_minutes}min windows)...")
        
        flows = self.user_flows.copy()
        flows['time_window'] = flows['timestamp'].dt.floor(f'{time_window_minutes}min')
        
        clusters = []
        
        for time_window, group in flows.groupby('time_window'):
            if len(group) >= 3:  # At least 3 transactions in same window
                wallets = set(group['start_wallet']) | set(group['end_wallet'])
                
                clusters.append({
                    'timestamp': time_window,
                    'transaction_count': len(group),
                    'unique_wallets': len(wallets),
                    'total_volume_usd': group['usd_value'].sum(),
                    'sample_wallets': list(wallets)[:5]
                })
        
        result_df = pd.DataFrame(clusters)
        
        if not result_df.empty:
            result_df = result_df.sort_values('transaction_count', ascending=False)
            print(f"   🚨 Found {len(result_df)} clustered time windows")
            print(f"   📊 Largest cluster: {result_df['transaction_count'].max()} transactions")
        else:
            print(f"   ✓ No temporal clustering detected")
        
        return result_df
    
    def _compile_suspicious_addresses(self):
        """Compile set of all suspicious addresses"""
        
        # From self-transfers
        if not self.results['self_transfers'].empty:
            self.suspicious_addresses.update(self.results['self_transfers']['wallet'])
        
        # From round-trips
        if not self.results['rapid_roundtrips'].empty:
            self.suspicious_addresses.update(self.results['rapid_roundtrips']['wallet_a'])
            self.suspicious_addresses.update(self.results['rapid_roundtrips']['wallet_b'])
        
        # From high-frequency pairs
        if not self.results['high_frequency_pairs'].empty:
            self.suspicious_addresses.update(self.results['high_frequency_pairs']['wallet_a'])
            self.suspicious_addresses.update(self.results['high_frequency_pairs']['wallet_b'])
        
        # From circular patterns
        if not self.results['circular_patterns'].empty:
            for wallets_list in self.results['circular_patterns']['wallets']:
                self.suspicious_addresses.update(wallets_list)
        
        print(f"\n{'='*70}")
        print(f"🚨 IDENTIFIED {len(self.suspicious_addresses)} SUSPICIOUS WALLETS")
        print(f"{'='*70}")
    
    def generate_report(self) -> str:
        """Generate comprehensive analysis report"""
        
        report = []
        report.append("="*70)
        report.append("WASH TRADING ANALYSIS REPORT")
        report.append("Transaction-Hash Based Detection")
        report.append("="*70)
        report.append(f"\nGenerated: {pd.Timestamp.now()}")
        report.append(f"Original transfers: {len(self.df):,}")
        report.append(f"User-to-user flows extracted: {len(self.user_flows):,}")
        report.append(f"Date range: {self.user_flows['timestamp'].min()} to {self.user_flows['timestamp'].max()}")
        report.append("\n" + "="*70)
        
        # Summary
        report.append("\n🔍 DETECTION SUMMARY:\n")
        
        findings = []
        
        if not self.results['self_transfers'].empty:
            count = len(self.results['self_transfers'])
            findings.append(f"   🚨 {count} wallets with self-transfers")
        
        if not self.results['rapid_roundtrips'].empty:
            count = len(self.results['rapid_roundtrips'])
            findings.append(f"   🚨 {count} wallet pairs with rapid round-trips")
        
        if not self.results['high_frequency_pairs'].empty:
            count = len(self.results['high_frequency_pairs'])
            findings.append(f"   🚨 {count} high-frequency trading pairs")
        
        if not self.results['circular_patterns'].empty:
            count = len(self.results['circular_patterns'])
            findings.append(f"   🚨 {count} circular trading patterns")
        
        if findings:
            report.extend(findings)
            report.append(f"\n   TOTAL SUSPICIOUS WALLETS: {len(self.suspicious_addresses)}")
        else:
            report.append("   ✅ No obvious wash trading patterns detected")
        
        # Volume concentration
        if not self.results['volume_concentration'].empty:
            report.append("\n" + "="*70)
            report.append("\n💰 VOLUME CONCENTRATION:\n")
            top_10_pct = self.results['volume_concentration'].head(10)['volume_percentage'].sum()
            report.append(f"   Top 10 wallets control: {top_10_pct:.2f}% of volume")
            
            if top_10_pct > 60:
                report.append("   ⚠️ HIGH CONCENTRATION - Strong wash trading indicator")
            elif top_10_pct > 40:
                report.append("   ⚠️ MEDIUM CONCENTRATION - Potential wash trading")
        
        report.append("\n" + "="*70)
        
        return "\n".join(report)
    
    def save_results(self, output_dir: str = "outputs/reports"):
        """Save all analysis results"""
        
        os.makedirs(output_dir, exist_ok=True)
        timestamp = pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')
        
        # Save user flows
        if self.user_flows is not None and len(self.user_flows) > 0:
            flows_file = f"{output_dir}/user_flows_{timestamp}.csv"
            self.user_flows.to_csv(flows_file, index=False)
            print(f"✓ Saved user flows: {flows_file}")
        
        # Save each analysis result
        for name, df in self.results.items():
            if isinstance(df, pd.DataFrame) and not df.empty:
                filename = f"{output_dir}/{name}_{timestamp}.csv"
                df.to_csv(filename, index=False)
                print(f"✓ Saved: {filename}")
        
        # Save text report
        report_text = self.generate_report()
        report_file = f"{output_dir}/wash_trading_report_{timestamp}.txt"
        with open(report_file, 'w') as f:
            f.write(report_text)
        print(f"✓ Saved report: {report_file}")
        
        # Save suspicious addresses
        if self.suspicious_addresses:
            suspicious_file = f"{output_dir}/suspicious_wallets_{timestamp}.txt"
            with open(suspicious_file, 'w') as f:
                for addr in sorted(self.suspicious_addresses):
                    f.write(f"{addr}\n")
            print(f"✓ Saved suspicious wallets: {suspicious_file}")


def main():
    """Main execution"""
    
    if len(sys.argv) < 2:
        print("Usage: python detect_wash_trading.py <processed_csv_filename>")
        print("Example: python detect_wash_trading.py WIF_solana_30d_processed.csv")
        sys.exit(1)
    
    csv_filename = sys.argv[1]
    
    try:
        # Initialize detector
        detector = TransactionBasedWashTradingDetector(csv_filename)
        
        # Extract user flows
        detector.extract_user_flows()
        
        # Run all analyses
        if len(detector.user_flows) > 0:
            detector.run_all_analyses()
            
            # Print report
            print("\n" + detector.generate_report())
            
            # Save results
            print("\n" + "="*70)
            print("SAVING RESULTS")
            print("="*70 + "\n")
            detector.save_results()
            
            print("\n✅ Analysis complete!")
            print("\nGenerated files:")
            print("  • user_flows_*.csv - Extracted start→end wallet flows")
            print("  • self_transfers_*.csv - Self-transfer patterns")
            print("  • rapid_roundtrips_*.csv - Round-trip trading pairs")
            print("  • high_frequency_pairs_*.csv - High-frequency pairs")
            print("  • circular_patterns_*.csv - Circular trading patterns")
            print("  • wash_trading_report_*.txt - Comprehensive report")
            print("  • suspicious_wallets_*.txt - List of flagged wallets")
        else:
            print("\n⚠️ No user-to-user flows found. Unable to perform wash trading analysis.")
            print("This could mean:")
            print("  • All transactions involve intermediaries only")
            print("  • Dataset doesn't contain actual user wallet activity")
            print("  • Token may be primarily traded through aggregators")
        
    except FileNotFoundError as e:
        print(f"\n✗ Error: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"\n✗ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
