"""
Holder Analysis
Analyzes token holder concentration and AMM liquidity
"""

import pandas as pd
import time
from typing import Dict, Any, List, Tuple, Optional
from data_fetcher import LiveDataFetcher

class HolderAnalyzer:
    """Analyzes token holder distribution and liquidity concentration"""

    def __init__(self, fetcher: LiveDataFetcher, token_address: str, token_name: str, chain: str):
        self.fetcher = fetcher
        self.token_address = token_address
        self.token_name = token_name
        self.chain = chain
        self.raw_data = None
        self.df_holders = None

    def _extract_label(self, address_info):
        """Helper to extract a readable label if arkham entity data exists."""
        if "arkhamEntity" in address_info:
            entity_name = address_info["arkhamEntity"].get("name")
            if entity_name:
                return entity_name

        if "arkhamLabel" in address_info:
            label_name = address_info["arkhamLabel"].get("name")
            if label_name:
                return label_name

        return "Wallet"

    def process_holder_data(self) -> pd.DataFrame:
        """Parses raw JSON data into a DataFrame"""

        print(f"\n[1/3] Processing Top Holders...")

        # Fetch data using the passed fetcher instance
        self.raw_data = self.fetcher.fetch_token_holders(self.token_address, self.chain)

        if not self.raw_data:
            print("  ⚠ No holder data returned")
            return pd.DataFrame()

        holders_data = self.raw_data.get("addressTopHolders", {}).get(self.chain, [])

        if not holders_data:
            print(f"  ⚠ No holder list found for chain: {self.chain}")
            return pd.DataFrame()

        processed_list = []
        for entry in holders_data:
            address_info = entry.get("address", {})
            current_address = address_info.get("address", "Unknown")
            balance = entry.get("balance", 0)
            usd_value = entry.get("usd", 0)
            pct_of_cap = entry.get("pctOfCap", 0)

            processed_list.append({
                "Address": current_address,
                "Label": self._extract_label(address_info),
                "Balance": balance,
                "USD Value": usd_value,
                "Holding %": pct_of_cap * 100,
            })

        self.df_holders = pd.DataFrame(processed_list)

        # Sort and Rank
        self.df_holders.sort_values(by="Holding %", ascending=False, inplace=True)
        self.df_holders.reset_index(drop=True, inplace=True)
        self.df_holders["Rank"] = self.df_holders.index + 1

        print(f"  ✓ Extracted {len(self.df_holders):,} unique holders")

        return self.df_holders

    def calculate_concentration_metrics(self) -> Dict[str, float]:
        """Calculates Top 3, Top 10, Gini Coefficient and returns metrics"""

        metrics = {
            "top_3_ratio": 0.0,
            "top_10_ratio": 0.0,
            "whale_dominance": 0.0,
            "gini_coefficient": 0.0
        }

        if self.df_holders is None or self.df_holders.empty:
            return metrics

        print(f"\n[2/3] Calculating Risk Metrics...")

        # Standard Metrics
        top_3_ratio = self.df_holders.head(3)["Holding %"].sum()
        top_10_ratio = self.df_holders.head(10)["Holding %"].sum()
        top_1_dominance = self.df_holders.iloc[0]["Holding %"] if len(self.df_holders) > 0 else 0

        # Gini Coefficient
        sorted_holdings = self.df_holders["Holding %"].sort_values(ascending=True).reset_index(drop=True)
        n = len(sorted_holdings)
        if n == 0:
            gini_coeff = 0
        else:
            index = sorted_holdings.index + 1
            numerator = (2 * index * sorted_holdings).sum()
            denominator = n * sorted_holdings.sum()
            gini_coeff = (numerator / denominator) - ((n + 1) / n)

        print("-" * 50)
        print("CONCENTRATION REPORT:")
        print(f"  • Top 3 Ratio:      {top_3_ratio:.4f}%")
        print(f"  • Top 10 Ratio:     {top_10_ratio:.4f}%")
        print(f"  • Whale Dominance:  {top_1_dominance:.4f}% (Top 1)")
        print(f"  • Gini Coefficient: {gini_coeff:.4f}")

        if top_10_ratio > 80:
            print(f"  🚨 RISK: High Concentration (>80% in Top 10)")
        if gini_coeff > 0.9:
            print(f"  🚨 RISK: Extreme Wealth Inequality (Gini > 0.9)")
        print("-" * 50)

        metrics = {
            "top_3_ratio": top_3_ratio,
            "top_10_ratio": top_10_ratio,
            "whale_dominance": top_1_dominance,
            "gini_coefficient": gini_coeff
        }
        return metrics

    def analyze_amm_liquidity(self):
        """Deep dive into AMM LPs"""
        if self.df_holders is None: return

        print(f"\n[3/3] Analyzing AMM Liquidity from Top 100 holders list...")

        top_100 = self.df_holders.head(100)
        target_amms = ['Orca', 'Raydium', 'Meteora']
        mask = top_100['Label'].str.contains('|'.join(target_amms), case=False, na=False)
        target_lps = top_100[mask]

        if target_lps.empty:
            print("  ✓ No major AMM addresses found in Top 100.")
            return

        results = []
        filtered_target_lps = []

        for _, row in target_lps.iterrows():
            address = row['Address']
            label = row['Label']

            # Fetch balance using the Fetcher class logic
            balance_data = self.fetcher.fetch_wallet_balance(address, self.chain)

            if not balance_data or 'balances' not in balance_data:
                continue

            tokens = balance_data.get('balances', {}).get(self.chain, [])
            tokens_sorted = sorted(tokens, key=lambda x: x.get('usd', 0), reverse=True)

            pair_info = []
            for t in tokens_sorted[:2]:
                symbol = t.get('symbol', 'UNK')
                usd_val = t.get('usd', 0)
                pair_info.append(f"{symbol} (${usd_val/1000:.1f}k)")

            pair_str = " / ".join(pair_info)

            if self.token_name in pair_str:
                filtered_target_lps.append(pair_str)
                results.append({
                    "AMM": label,
                    "Address": address,
                    "Identified Pair": pair_str,
                    "Pool USD": "${:,.2f}".format(row['USD Value'])
                })

            time.sleep(0.2) # Small delay for aesthetics/rate-limit safety
            
        print(f"  -> Found {len(filtered_target_lps)} AMM addresses. Checking pairs...")

        if results:
            print("\n" + "-"*70)
            print("IDENTIFIED LIQUIDITY PAIRS:")
            print("-"*70)
            df_res = pd.DataFrame(results)
            print(df_res.to_string(index=False))
            print("="*70)

    def run_analysis(self) -> Tuple[Dict[str, float], List[str]]:
        """Main execution method - Returns Metrics and Top Holder Addresses"""
        print(f"\n{'='*70}")
        print("HOLDER & RISK ANALYSIS")
        print(f"{'='*70}")

        self.process_holder_data()
        metrics = self.calculate_concentration_metrics()
        self.analyze_amm_liquidity()

        # Return top 50 addresses for Whale Multiplier analysis
        top_holders = []
        if self.df_holders is not None and not self.df_holders.empty:
            top_holders = self.df_holders.head(50)['Address'].tolist()

        return metrics, top_holders
