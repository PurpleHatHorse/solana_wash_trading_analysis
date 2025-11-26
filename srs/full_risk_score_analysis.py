"""
Full Risk Score: Wash Trading + Bot Detection Analysis + Token Holders Analysis
Produces unified risk assessment
"""

import sys
from pathlib import Path

# Add root directory to path
ROOT_DIR = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT_DIR))

import pandas as pd
from typing import Dict, Set, Tuple, List, Any, Optional
from datetime import datetime
import os


class RiskScoreAnalyzer:

    def __init__(self,
                 wash_trading_detector,
                 bot_detector,
                 token: str):
        self.wash_detector = wash_trading_detector
        self.bot_detector = bot_detector
        self.token = token
        self.risk_results = None
        self.token_risk_data = None

    def calculate_token_risk_score(self, holder_metrics: Dict[str, float]) -> Dict[str, Any]:
        """
        Calculates a global risk score for the token asset itself.
        """
        if not holder_metrics:
            return {"token_risk_score": 0, "risk_components": {}}

        # 1. Concentration Component (0-100)
        # If Top 10 hold > 80%, this component hits max risk
        conc_score = min(holder_metrics.get('top_10_ratio', 0) * 1.25, 100)

        # Penalize specifically for high Gini
        if holder_metrics.get('gini_coefficient', 0) > 0.9:
            conc_score = max(conc_score, 90) # Floor at 90 if Gini is extreme

        # 2. Bot Component (0-100)
        total_wallets = len(self.risk_results) if self.risk_results is not None else 0
        bot_count = (self.risk_results['bot_classification'] == 'BOT').sum() if total_wallets > 0 else 0
        bot_ratio = bot_count / total_wallets if total_wallets > 0 else 0
        # If > 30% of wallets are bots, max risk
        bot_score = min(bot_ratio * 333, 100)

        # 3. Wash Trading Component (0-100)
        wash_count = self.risk_results['is_wash_trading'].sum() if total_wallets > 0 else 0
        wash_ratio = wash_count / total_wallets if total_wallets > 0 else 0
        # If > 20% of wallets are wash trading, max risk
        wash_score = min(wash_ratio * 500, 100)

        # Final Weighted Average
        final_token_score = (conc_score * 0.40) + (bot_score * 0.30) + (wash_score * 0.30)

        return {
            "token_risk_score": final_token_score,
            "risk_components": {
                "concentration_risk": conc_score,
                "bot_activity_risk": bot_score,
                "wash_trading_risk": wash_score,
                "bot_ratio": bot_ratio,
                "wash_ratio": wash_ratio
            }
        }

    def create_risk_analysis(self, holder_metrics: Dict = None, top_holders: List[str] = None) -> pd.DataFrame:
        """Create unified analysis with risk scores"""

        print(f"\n{'='*70}")
        print("RISK SCORE ANALYSIS: WASH TRADING + BOT DETECTION + HOLDER ANALYSIS")
        print(f"{'='*70}\n")

        # Get suspicious wallets from wash trading
        wash_wallets = self.wash_detector.suspicious_wallets

        # Get bot classifications
        bot_results = self.bot_detector.classification_results

        # Get all unique wallets
        all_wallets = set(bot_results['wallet'].tolist()) | wash_wallets
        top_holders_set = set(top_holders) if top_holders else set()

        print(f"Total unique wallets analyzed: {len(all_wallets)}")
        print(f"Wash trading suspicious: {len(wash_wallets)}")
        print(f"Bot detected: {(bot_results['classification'] == 'BOT').sum()}")
        if top_holders:
            print(f"Top Holders Integrated: {len(top_holders)}")

        # Build risk dataset
        risk_data = []

        for wallet in all_wallets:
            # Get bot data
            bot_row = bot_results[bot_results['wallet'] == wallet]

            if len(bot_row) > 0:
                bot_row = bot_row.iloc[0]
                bot_score = bot_row['bot_score']
                bot_classification = bot_row['classification']
                bot_confidence = bot_row['bot_confidence']
            else:
                bot_score = 0.0
                bot_classification = 'UNKNOWN'
                bot_confidence = 'N/A'

            # Check wash trading flags
            is_wash_trading = wallet in wash_wallets

            wash_flags = []
            for analysis_name, analysis_df in self.wash_detector.results.items():
                if isinstance(analysis_df, pd.DataFrame) and not analysis_df.empty:
                    if analysis_name in ['self_transfers', 'volume_concentration']:
                        if 'wallet' in analysis_df.columns and wallet in analysis_df['wallet'].values:
                            wash_flags.append(analysis_name)
                    elif analysis_name in ['rapid_roundtrips', 'high_frequency_pairs']:
                        if 'wallet_a' in analysis_df.columns:
                            if wallet in analysis_df['wallet_a'].values or wallet in analysis_df['wallet_b'].values:
                                wash_flags.append(analysis_name)
                    elif analysis_name == 'circular_patterns':
                        if 'wallets' in analysis_df.columns:
                            for wallet_list in analysis_df['wallets']:
                                if wallet in wallet_list:
                                    wash_flags.append(analysis_name)
                                    break

            # Check if wallet is a Top Holder (Whale)
            is_top_holder = wallet in top_holders_set

            # Calculate risk level
            risk_level, risk_score = self._calculate_risk_level(
                bot_score,
                bot_classification,
                is_wash_trading,
                len(wash_flags),
                is_top_holder
            )

            risk_data.append({
                'wallet': wallet,
                'risk_level': risk_level,
                'risk_score': risk_score,
                'bot_score': bot_score,
                'bot_classification': bot_classification,
                'bot_confidence': bot_confidence,
                'is_wash_trading': is_wash_trading,
                'wash_trading_flags': ','.join(wash_flags) if wash_flags else '',
                'wash_flag_count': len(wash_flags),
                'is_top_holder': is_top_holder,
                'risk_threat': 'CRITICAL' if (bot_classification == 'BOT' and is_wash_trading) else
                                  'HIGH' if (bot_classification == 'BOT' or is_wash_trading) else
                                  'MEDIUM' if bot_classification == 'UNCERTAIN' else 'LOW'
            })

        self.risk_results = pd.DataFrame(risk_data).sort_values('risk_score', ascending=False)

        # Calculate Global Token Risk
        if holder_metrics:
            self.token_risk_data = self.calculate_token_risk_score(holder_metrics)

        # Summary stats
        print(f"\n{'='*70}")
        print("RISK LEVEL DISTRIBUTION")
        print(f"{'='*70}\n")

        for risk in ['CRITICAL', 'HIGH', 'MEDIUM', 'LOW']:
            count = (self.risk_results['risk_level'] == risk).sum()
            pct = count / len(self.risk_results) * 100
            print(f"{risk:8s}: {count:4d} wallets ({pct:5.1f}%)")

        # Cross-analysis
        bot_and_wash = self.risk_results[
            (self.risk_results['bot_classification'] == 'BOT') &
            (self.risk_results['is_wash_trading'] == True)
        ]

        print(f"\n🚨 CRITICAL: {len(bot_and_wash)} wallets are BOTH bots AND wash traders")

        # Whale Alert
        whale_threats = self.risk_results[
            (self.risk_results['is_top_holder'] == True) &
            (self.risk_results['risk_level'].isin(['CRITICAL', 'HIGH']))
        ]
        if not whale_threats.empty:
             print(f"🚨 WHALE ALERT: {len(whale_threats)} Top Holders detected engaging in suspicious activity!")

        return self.risk_results

    def _calculate_risk_level(self,
                             bot_score: float,
                             bot_classification: str,
                             is_wash_trading: bool,
                             wash_flag_count: int,
                             is_top_holder: bool = False) -> Tuple[str, float]:
        """Calculate overall risk level and score"""

        risk_score = 0.0

        # Bot contribution (0-50 points)
        risk_score += bot_score * 50

        # Wash trading contribution (0-50 points)
        if is_wash_trading:
            risk_score += min(wash_flag_count * 15, 50)

        # Whale Multiplier
        # If a suspicious actor is also a top holder, max out the risk immediately
        if is_top_holder:
            if is_wash_trading or bot_classification == 'BOT':
                risk_score = 100  # Instant Critical: A whale acting like a bot/washer is maximum danger
            else:
                risk_score += 20  # Just being a whale adds base scrutiny (risk premium)

        # Normalize to 0-100
        risk_score = min(risk_score, 100)

        # Determine risk level
        if risk_score >= 75:
            risk_level = 'CRITICAL'
        elif risk_score >= 50:
            risk_level = 'HIGH'
        elif risk_score >= 25:
            risk_level = 'MEDIUM'
        else:
            risk_level = 'LOW'

        return risk_level, risk_score

    def generate_report(self) -> str:
        """Generate comprehensive text report"""

        report = []
        report.append("="*70)
        report.append(f"RISK SCORE ANALYSIS REPORT: {self.token}")
        report.append("="*70)
        report.append(f"\nGenerated: {datetime.now()}")
        report.append(f"Token: {self.token}")
        report.append(f"Total Wallets Analyzed: {len(self.risk_results)}")

        # Token Global Risk Score
        if self.token_risk_data:
            tr = self.token_risk_data
            components = tr['risk_components']
            report.append("\n" + "="*70)
            report.append(f"TOKEN GLOBAL HEALTH SCORE: {100 - tr['token_risk_score']:.1f}/100")
            report.append("="*70)
            report.append(f"Global Risk Score: {tr['token_risk_score']:.1f} / 100 (Lower is Better)")
            report.append("Risk Breakdown:")
            report.append(f"  • Concentration Risk: {components.get('concentration_risk', 0):.1f}/100")
            report.append(f"  • Bot Activity Risk:  {components.get('bot_activity_risk', 0):.1f}/100 (Ratio: {components.get('bot_ratio', 0)*100:.1f}%)")
            report.append(f"  • Wash Trading Risk:  {components.get('wash_trading_risk', 0):.1f}/100 (Ratio: {components.get('wash_ratio', 0)*100:.1f}%)")

        # Risk distribution
        report.append("\n" + "-"*70)
        report.append("RISK DISTRIBUTION")
        report.append("-"*70 + "\n")

        for risk in ['CRITICAL', 'HIGH', 'MEDIUM', 'LOW']:
            subset = self.risk_results[self.risk_results['risk_level'] == risk]
            count = len(subset)
            pct = count / len(self.risk_results) * 100
            report.append(f"{risk:10s}: {count:4d} wallets ({pct:5.1f}%)")

        # Top threats
        report.append("\n" + "-"*70)
        report.append("TOP 10 HIGHEST RISK WALLETS")
        report.append("-"*70 + "\n")

        top_10 = self.risk_results.head(10)
        for idx, row in top_10.iterrows():
            whale_tag = " [WHALE]" if row['is_top_holder'] else ""
            report.append(f"Wallet: {row['wallet']}{whale_tag}")
            report.append(f"  Risk: {row['risk_level']} (Score: {row['risk_score']:.1f}/100)")
            report.append(f"  Bot: {row['bot_classification']} (Score: {row['bot_score']:.3f})")
            report.append(f"  Wash Trading: {'YES' if row['is_wash_trading'] else 'No'}")
            if row['wash_trading_flags']:
                report.append(f"  Wash Flags: {row['wash_trading_flags']}")
            report.append("")

        # Key findings
        report.append("\n" + "-"*70)
        report.append("KEY FINDINGS")
        report.append("-"*70 + "\n")

        critical = self.risk_results[self.risk_results['risk_level'] == 'CRITICAL']
        bots = self.risk_results[self.risk_results['bot_classification'] == 'BOT']
        wash = self.risk_results[self.risk_results['is_wash_trading'] == True]
        both = self.risk_results[
            (self.risk_results['bot_classification'] == 'BOT') &
            (self.risk_results['is_wash_trading'] == True)
        ]
        whales = self.risk_results[
            (self.risk_results['is_top_holder'] == True) &
            (self.risk_results['risk_level'].isin(['CRITICAL', 'HIGH']))
        ]

        report.append(f"• Critical Risk Wallets: {len(critical)}")
        report.append(f"• Confirmed Bots: {len(bots)}")
        report.append(f"• Wash Traders: {len(wash)}")
        report.append(f"• Bots + Wash Trading: {len(both)} (HIGHEST CONCERN)")
        report.append(f"• Suspicious Whales:   {len(whales)} (Top Holders with High Risk)")

        if len(whales) > 0:
            report.append(f"\n⚠️  WHALE WARNING: {len(whales)} Top Holders are flagged as High/Critical Risk!")

        report.append("\n" + "="*70)

        return "\n".join(report)

    def save_results(self, output_dir: str, token: str):
        """Save all risk results"""

        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        os.makedirs(output_dir, exist_ok=True)

        # Save master CSV
        master_file = f"{output_dir}/{token}_risk_analysis_{timestamp}.csv"
        self.risk_results.to_csv(master_file, index=False)
        print(f"✓ Saved master analysis: {master_file}")

        # Save critical wallets
        critical = self.risk_results[self.risk_results['risk_level'] == 'CRITICAL']
        if len(critical) > 0:
            critical_file = f"{output_dir}/{token}_critical_wallets_{timestamp}.txt"
            with open(critical_file, 'w') as f:
                f.write(f"CRITICAL RISK WALLETS: {token}\n")
                f.write("="*70 + "\n\n")
                for _, row in critical.iterrows():
                    whale_tag = " [WHALE]" if row['is_top_holder'] else ""
                    f.write(f"{row['wallet']}{whale_tag}\n")
                    f.write(f"  Risk Score: {row['risk_score']:.1f}/100\n")
                    f.write(f"  Bot Score: {row['bot_score']:.3f}\n")
                    f.write(f"  Wash Trading: {'YES' if row['is_wash_trading'] else 'No'}\n")
                    if row['wash_trading_flags']:
                        f.write(f"  Flags: {row['wash_trading_flags']}\n")
                    f.write("\n")
            print(f"✓ Saved critical wallets: {critical_file}")

        # Save text report
        report_file = f"{output_dir}/{token}_risk_report_{timestamp}.txt"
        with open(report_file, 'w') as f:
            f.write(self.generate_report())
        print(f"✓ Saved risk report: {report_file}")
