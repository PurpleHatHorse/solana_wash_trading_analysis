"""
Bot Detection using Multi-Endpoint Analysis
"""

import pandas as pd
import numpy as np
import requests
import time
import json
import os
from typing import Dict, List, Tuple, Optional
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed


class MultiEndpointDataAggregator:
    """Fetches data from multiple Arkham API endpoints with rate limiting"""
    
    def __init__(self, api_key: str, base_url: str, cache_dir: str):
        self.api_key = api_key
        self.base_url = base_url
        self.headers = {"API-Key": api_key}
        self.cache_dir = cache_dir
        os.makedirs(self.cache_dir, exist_ok=True)
        
        self.request_times = {'standard': [], 'heavy': []}
        self.heavy_endpoints = {
            '/transfers', '/counterparties/address/', '/counterparties/entity/',
            '/token/top_flow/', '/token/volume/', '/transfers/histogram'
        }
    
    def _is_heavy_endpoint(self, endpoint: str) -> bool:
        return any(heavy in endpoint for heavy in self.heavy_endpoints)
    
    def _enforce_rate_limit(self, endpoint: str):
        endpoint_type = 'heavy' if self._is_heavy_endpoint(endpoint) else 'standard'
        limit = 1 if endpoint_type == 'heavy' else 20
        window = 1.0
        
        now = time.time()
        self.request_times[endpoint_type] = [
            t for t in self.request_times[endpoint_type] if now - t < window
        ]
        
        if len(self.request_times[endpoint_type]) >= limit:
            sleep_time = window - (now - self.request_times[endpoint_type][0])
            if sleep_time > 0:
                time.sleep(sleep_time)
        
        self.request_times[endpoint_type].append(time.time())
    
    def _get_cache_path(self, endpoint: str, params: dict) -> str:
        param_str = "_".join(f"{k}={v}" for k, v in sorted(params.items()) if v is not None)
        cache_key = f"{endpoint.replace('/', '_')}_{param_str}.json"
        return os.path.join(self.cache_dir, cache_key)
    
    def _load_from_cache(self, cache_path: str, max_age_hours: int = 24) -> Optional[dict]:
        if not os.path.exists(cache_path):
            return None
        
        cache_age = time.time() - os.path.getmtime(cache_path)
        if cache_age > max_age_hours * 3600:
            return None
        
        try:
            with open(cache_path, 'r') as f:
                return json.load(f)
        except:
            return None
    
    def _save_to_cache(self, cache_path: str, data: dict):
        try:
            with open(cache_path, 'w') as f:
                json.dump(data, f)
        except:
            pass
    
    def api_call(self, endpoint: str, params: dict = None, retries: int = 3) -> Optional[dict]:
        params = params or {}
        
        cache_path = self._get_cache_path(endpoint, params)
        cached_data = self._load_from_cache(cache_path)
        if cached_data is not None:
            return cached_data
        
        self._enforce_rate_limit(endpoint)
        url = f"{self.base_url}{endpoint}"
        
        for attempt in range(retries):
            try:
                response = requests.get(url, headers=self.headers, params=params, timeout=30)
                
                if response.status_code == 429:
                    time.sleep(2 ** attempt)
                    continue
                
                if response.status_code == 404:
                    return None
                
                response.raise_for_status()
                data = response.json()
                self._save_to_cache(cache_path, data)
                return data
                
            except requests.exceptions.RequestException:
                if attempt == retries - 1:
                    return None
                time.sleep(1)
        
        return None
    
    def batch_api_calls(self, calls: List[Tuple[str, dict]], max_workers: int = 5) -> List[Optional[dict]]:
        results = [None] * len(calls)
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_index = {
                executor.submit(self.api_call, endpoint, params): idx
                for idx, (endpoint, params) in enumerate(calls)
            }
            
            for future in tqdm(as_completed(future_to_index), total=len(calls), desc="  API Calls", leave=False):
                idx = future_to_index[future]
                try:
                    results[idx] = future.result()
                except:
                    results[idx] = None
        
        return results


class BotDetector:
    """Bot detection using multi-endpoint analysis"""
    
    THRESHOLDS = {
        'high_frequency_txs_per_hour': 10,
        'precision_decimals': 4,
        'counterparty_diversity': 5,
        'off_hours_ratio': 0.4,
        'concentration_threshold': 0.7
    }
    
    def __init__(self, api_key: str, user_flows: pd.DataFrame, 
                 base_url: str = "https://api.arkm.com",
                 cache_dir: str = "data/cache",
                 endpoints: List[str] = None,
                 time_window: str = "7d",
                 max_workers: int = 3):
        self.api_key = api_key
        self.user_flows = user_flows
        self.base_url = base_url
        self.cache_dir = cache_dir
        self.time_window = time_window
        self.max_workers = max_workers
        self.endpoints = endpoints or ['transfers', 'intelligence', 'counterparties']
        self.aggregator = MultiEndpointDataAggregator(api_key, base_url, cache_dir)
        self.wallet_data = {}
        self.classification_results = None
    
    def batch_fetch_wallet_data(self, wallets: List[str]) -> Dict[str, Dict]:
        """Fetch multi-endpoint data for wallets"""
        
        print(f"\n{'='*70}")
        print(f"FETCHING API DATA: {len(self.endpoints)} endpoints × {len(wallets)} wallets")
        print(f"{'='*70}\n")
        
        all_wallet_data = {}
        calls = []
        call_metadata = []
        
        for wallet in wallets:
            if 'transfers' in self.endpoints:
                calls.append(('/transfers', {
                    'base': wallet,
                    'timeLast': self.time_window,
                    'limit': 100
                }))
                call_metadata.append((wallet, 'transfers'))
            
            if 'counterparties' in self.endpoints:
                calls.append((f'/counterparties/address/{wallet}', {
                    'flow': 'either',
                    'timeLast': self.time_window,
                    'limit': 50
                }))
                call_metadata.append((wallet, 'counterparties'))
            
            if 'intelligence' in self.endpoints:
                calls.append((f'/intelligence/address_enriched/{wallet}/all', {
                    'includeTags': 'true',
                    'includeEntityPredictions': 'true',
                    'includeClusterIds': 'true'
                }))
                call_metadata.append((wallet, 'intelligence'))
            
            if 'balances' in self.endpoints:
                calls.append((f'/balances/address/{wallet}', {}))
                call_metadata.append((wallet, 'balances'))
            
            if 'portfolio' in self.endpoints:
                calls.append((f'/portfolio/address/{wallet}', {}))
                call_metadata.append((wallet, 'portfolio'))
            
            if 'flow' in self.endpoints:
                calls.append((f'/flow/address/{wallet}', {}))
                call_metadata.append((wallet, 'flow'))
        
        print(f"Total API calls: {len(calls)}")
        results = self.aggregator.batch_api_calls(calls, max_workers=self.max_workers)
        
        for result, (wallet, endpoint_name) in zip(results, call_metadata):
            if wallet not in all_wallet_data:
                all_wallet_data[wallet] = {'wallet': wallet}
            all_wallet_data[wallet][endpoint_name] = result
        
        self.wallet_data = all_wallet_data
        
        total_expected = len(wallets) * len(self.endpoints)
        total_received = sum(
            sum(1 for k, v in data.items() if k != 'wallet' and v is not None)
            for data in all_wallet_data.values()
        )
        
        print(f"\n✓ Fetched data for {len(all_wallet_data)} wallets")
        print(f"✓ Success rate: {total_received}/{total_expected} ({total_received/total_expected*100:.1f}%)")
        
        return all_wallet_data
    
    def extract_features(self, wallet: str) -> Dict:
        """Extract features from multi-endpoint data"""
        
        if wallet not in self.wallet_data:
            return {}
        
        data = self.wallet_data[wallet]
        features = {'wallet': wallet}
        
        # TRANSFERS endpoint features
        if 'transfers' in data and data['transfers']:
            transfers = data['transfers']
            if isinstance(transfers, dict) and 'transfers' in transfers:
                transfers_df = pd.DataFrame(transfers['transfers'])
                
                if not transfers_df.empty:
                    features['api_transfer_count'] = len(transfers_df)
                    features['api_total_volume'] = transfers_df.get('historicalUSD', pd.Series([0])).sum()
                    
                    if 'blockTimestamp' in transfers_df.columns:
                        transfers_df['blockTimestamp'] = pd.to_datetime(transfers_df['blockTimestamp'])
                        time_span = (transfers_df['blockTimestamp'].max() - 
                                   transfers_df['blockTimestamp'].min()).total_seconds() / 3600
                        features['api_txs_per_hour'] = len(transfers_df) / max(time_span, 1)
                        
                        transfers_df['hour'] = transfers_df['blockTimestamp'].dt.hour
                        off_hours = transfers_df[transfers_df['hour'].between(0, 6)].shape[0]
                        features['api_off_hours_ratio'] = off_hours / len(transfers_df)
                        
                        time_diffs = transfers_df['blockTimestamp'].diff().dt.total_seconds().dropna()
                        if len(time_diffs) > 0:
                            features['api_time_regularity_cv'] = time_diffs.std() / (time_diffs.mean() + 1)
                    
                    if 'historicalUSD' in transfers_df.columns:
                        usd_values = transfers_df['historicalUSD'].dropna()
                        if len(usd_values) > 0:
                            features['api_value_cv'] = usd_values.std() / (usd_values.mean() + 1)
                            
                            def count_decimals(value):
                                if pd.isna(value) or value == 0:
                                    return 0
                                value_str = f"{value:.10f}".rstrip('0')
                                return len(value_str.split('.')[1]) if '.' in value_str else 0
                            
                            decimals = usd_values.apply(count_decimals)
                            features['api_avg_decimal_places'] = decimals.mean()
        
        # COUNTERPARTIES endpoint features
        if 'counterparties' in data and data['counterparties']:
            cp = data['counterparties']
            if isinstance(cp, dict) and 'counterparties' in cp:
                cp_df = pd.DataFrame(cp['counterparties'])
                
                if not cp_df.empty:
                    features['cp_unique_counterparties'] = len(cp_df)
                    
                    if 'totalVolumeUSD' in cp_df.columns:
                        total_vol = cp_df['totalVolumeUSD'].sum()
                        if total_vol > 0:
                            features['cp_top_counterparty_ratio'] = cp_df['totalVolumeUSD'].iloc[0] / total_vol
                    
                    if 'arkhamEntity' in cp_df.columns:
                        entity_types = cp_df['arkhamEntity'].apply(
                            lambda x: x.get('type') if isinstance(x, dict) else None
                        )
                        features['cp_dex_ratio'] = (entity_types == 'dex').sum() / len(cp_df)
                        features['cp_cex_ratio'] = (entity_types == 'cex').sum() / len(cp_df)
        
        # INTELLIGENCE endpoint features
        if 'intelligence' in data and data['intelligence']:
            intel = data['intelligence']
            all_tags = []
            has_prediction = False
            
            if isinstance(intel, list):
                for chain_data in intel:
                    if isinstance(chain_data, dict):
                        if 'tags' in chain_data and chain_data['tags']:
                            all_tags.extend([tag.get('name', '') for tag in chain_data['tags']])
                        if 'entityPredictions' in chain_data and chain_data['entityPredictions']:
                            has_prediction = True
            
            features['intel_tag_count'] = len(set(all_tags))
            features['intel_has_entity_prediction'] = has_prediction
            
            bot_keywords = ['bot', 'mev', 'flashbot', 'arbitrage', 'automated', 'sniper']
            features['intel_has_bot_tag'] = any(
                any(keyword in tag.lower() for keyword in bot_keywords) for tag in all_tags
            )
        
        # BALANCES endpoint features
        if 'balances' in data and data['balances']:
            balances = data['balances']
            if isinstance(balances, dict) and 'balances' in balances:
                balances_df = pd.DataFrame(balances['balances'])
                
                if not balances_df.empty:
                    features['bal_token_diversity'] = len(balances_df)
                    if 'balanceUSD' in balances_df.columns:
                        features['bal_total_usd'] = balances_df['balanceUSD'].sum()
                        if features['bal_total_usd'] > 0:
                            features['bal_portfolio_concentration'] = (
                                balances_df['balanceUSD'].max() / features['bal_total_usd']
                            )
        
        # FLOW endpoint features
        if 'flow' in data and data['flow']:
            flow = data['flow']
            if isinstance(flow, dict) and 'flows' in flow:
                flow_df = pd.DataFrame(flow['flows'])
                
                if not flow_df.empty:
                    if 'inflowUSD' in flow_df.columns and 'outflowUSD' in flow_df.columns:
                        total_inflow = flow_df['inflowUSD'].sum()
                        total_outflow = flow_df['outflowUSD'].sum()
                        
                        if (total_inflow + total_outflow) > 0:
                            features['flow_balance_ratio'] = (
                                (total_inflow - total_outflow) / (total_inflow + total_outflow)
                            )
        
        return features
    
    def calculate_bot_score(self, features: Dict) -> Tuple[float, Dict]:
        """Calculate bot probability score"""
        
        score = 0.0
        max_score = 0.0
        reasoning = {}
        
        weights = {
            'timing': 0.20,
            'patterns': 0.20,
            'counterparty': 0.20,
            'intelligence': 0.25,
            'portfolio': 0.10,
            'flow': 0.05
        }
        
        # TIMING
        timing_score = 0
        timing_max = 5
        
        if features.get('api_txs_per_hour', 0) > self.THRESHOLDS['high_frequency_txs_per_hour']:
            timing_score += 1.5
            reasoning['high_frequency'] = True
        
        if features.get('api_time_regularity_cv', 1) < 0.5:
            timing_score += 1.5
            reasoning['regular_timing'] = True
        
        if features.get('api_off_hours_ratio', 0) > self.THRESHOLDS['off_hours_ratio']:
            timing_score += 2
            reasoning['off_hours_active'] = True
        
        score += (timing_score / timing_max) * weights['timing']
        max_score += weights['timing']
        
        # PATTERNS
        pattern_score = 0
        pattern_max = 3
        
        if features.get('api_avg_decimal_places', 0) >= self.THRESHOLDS['precision_decimals']:
            pattern_score += 1.5
            reasoning['precise_values'] = True
        
        if features.get('api_value_cv', 1) < 0.3:
            pattern_score += 1.5
            reasoning['consistent_values'] = True
        
        score += (pattern_score / pattern_max) * weights['patterns']
        max_score += weights['patterns']
        
        # COUNTERPARTY
        cp_score = 0
        cp_max = 4
        
        if features.get('cp_unique_counterparties', 100) < self.THRESHOLDS['counterparty_diversity']:
            cp_score += 2
            reasoning['limited_counterparties'] = True
        
        if features.get('cp_top_counterparty_ratio', 0) > 0.6:
            cp_score += 2
            reasoning['concentrated_trading'] = True
        
        score += (cp_score / cp_max) * weights['counterparty']
        max_score += weights['counterparty']
        
        # INTELLIGENCE
        intel_score = 0
        intel_max = 3
        
        if features.get('intel_has_bot_tag', False):
            intel_score += 3
            reasoning['bot_tag_detected'] = True
        elif features.get('intel_has_entity_prediction', False):
            intel_score += 1
            reasoning['known_entity'] = True
        
        score += (intel_score / intel_max) * weights['intelligence']
        max_score += weights['intelligence']
        
        # PORTFOLIO
        port_score = 0
        port_max = 2
        
        if features.get('bal_portfolio_concentration', 0) > 0.9:
            port_score += 1
            reasoning['concentrated_portfolio'] = True
        
        if features.get('bal_token_diversity', 100) < 3:
            port_score += 1
            reasoning['low_diversity'] = True
        
        score += (port_score / port_max) * weights['portfolio']
        max_score += weights['portfolio']
        
        # FLOW
        flow_score = 0
        flow_max = 1
        
        if abs(features.get('flow_balance_ratio', 0)) < 0.1:
            flow_score += 1
            reasoning['balanced_flow'] = True
        
        score += (flow_score / flow_max) * weights['flow']
        max_score += weights['flow']
        
        normalized_score = score / max_score if max_score > 0 else 0
        return normalized_score, reasoning
    
    def classify_wallets(self, min_transactions: int = 5, sample_size: int = None) -> pd.DataFrame:
        """Classify wallets as BOT or HUMAN"""
        
        print(f"\n{'='*70}")
        print("BOT DETECTION")
        print(f"{'='*70}\n")
        
        # Get eligible wallets
        start_wallets = self.user_flows['start_wallet'].value_counts()
        end_wallets = self.user_flows['end_wallet'].value_counts()
        all_wallets = (start_wallets + end_wallets.reindex(start_wallets.index, fill_value=0)).sort_values(ascending=False)
        
        eligible_wallets = all_wallets[all_wallets >= min_transactions].index.tolist()
        
        if sample_size:
            eligible_wallets = eligible_wallets[:sample_size]
        
        print(f"Wallets to analyze: {len(eligible_wallets)}")
        print(f"Min transactions: {min_transactions}")
        
        # Fetch API data
        self.batch_fetch_wallet_data(eligible_wallets)
        
        # Extract features and classify
        print(f"\n{'='*70}")
        print("FEATURE EXTRACTION & CLASSIFICATION")
        print(f"{'='*70}\n")
        
        results = []
        
        for wallet in tqdm(eligible_wallets, desc="Classifying"):
            wallet_txs = self.user_flows[
                (self.user_flows['start_wallet'] == wallet) |
                (self.user_flows['end_wallet'] == wallet)
            ]
            
            features = {
                'wallet': wallet,
                'local_tx_count': len(wallet_txs),
                'local_volume_usd': wallet_txs['usd_value'].sum()
            }
            
            api_features = self.extract_features(wallet)
            features.update(api_features)
            
            bot_score, reasoning = self.calculate_bot_score(features)
            features['bot_score'] = bot_score
            features['bot_confidence'] = 'HIGH' if bot_score > 0.7 else 'MEDIUM' if bot_score > 0.4 else 'LOW'
            features['classification'] = 'BOT' if bot_score > 0.6 else 'UNCERTAIN' if bot_score > 0.4 else 'HUMAN'
            features['reasoning'] = str(reasoning)
            
            results.append(features)
        
        self.classification_results = pd.DataFrame(results).sort_values('bot_score', ascending=False)
        
        # Summary
        bot_count = (self.classification_results['classification'] == 'BOT').sum()
        human_count = (self.classification_results['classification'] == 'HUMAN').sum()
        uncertain_count = (self.classification_results['classification'] == 'UNCERTAIN').sum()
        
        print(f"\n{'='*70}")
        print("BOT CLASSIFICATION SUMMARY")
        print(f"{'='*70}\n")
        print(f"🤖 BOTS:      {bot_count:4d} ({bot_count/len(results)*100:5.1f}%)")
        print(f"👤 HUMANS:    {human_count:4d} ({human_count/len(results)*100:5.1f}%)")
        print(f"❓ UNCERTAIN: {uncertain_count:4d} ({uncertain_count/len(results)*100:5.1f}%)")
        
        return self.classification_results