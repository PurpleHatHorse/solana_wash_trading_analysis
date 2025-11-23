"""
Arkham Intelligence API Client
Handles all interactions with Arkham API for transfer data collection
"""

import requests
import time
import json
import os
from typing import Dict, List, Optional
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()


class ArkhamAPIClient:
    """Client for Arkham Intelligence Transfer API"""
    
    def __init__(self):
        self.api_key = os.getenv('ARKHAM_API_KEY')
        self.base_url = os.getenv('ARKHAM_BASE_URL', 'https://api.arkm.com')
        self.rate_limit_delay = float(os.getenv('RATE_LIMIT_DELAY', '1.1'))
        
        if not self.api_key:
            raise ValueError("ARKHAM_API_KEY not found in environment variables")
        
        self.headers = {
            'API-Key': self.api_key,
            'Content-Type': 'application/json'
        }
        
        self.session = requests.Session()
        self.session.headers.update(self.headers)
        
        print(f"✓ Arkham API Client initialized")
        print(f"  Base URL: {self.base_url}")
        print(f"  Rate Limit Delay: {self.rate_limit_delay}s")
    
    def _make_request(self, endpoint: str, params: Dict) -> Optional[Dict]:
        """
        Make API request with error handling and rate limiting
        
        Args:
            endpoint: API endpoint (e.g., '/transfers')
            params: Query parameters
            
        Returns:
            Response JSON or None if error
        """
        url = f"{self.base_url}{endpoint}"
        
        # Remove None values from params
        params = {k: v for k, v in params.items() if v is not None}
        
        try:
            response = self.session.get(url, params=params, timeout=30)
            
            # Handle rate limiting
            if response.status_code == 429:
                print("⚠ Rate limited. Waiting 60 seconds...")
                time.sleep(60)
                return self._make_request(endpoint, params)
            
            # Handle success
            if response.status_code == 200:
                # Rate limit: 1 request per second
                time.sleep(self.rate_limit_delay)
                return response.json()
            
            # Handle errors
            elif response.status_code == 400:
                print(f"✗ Bad Request (400): {response.text}")
                return None
            
            elif response.status_code == 401:
                print(f"✗ Unauthorized (401): Check your API key")
                return None
            
            else:
                print(f"✗ Error {response.status_code}: {response.text}")
                return None
                
        except requests.exceptions.Timeout:
            print(f"✗ Request timeout for {url}")
            return None
            
        except requests.exceptions.RequestException as e:
            print(f"✗ Request failed: {e}")
            return None
    
    def get_transfers(
        self,
        # Core filters
        chains: str = 'solana',
        tokens: Optional[str] = None,
        base: Optional[str] = None,
        
        # Direction filters
        flow: Optional[str] = None,  # "in", "out", "self", "all"
        from_addresses: Optional[str] = None,
        to_addresses: Optional[str] = None,
        counterparties: Optional[str] = None,
        
        # Time filters
        timeLast: Optional[str] = None,
        timeGte: Optional[str] = None,
        timeLte: Optional[str] = None,
        
        # Value filters
        valueGte: Optional[float] = None,
        valueLte: Optional[float] = None,
        usdGte: Optional[float] = None,
        usdLte: Optional[float] = None,
        
        # Sorting & pagination
        sortKey: str = 'time',
        sortDir: str = 'desc',
        limit: int = 100,
        offset: int = 0
    ) -> Optional[Dict]:
        """
        Get transfers from Arkham API
        
        Args:
            chains: Chain name (e.g., 'solana', 'ethereum')
            tokens: Token address or ID
            base: Base entity or address for filtering
            flow: Transfer direction ("in", "out", "self", "all")
            from_addresses: Comma-separated sender addresses
            to_addresses: Comma-separated receiver addresses
            counterparties: Strict counterparty filter
            timeLast: Relative time filter (e.g., "24h", "7d")
            timeGte: Start timestamp in milliseconds
            timeLte: End timestamp in milliseconds
            valueGte: Minimum raw token value
            valueLte: Maximum raw token value
            usdGte: Minimum USD value
            usdLte: Maximum USD value
            sortKey: Sort field ("time", "value", "usd")
            sortDir: Sort direction ("asc", "desc")
            limit: Max results (default 50, max 100)
            offset: Pagination offset
            
        Returns:
            API response with 'transfers' and 'count' fields
        """
        
        params = {
            'chains': chains,
            'tokens': tokens,
            'base': base,
            'flow': flow,
            'from': from_addresses,
            'to': to_addresses,
            'counterparties': counterparties,
            'timeLast': timeLast,
            'timeGte': timeGte,
            'timeLte': timeLte,
            'valueGte': valueGte,
            'valueLte': valueLte,
            'usdGte': usdGte,
            'usdLte': usdLte,
            'sortKey': sortKey,
            'sortDir': sortDir,
            'limit': limit,
            'offset': offset
        }
        
        return self._make_request('/transfers', params)
    
    def get_all_transfers(
        self,
        chains: str = 'solana',
        tokens: Optional[str] = None,
        timeLast: str = '30d',
        usdGte: Optional[float] = None,
        max_results: int = 5000,
        **kwargs
    ) -> List[Dict]:
        """
        Fetch all transfers with automatic pagination
        
        Args:
            chains: Blockchain network
            tokens: Token address
            timeLast: Time range (e.g., "7d", "30d")
            usdGte: Minimum USD value filter
            max_results: Maximum total results to fetch
            **kwargs: Additional parameters for get_transfers()
            
        Returns:
            List of all transfer records
        """
        
        all_transfers = []
        offset = 0
        limit = 100
        
        print(f"\n{'='*70}")
        print(f"COLLECTING TRANSFER DATA")
        print(f"{'='*70}")
        print(f"Token: {tokens[:10] if tokens else 'N/A'}...")
        print(f"Chain: {chains}")
        print(f"Time Range: {timeLast}")
        if usdGte:
            print(f"Min USD: ${usdGte:,.2f}")
        print(f"{'='*70}\n")
        
        batch_num = 1
        
        while len(all_transfers) < max_results:
            print(f"Batch #{batch_num} | Offset: {offset} | ", end='')
            
            result = self.get_transfers(
                chains=chains,
                tokens=tokens,
                timeLast=timeLast,
                usdGte=usdGte,
                limit=limit,
                offset=offset,
                sortKey='time',
                sortDir='desc',
                **kwargs
            )
            
            if not result:
                print("✗ API request failed")
                break
            
            transfers = result.get('transfers', [])
            count = result.get('count', 0)
            
            if not transfers:
                print("✓ No more transfers (empty batch)")
                break
            
            all_transfers.extend(transfers)
            print(f"Collected: {len(transfers)} | Total: {len(all_transfers)}")
            
            # Check if we reached the last page
            if len(transfers) < limit:
                print(f"\n✓ Reached last page (got {len(transfers)} < {limit})")
                break
            
            offset += limit
            batch_num += 1
        
        print(f"\n{'='*70}")
        print(f"✓ COLLECTION COMPLETE")
        print(f"  Total Transfers: {len(all_transfers)}")
        if all_transfers:
            total_usd = sum(t.get('historicalUSD', 0) or 0 for t in all_transfers)
            print(f"  Total USD Volume: ${total_usd:,.2f}")
        print(f"{'='*70}\n")
        
        return all_transfers
    
    def save_raw_data(self, transfers: List[Dict], filename: str):
        """Save raw transfer data to JSON"""
        filepath = f"data/raw/{filename}"
        
        # Add metadata
        data = {
            'metadata': {
                'collected_at': datetime.now().isoformat(),
                'total_transfers': len(transfers),
                'source': 'Arkham Intelligence API',
                'api_version': '1.0.0'
            },
            'transfers': transfers
        }
        
        os.makedirs('data/raw', exist_ok=True)
        
        with open(filepath, 'w') as f:
            json.dump(data, f, indent=2)
        
        print(f"✓ Raw data saved: {filepath}")
        print(f"  File size: {os.path.getsize(filepath) / 1024:.2f} KB")


# Test functionality
if __name__ == "__main__":
    client = ArkhamAPIClient()
    
    # Test single request
    print("\nTesting single transfer request...")
    result = client.get_transfers(
        chains='solana',
        tokens=os.getenv('TARGET_TOKEN'),
        timeLast='7d',
        limit=10
    )
    
    if result:
        print(f"✓ Success! Got {len(result.get('transfers', []))} transfers")
        print(f"  Total count: {result.get('count', 0)}")
