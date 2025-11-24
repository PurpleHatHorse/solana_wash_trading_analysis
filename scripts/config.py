"""
Configuration Management
Loads settings from .env file with validation
"""

import os
from typing import List, Optional
from pathlib import Path

# Don't load dotenv here - let caller handle it
# This prevents double-loading issues


class Config:
    """Application configuration from .env file"""
    
    # API Configuration
    ARKHAM_API_KEY: str = os.getenv('ARKHAM_API_KEY', '')
    ARKHAM_BASE_URL: str = os.getenv('ARKHAM_BASE_URL', 'https://api.arkm.com')
    
    # Token Configuration
    TOKENS: List[str] = os.getenv('TOKENS', 'WIF').split(',')
    TOKENS = [t.strip() for t in TOKENS if t.strip()]
    
    CHAIN: str = os.getenv('CHAIN', 'solana').lower()
    TIME_WINDOW: str = os.getenv('TIME_WINDOW', '7d')
    
    # Analysis Settings
    MIN_TRANSACTIONS: int = int(os.getenv('MIN_TRANSACTIONS', '5'))
    SAMPLE_SIZE: Optional[int] = None
    _sample = os.getenv('SAMPLE_SIZE', '').strip()
    if _sample:
        SAMPLE_SIZE = int(_sample)
    
    QUICK_MODE: bool = os.getenv('QUICK_MODE', 'false').lower() == 'true'
    
    # API Endpoints
    API_ENDPOINTS: Optional[List[str]] = None
    _endpoints = os.getenv('API_ENDPOINTS', '').strip()
    if _endpoints:
        API_ENDPOINTS = [e.strip() for e in _endpoints.split(',')]
    
    # Smart endpoint selection
    if API_ENDPOINTS is None:
        if QUICK_MODE:
            API_ENDPOINTS = ['transfers', 'intelligence', 'counterparties']
        else:
            API_ENDPOINTS = ['transfers', 'counterparties', 'intelligence', 
                           'balances', 'portfolio', 'flow']
    
    # Output Settings
    OUTPUT_DIR: str = os.getenv('OUTPUT_DIR', 'outputs')
    CACHE_DIR: str = os.getenv('CACHE_DIR', 'data/cache')
    CACHE_MAX_AGE_HOURS: int = int(os.getenv('CACHE_MAX_AGE_HOURS', '24'))
    
    # Performance Settings
    MAX_WORKERS: int = int(os.getenv('MAX_WORKERS', '3'))
    ENFORCE_RATE_LIMITS: bool = os.getenv('ENFORCE_RATE_LIMITS', 'true').lower() == 'true'
    
    # Endpoint Information
    ENDPOINT_INFO = {
        'transfers': {'rate_limit': 'heavy', 'description': 'Detailed transfer history', 'required': True},
        'counterparties': {'rate_limit': 'heavy', 'description': 'Aggregated counterparty relationships'},
        'intelligence': {'rate_limit': 'standard', 'description': 'Tags, labels, entity predictions'},
        'balances': {'rate_limit': 'standard', 'description': 'Current token holdings'},
        'portfolio': {'rate_limit': 'standard', 'description': 'Historical portfolio snapshots'},
        'flow': {'rate_limit': 'standard', 'description': 'Aggregate inflow/outflow data'}
    }
    
    @classmethod
    def validate(cls) -> bool:
        """Validate configuration"""
        errors = []
        
        if not cls.ARKHAM_API_KEY:
            errors.append("ARKHAM_API_KEY is required in .env file")
        
        if not cls.ARKHAM_BASE_URL:
            errors.append("ARKHAM_BASE_URL is required in .env file")
        
        if not cls.TOKENS:
            errors.append("At least one token must be specified in TOKENS")
        
        supported_chains = ['solana', 'ethereum', 'bsc', 'polygon', 'arbitrum_one', 'base', 'optimism', 'avalanche']
        if cls.CHAIN not in supported_chains:
            errors.append(f"Unsupported chain: {cls.CHAIN}")
        
        if not cls.TIME_WINDOW or cls.TIME_WINDOW[-1] not in ['h', 'd']:
            errors.append(f"Invalid TIME_WINDOW format: {cls.TIME_WINDOW}")
        
        if cls.API_ENDPOINTS:
            invalid_endpoints = [ep for ep in cls.API_ENDPOINTS if ep not in cls.ENDPOINT_INFO]
            if invalid_endpoints:
                errors.append(f"Invalid endpoints: {', '.join(invalid_endpoints)}")
            
            if 'transfers' not in cls.API_ENDPOINTS:
                errors.append("'transfers' endpoint is required")
        
        if errors:
            print("❌ Configuration Errors:")
            for error in errors:
                print(f"  • {error}")
            return False
        
        return True
    
    @classmethod
    def estimate_time(cls, num_wallets: int) -> float:
        """Estimate analysis time"""
        calls_per_wallet = len(cls.API_ENDPOINTS)
        heavy_endpoints = sum(1 for ep in cls.API_ENDPOINTS 
                            if cls.ENDPOINT_INFO[ep]['rate_limit'] == 'heavy')
        standard_endpoints = calls_per_wallet - heavy_endpoints
        
        heavy_time = (num_wallets * heavy_endpoints)
        standard_time = (num_wallets * standard_endpoints) / 20
        
        return heavy_time + standard_time
    
    @classmethod
    def print_config(cls):
        """Print current configuration"""
        print("="*70)
        print("CONFIGURATION")
        print("="*70)
        print(f"API Key: {'✓ Set' if cls.ARKHAM_API_KEY else '✗ Missing'}")
        if cls.ARKHAM_API_KEY:
            print(f"  Length: {len(cls.ARKHAM_API_KEY)} chars")
            print(f"  Preview: {cls.ARKHAM_API_KEY[:8]}...{cls.ARKHAM_API_KEY[-4:]}")
        print(f"Base URL: {cls.ARKHAM_BASE_URL}")
        print(f"Tokens: {', '.join(cls.TOKENS)}")
        print(f"Chain: {cls.CHAIN}")
        print(f"Time Window: {cls.TIME_WINDOW}")
        print(f"Min Transactions: {cls.MIN_TRANSACTIONS}")
        print(f"Sample Size: {cls.SAMPLE_SIZE or 'All'}")
        print(f"Quick Mode: {'Yes' if cls.QUICK_MODE else 'No'}")
        print(f"\nAPI Endpoints ({len(cls.API_ENDPOINTS)}):")
        
        for endpoint in cls.API_ENDPOINTS:
            info = cls.ENDPOINT_INFO[endpoint]
            rate = "1 req/s" if info['rate_limit'] == 'heavy' else "20 req/s"
            print(f"  • {endpoint:15s} [{rate:9s}] - {info['description']}")
        
        print(f"\nMax Workers: {cls.MAX_WORKERS}")
        print(f"Cache: {cls.CACHE_DIR} (max age: {cls.CACHE_MAX_AGE_HOURS}h)")
        print(f"Output: {cls.OUTPUT_DIR}")
        print("="*70 + "\n")


# Export singleton instance
config = Config()