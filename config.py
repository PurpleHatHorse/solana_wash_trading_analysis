"""
Configuration Management
Loads settings from .env file with validation
"""

import os
from typing import List, Optional
from pathlib import Path
from dotenv import load_dotenv

# LOAD DOTENV IMMEDIATELY
# This ensures variables are available when the class attributes are defined
load_dotenv(override=True)

class Config:
    """Application configuration from .env file"""
    
    # API Configuration
    ARKHAM_API_KEY: str = os.getenv('ARKHAM_API_KEY', '')
    ARKHAM_BASE_URL: str = os.getenv('ARKHAM_BASE_URL', 'https://api.arkm.com')
    
    # Token Configuration
    # Handle string splitting safely
    _tokens_str = os.getenv('TOKENS', 'WIF')
    TOKENS: List[str] = [t.strip() for t in _tokens_str.split(',') if t.strip()]
    
    CHAIN: str = os.getenv('CHAIN', 'solana').lower()
    TIME_WINDOW: str = os.getenv('TIME_WINDOW', '7d')
    
    # Analysis Settings
    MIN_TRANSACTIONS: int = int(os.getenv('MIN_TRANSACTIONS', '5'))
    
    # Handle Sample Size (Optional Int)
    SAMPLE_SIZE: Optional[int] = None
    _sample = os.getenv('SAMPLE_SIZE', '').strip()
    if _sample and _sample.lower() != 'none':
        SAMPLE_SIZE = int(_sample)
    
    QUICK_MODE: bool = os.getenv('QUICK_MODE', 'false').lower() == 'true'
    
    # API Endpoints
    API_ENDPOINTS: Optional[List[str]] = None
    _endpoints = os.getenv('API_ENDPOINTS', '').strip()
    if _endpoints:
        API_ENDPOINTS = [e.strip() for e in _endpoints.split(',')]
    
    # Smart endpoint selection
    if not API_ENDPOINTS:
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
    
    # Endpoint Information (Static)
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
        if not cls.TOKENS:
            errors.append("At least one token must be specified in TOKENS")
        
        if cls.API_ENDPOINTS:
            invalid_endpoints = [ep for ep in cls.API_ENDPOINTS if ep not in cls.ENDPOINT_INFO]
            if invalid_endpoints:
                errors.append(f"Invalid endpoints: {', '.join(invalid_endpoints)}")
        
        if errors:
            print("❌ Configuration Errors:")
            for error in errors:
                print(f"  • {error}")
            return False
        return True

    @classmethod
    def print_config(cls):
        """Print current configuration"""
        print("="*70)
        print("CONFIGURATION LOADED")
        print(f"Chain: {cls.CHAIN} | Window: {cls.TIME_WINDOW}")
        print(f"Endpoints: {', '.join(cls.API_ENDPOINTS)}")
        print("="*70)

# Export singleton instance
config = Config()
