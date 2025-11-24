"""
Test API Key Connection
"""

import sys
from pathlib import Path
import os

# Add root to path
ROOT_DIR = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT_DIR))
sys.path.insert(0, str(ROOT_DIR / 'scripts'))

from dotenv import load_dotenv
import requests

# Load .env
load_dotenv()

# Get API key
api_key = os.getenv('ARKHAM_API_KEY')
base_url = os.getenv('ARKHAM_BASE_URL', 'https://api.arkm.com')

print("="*70)
print("API KEY VALIDATION TEST")
print("="*70 + "\n")

# Check if API key exists
if not api_key:
    print("❌ ARKHAM_API_KEY not found in .env file!")
    print("\nPlease check:")
    print("  1. .env file exists in root directory")
    print("  2. .env contains: ARKHAM_API_KEY=your_key_here")
    print("  3. No extra spaces around the = sign")
    sys.exit(1)

print(f"✓ API Key found in .env")
print(f"  Length: {len(api_key)} characters")
print(f"  First 8 chars: {api_key[:8]}...")
print(f"  Last 4 chars: ...{api_key[-4:]}")
print(f"\n✓ Base URL: {base_url}\n")

# Test 1: Health check
print("-"*70)
print("TEST 1: Health Check Endpoint")
print("-"*70)

try:
    response = requests.get(f"{base_url}/health", timeout=10)
    print(f"Status: {response.status_code}")
    print(f"Response: {response.text}")
    
    if response.status_code == 200:
        print("✓ API server is reachable\n")
    else:
        print("⚠ Unexpected status code\n")
except Exception as e:
    print(f"✗ Connection failed: {e}\n")

# Test 2: Authenticated endpoint
print("-"*70)
print("TEST 2: Authenticated Request (Chains List)")
print("-"*70)

headers = {"API-Key": api_key}

try:
    response = requests.get(
        f"{base_url}/chains",
        headers=headers,
        timeout=10
    )
    
    print(f"Status: {response.status_code}")
    
    if response.status_code == 200:
        print("✓ Authentication successful!")
        data = response.json()
        print(f"✓ Response: {data}")
    elif response.status_code == 401:
        print("✗ Authentication FAILED")
        print(f"Response: {response.text}")
        print("\nPossible issues:")
        print("  • API key is invalid or expired")
        print("  • API key has extra spaces/newlines")
        print("  • API key not properly formatted in .env")
        print("\nTo fix:")
        print("  1. Get new API key from: https://info.arkm.com/api-platform")
        print("  2. Update .env: ARKHAM_API_KEY=your_new_key")
        print("  3. Make sure no spaces: ARKHAM_API_KEY=abc123 (NOT 'abc123')")
    else:
        print(f"⚠ Unexpected status: {response.status_code}")
        print(f"Response: {response.text}")
        
except Exception as e:
    print(f"✗ Request failed: {e}")

# Test 3: Try a simple transfer query
print("\n" + "-"*70)
print("TEST 3: Transfer Query (Ethereum USDT)")
print("-"*70)

try:
    response = requests.get(
        f"{base_url}/transfers",
        headers=headers,
        params={
            'tokens': 'tether',
            'chains': 'ethereum',
            'timeLast': '1h',
            'limit': 1
        },
        timeout=10
    )
    
    print(f"Status: {response.status_code}")
    
    if response.status_code == 200:
        print("✓ Transfer query successful!")
        data = response.json()
        if 'transfers' in data:
            print(f"✓ Got {len(data['transfers'])} transfers")
    elif response.status_code == 401:
        print("✗ Authentication failed on transfer query")
    else:
        print(f"Status: {response.status_code}")
        print(f"Response: {response.text[:200]}")
        
except Exception as e:
    print(f"✗ Request failed: {e}")

print("\n" + "="*70)
print("TEST COMPLETE")
print("="*70)
