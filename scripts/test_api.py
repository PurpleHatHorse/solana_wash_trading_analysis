"""
Test Arkham API Access Levels
"""

import os
import requests
from dotenv import load_dotenv

load_dotenv()

def test_api_access():
    """Test different API endpoints to check access level"""
    
    api_key = os.getenv('ARKHAM_API_KEY')
    base_url = 'https://api.arkm.com'
    
    headers = {'API-Key': api_key}
    
    print("="*70)
    print("ARKHAM API ACCESS TEST")
    print("="*70)
    print(f"\nBase URL: {base_url}")
    print(f"API Key: {api_key[:10]}...{api_key[-4:]}\n")
    print("="*70)
    
    # Test 1: Health check (no auth required)
    print("\n[Test 1] Health Check (Public)")
    try:
        response = requests.get(f"{base_url}/health", timeout=10)
        print(f"Status: {response.status_code}")
        if response.status_code == 200:
            print("✓ Server is online")
        else:
            print(f"✗ Unexpected response: {response.text}")
    except Exception as e:
        print(f"✗ Error: {e}")
    
    # Test 2: Get chains (basic endpoint)
    print("\n[Test 2] Get Chains (Basic Auth)")
    try:
        response = requests.get(f"{base_url}/chains", headers=headers, timeout=10)
        print(f"Status: {response.status_code}")
        
        if response.status_code == 200:
            chains = response.json()
            print(f"✓ API key is valid!")
            print(f"Supported chains: {', '.join(chains[:5])}...")
        elif response.status_code == 401:
            print("✗ 401 Unauthorized - API key is invalid")
            return False
        else:
            print(f"⚠ Status {response.status_code}: {response.text[:200]}")
    except Exception as e:
        print(f"✗ Error: {e}")
        return False
    
    # Test 3: Intelligence endpoint (standard endpoint)
    print("\n[Test 3] Intelligence Endpoint (Standard Auth)")
    try:
        response = requests.get(
            f"{base_url}/intelligence/address/0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045",
            headers=headers,
            timeout=10
        )
        print(f"Status: {response.status_code}")
        
        if response.status_code == 200:
            print("✓ Access to intelligence endpoint confirmed")
        elif response.status_code == 401:
            print("✗ 401 Unauthorized")
        else:
            print(f"⚠ Status {response.status_code}")
    except Exception as e:
        print(f"✗ Error: {e}")
    
    # Test 4: Transfers endpoint (HEAVY endpoint - restricted)
    print("\n[Test 4] Transfers Endpoint (Heavy/Restricted)")
    print("Note: This endpoint requires special API tier access")
    try:
        response = requests.get(
            f"{base_url}/transfers",
            headers=headers,
            params={
                'chains': 'ethereum',
                'timeLast': '24h',
                'limit': 1
            },
            timeout=10
        )
        print(f"Status: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            print("✓ SUCCESS! You have access to /transfers endpoint!")
            print(f"Transfer count: {data.get('count', 0)}")
            return True
        elif response.status_code == 401:
            print("✗ 401 Unauthorized")
            print("\n  Possible reasons:")
            print("  1. Your API key doesn't have access to heavy endpoints")
            print("  2. You need to upgrade your API tier")
            print("  3. You need Arkham Exchange account access")
            print("\n  Solutions:")
            print("  • Apply for API platform access: https://info.arkm.com/api-platform")
            print("  • Sign up for Arkham Exchange: https://arkm.com")
            print("  • Request rate limit increase if you have basic access")
            return False
        elif response.status_code == 403:
            print("✗ 403 Forbidden")
            print("  Your API tier doesn't include access to this endpoint")
            return False
        else:
            print(f"⚠ Status {response.status_code}: {response.text[:200]}")
            return False
    except Exception as e:
        print(f"✗ Error: {e}")
        return False

if __name__ == "__main__":
    success = test_api_access()
    
    print("\n" + "="*70)
    if success:
        print("✓ ALL TESTS PASSED")
        print("You can proceed with data collection!")
    else:
        print("✗ TRANSFERS ENDPOINT NOT ACCESSIBLE")
        print("\nYour API key works for basic endpoints but not /transfers")
        print("You need to upgrade your API access to use this endpoint")
    print("="*70 + "\n")
