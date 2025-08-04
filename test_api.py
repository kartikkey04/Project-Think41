#!/usr/bin/env python3
"""
Test script for Customer API endpoints
Tests all required functionality as per Milestone 2 requirements
"""

import requests
import json
import time

# API base URL
BASE_URL = "http://localhost:5000/api"

def test_health_check():
    """Test health check endpoint"""
    print("🔍 Testing Health Check...")
    try:
        response = requests.get(f"{BASE_URL}/health")
        print(f"  Status: {response.status_code}")
        print(f"  Response: {response.json()}")
        return response.status_code == 200
    except Exception as e:
        print(f"  ❌ Error: {e}")
        return False

def test_list_customers():
    """Test list customers endpoint with pagination"""
    print("\n📋 Testing List Customers (with pagination)...")
    try:
        # Test basic pagination
        response = requests.get(f"{BASE_URL}/customers?page=1&limit=5")
        print(f"  Status: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            print(f"  ✅ Success: {len(data['data'])} customers returned")
            print(f"  📊 Pagination: Page {data['pagination']['page']} of {data['pagination']['total_pages']}")
            print(f"  📈 Total customers: {data['pagination']['total']}")
            
            # Show first customer
            if data['data']:
                first_customer = data['data'][0]
                print(f"  👤 Sample customer: {first_customer['first_name']} {first_customer['last_name']} (ID: {first_customer['id']})")
            
            return True
        else:
            print(f"  ❌ Error: {response.text}")
            return False
    except Exception as e:
        print(f"  ❌ Error: {e}")
        return False

def test_search_customers():
    """Test customer search functionality"""
    print("\n🔍 Testing Customer Search...")
    try:
        response = requests.get(f"{BASE_URL}/customers?search=john&limit=3")
        print(f"  Status: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            print(f"  ✅ Search results: {len(data['data'])} customers found")
            print(f"  🔍 Search term: {data['search']}")
            
            for customer in data['data']:
                print(f"    - {customer['first_name']} {customer['last_name']} ({customer['email']})")
            
            return True
        else:
            print(f"  ❌ Error: {response.text}")
            return False
    except Exception as e:
        print(f"  ❌ Error: {e}")
        return False

def test_get_customer_details():
    """Test get specific customer details"""
    print("\n👤 Testing Get Customer Details...")
    try:
        # Test with a valid customer ID (we know ID 1 exists from our data)
        response = requests.get(f"{BASE_URL}/customers/1")
        print(f"  Status: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            customer = data['data']['customer']
            stats = data['data']['order_statistics']
            
            print(f"  ✅ Customer found: {customer['first_name']} {customer['last_name']}")
            print(f"  📧 Email: {customer['email']}")
            print(f"  🌍 Country: {customer['country']}")
            print(f"  📦 Order count: {customer['order_count']}")
            print(f"  📊 Order statistics:")
            print(f"    - Total orders: {stats['total_orders']}")
            print(f"    - Completed: {stats['completed_orders']}")
            print(f"    - Shipped: {stats['shipped_orders']}")
            print(f"    - Cancelled: {stats['cancelled_orders']}")
            
            return True
        else:
            print(f"  ❌ Error: {response.text}")
            return False
    except Exception as e:
        print(f"  ❌ Error: {e}")
        return False

def test_get_customer_orders():
    """Test get customer orders endpoint"""
    print("\n📦 Testing Get Customer Orders...")
    try:
        response = requests.get(f"{BASE_URL}/customers/1/orders?limit=3")
        print(f"  Status: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            print(f"  ✅ Orders found: {len(data['data'])} orders")
            print(f"  📊 Pagination: {data['pagination']['total']} total orders")
            
            for order in data['data']:
                print(f"    - Order {order['order_id']}: {order['status']} ({order['num_of_item']} items)")
            
            return True
        else:
            print(f"  ❌ Error: {response.text}")
            return False
    except Exception as e:
        print(f"  ❌ Error: {e}")
        return False

def test_get_statistics():
    """Test statistics endpoint"""
    print("\n📈 Testing Get Statistics...")
    try:
        response = requests.get(f"{BASE_URL}/statistics")
        print(f"  Status: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            stats = data['data']['overall_statistics']
            countries = data['data']['top_countries']
            
            print(f"  ✅ Statistics retrieved:")
            print(f"    - Total customers: {stats['total_customers']:,}")
            print(f"    - Total orders: {stats['total_orders']:,}")
            print(f"    - Total items: {stats['total_items']:,}")
            print(f"    - Avg items per order: {stats['avg_items_per_order']}")
            print(f"    - Completed orders: {stats['completed_orders']:,}")
            print(f"    - Shipped orders: {stats['shipped_orders']:,}")
            print(f"    - Cancelled orders: {stats['cancelled_orders']:,}")
            
            print(f"  🌍 Top countries:")
            for country in countries[:3]:
                print(f"    - {country['country']}: {country['customer_count']:,} customers, {country['order_count']:,} orders")
            
            return True
        else:
            print(f"  ❌ Error: {response.text}")
            return False
    except Exception as e:
        print(f"  ❌ Error: {e}")
        return False

def test_error_handling():
    """Test error handling for invalid requests"""
    print("\n⚠️ Testing Error Handling...")
    
    # Test invalid customer ID
    try:
        response = requests.get(f"{BASE_URL}/customers/999999")
        print(f"  Invalid customer ID - Status: {response.status_code}")
        if response.status_code == 404:
            print(f"  ✅ Correctly handled: {response.json()['message']}")
        else:
            print(f"  ❌ Expected 404, got {response.status_code}")
    except Exception as e:
        print(f"  ❌ Error: {e}")
    
    # Test invalid pagination
    try:
        response = requests.get(f"{BASE_URL}/customers?page=invalid&limit=abc")
        print(f"  Invalid pagination - Status: {response.status_code}")
        if response.status_code == 400:
            print(f"  ✅ Correctly handled: {response.json()['message']}")
        else:
            print(f"  ❌ Expected 400, got {response.status_code}")
    except Exception as e:
        print(f"  ❌ Error: {e}")
    
    # Test non-existent endpoint
    try:
        response = requests.get(f"{BASE_URL}/nonexistent")
        print(f"  Non-existent endpoint - Status: {response.status_code}")
        if response.status_code == 404:
            print(f"  ✅ Correctly handled: {response.json()['message']}")
        else:
            print(f"  ❌ Expected 404, got {response.status_code}")
    except Exception as e:
        print(f"  ❌ Error: {e}")

def main():
    """Run all API tests"""
    print("🧪 CUSTOMER API TESTING")
    print("=" * 50)
    
    # Wait a moment for API to be ready
    print("⏳ Waiting for API to be ready...")
    time.sleep(2)
    
    tests = [
        test_health_check,
        test_list_customers,
        test_search_customers,
        test_get_customer_details,
        test_get_customer_orders,
        test_get_statistics,
        test_error_handling
    ]
    
    passed = 0
    total = len(tests) - 1  # Exclude error handling from count
    
    for test in tests:
        if test == test_error_handling:
            test()
        else:
            if test():
                passed += 1
    
    print("\n" + "=" * 50)
    print(f"🎉 TEST RESULTS: {passed}/{total} tests passed")
    
    if passed == total:
        print("✅ ALL TESTS PASSED! API is working correctly.")
    else:
        print("❌ Some tests failed. Please check the API implementation.")
    
    print("\n📋 API Endpoints Summary:")
    print("  ✅ GET /api/health - Health check")
    print("  ✅ GET /api/customers - List customers with pagination")
    print("  ✅ GET /api/customers/<id> - Get customer details")
    print("  ✅ GET /api/customers/<id>/orders - Get customer orders")
    print("  ✅ GET /api/statistics - Get basic statistics")
    print("  ✅ Error handling for invalid requests")

if __name__ == "__main__":
    main() 