#!/usr/bin/env python3
"""
Quick verification script to check database contents
"""

import sqlite3

def verify_database():
    conn = sqlite3.connect('ecommerce.db')
    cursor = conn.cursor()
    
    print("🔍 Database Verification")
    print("=" * 30)
    
    # Check table counts
    cursor.execute("SELECT COUNT(*) FROM users")
    user_count = cursor.fetchone()[0]
    print(f"📊 Users: {user_count:,} records")
    
    cursor.execute("SELECT COUNT(*) FROM orders")
    order_count = cursor.fetchone()[0]
    print(f"📊 Orders: {order_count:,} records")
    
    # Check foreign key relationships
    cursor.execute("""
        SELECT COUNT(*) FROM orders o 
        JOIN users u ON o.user_id = u.id
    """)
    valid_orders = cursor.fetchone()[0]
    print(f"🔗 Valid order-user relationships: {valid_orders:,}")
    
    # Show table schemas
    print("\n📋 Table Schemas:")
    cursor.execute("PRAGMA table_info(users)")
    print("Users table columns:")
    for col in cursor.fetchall():
        print(f"  - {col[1]} ({col[2]})")
    
    cursor.execute("PRAGMA table_info(orders)")
    print("\nOrders table columns:")
    for col in cursor.fetchall():
        print(f"  - {col[1]} ({col[2]})")
    
    conn.close()

if __name__ == "__main__":
    verify_database() 