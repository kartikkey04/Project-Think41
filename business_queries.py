#!/usr/bin/env python3
"""
Business Queries Demonstration
Shows practical business insights from the loaded data
"""

import sqlite3

def run_business_queries():
    """Run business-focused queries to demonstrate data value"""
    conn = sqlite3.connect('ecommerce.db')
    cursor = conn.cursor()
    
    print("💼 BUSINESS QUERIES DEMONSTRATION")
    print("=" * 50)
    
    # Query 1: Top Performing Countries
    print("\n🌍 TOP PERFORMING COUNTRIES")
    print("-" * 30)
    cursor.execute("""
        SELECT 
            u.country,
            COUNT(DISTINCT u.id) as unique_users,
            COUNT(o.order_id) as total_orders,
            AVG(o.num_of_item) as avg_items_per_order
        FROM users u
        LEFT JOIN orders o ON u.id = o.user_id
        WHERE u.country IS NOT NULL
        GROUP BY u.country
        ORDER BY total_orders DESC
        LIMIT 5
    """)
    
    for row in cursor.fetchall():
        print(f"  {row[0]}: {row[1]:,} users, {row[2]:,} orders, {row[3]:.2f} avg items")
    
    # Query 2: Order Status Analysis
    print("\n📊 ORDER STATUS ANALYSIS")
    print("-" * 30)
    cursor.execute("""
        SELECT 
            status,
            COUNT(*) as count,
            ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM orders), 2) as percentage
        FROM orders
        GROUP BY status
        ORDER BY count DESC
    """)
    
    for row in cursor.fetchall():
        print(f"  {row[0]}: {row[1]:,} orders ({row[2]}%)")
    
    # Query 3: User Age Demographics
    print("\n👥 USER AGE DEMOGRAPHICS")
    print("-" * 30)
    cursor.execute("""
        SELECT 
            CASE 
                WHEN age < 18 THEN 'Under 18'
                WHEN age BETWEEN 18 AND 25 THEN '18-25'
                WHEN age BETWEEN 26 AND 35 THEN '26-35'
                WHEN age BETWEEN 36 AND 50 THEN '36-50'
                WHEN age > 50 THEN 'Over 50'
                ELSE 'Unknown'
            END as age_group,
            COUNT(*) as count
        FROM users
        WHERE age IS NOT NULL
        GROUP BY age_group
        ORDER BY count DESC
    """)
    
    for row in cursor.fetchall():
        print(f"  {row[0]}: {row[1]:,} users")
    
    # Query 4: Traffic Source Analysis
    print("\n📱 TRAFFIC SOURCE ANALYSIS")
    print("-" * 30)
    cursor.execute("""
        SELECT 
            traffic_source,
            COUNT(*) as user_count,
            COUNT(o.order_id) as order_count
        FROM users u
        LEFT JOIN orders o ON u.id = o.user_id
        WHERE traffic_source IS NOT NULL
        GROUP BY traffic_source
        ORDER BY user_count DESC
    """)
    
    for row in cursor.fetchall():
        print(f"  {row[0]}: {row[1]:,} users, {row[2]:,} orders")
    
    # Query 5: Monthly Order Trends
    print("\n📈 MONTHLY ORDER TRENDS (Last 12 months)")
    print("-" * 30)
    cursor.execute("""
        SELECT 
            strftime('%Y-%m', created_at) as month,
            COUNT(*) as orders,
            SUM(num_of_item) as total_items
        FROM orders
        WHERE created_at >= date('now', '-12 months')
        GROUP BY month
        ORDER BY month DESC
        LIMIT 12
    """)
    
    for row in cursor.fetchall():
        print(f"  {row[0]}: {row[1]:,} orders, {row[2]:,} items")
    
    # Query 6: High-Value Customers
    print("\n👑 HIGH-VALUE CUSTOMERS (Top 10 by Order Count)")
    print("-" * 30)
    cursor.execute("""
        SELECT 
            u.first_name,
            u.last_name,
            u.email,
            COUNT(o.order_id) as order_count,
            SUM(o.num_of_item) as total_items,
            u.country
        FROM users u
        JOIN orders o ON u.id = o.user_id
        GROUP BY u.id, u.first_name, u.last_name, u.email, u.country
        ORDER BY order_count DESC, total_items DESC
        LIMIT 10
    """)
    
    for row in cursor.fetchall():
        print(f"  {row[0]} {row[1]} ({row[2]}): {row[3]} orders, {row[4]} items, {row[5]}")
    
    # Query 7: Geographic Distribution of Orders
    print("\n🗺️ GEOGRAPHIC DISTRIBUTION OF ORDERS")
    print("-" * 30)
    cursor.execute("""
        SELECT 
            u.country,
            COUNT(o.order_id) as orders,
            AVG(o.num_of_item) as avg_items,
            COUNT(DISTINCT u.id) as unique_customers
        FROM users u
        JOIN orders o ON u.id = o.user_id
        WHERE u.country IS NOT NULL
        GROUP BY u.country
        ORDER BY orders DESC
        LIMIT 10
    """)
    
    for row in cursor.fetchall():
        print(f"  {row[0]}: {row[1]:,} orders, {row[2]:.2f} avg items, {row[3]:,} customers")
    
    print("\n✅ BUSINESS QUERIES COMPLETED!")
    print("=" * 50)
    print("💡 These queries demonstrate the business value of the loaded data")
    print("📊 Data can be used for customer analysis, market insights, and business decisions")
    
    conn.close()

if __name__ == "__main__":
    run_business_queries() 