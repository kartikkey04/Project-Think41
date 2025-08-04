#!/usr/bin/env python3
"""
Milestone 1: Database Design and Loading Customer & Order Data
Requirements:
1. Download dataset from https://github.com/recruit41/ecommerce-dataset
2. Extract users.csv and orders.csv from archive.zip
3. Design and create database tables for users and orders
4. Write code to load both CSV files into your database
5. Verify the data was loaded correctly by querying both tables
"""

import sqlite3
import csv
import os

def create_database():
    """Step 3: Design and create database tables for users and orders"""
    conn = sqlite3.connect('ecommerce.db')
    cursor = conn.cursor()
    
    # Create users table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY,
            first_name TEXT NOT NULL,
            last_name TEXT NOT NULL,
            email TEXT NOT NULL,
            age INTEGER,
            gender TEXT,
            state TEXT,
            street_address TEXT,
            postal_code TEXT,
            city TEXT,
            country TEXT,
            latitude REAL,
            longitude REAL,
            traffic_source TEXT,
            created_at TIMESTAMP
        )
    ''')
    
    # Create orders table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS orders (
            order_id INTEGER PRIMARY KEY,
            user_id INTEGER,
            status TEXT,
            gender TEXT,
            created_at TIMESTAMP,
            returned_at TIMESTAMP,
            shipped_at TIMESTAMP,
            delivered_at TIMESTAMP,
            num_of_item INTEGER,
            FOREIGN KEY (user_id) REFERENCES users (id)
        )
    ''')
    
    conn.commit()
    print("✅ Database tables created successfully!")
    return conn

def load_csv_data(conn):
    """Step 4: Write code to load both CSV files into your database"""
    csv_dir = "ecommerce-dataset-main/archive"
    cursor = conn.cursor()
    
    # Load users.csv
    users_csv = os.path.join(csv_dir, "users.csv")
    if os.path.exists(users_csv):
        print(f"📊 Loading users data from {users_csv}...")
        
        # Clear existing data
        cursor.execute("DELETE FROM users")
        
        with open(users_csv, 'r') as file:
            csv_reader = csv.DictReader(file)
            for row in csv_reader:
                try:
                    cursor.execute('''
                        INSERT INTO users (id, first_name, last_name, email, age, gender, 
                                         state, street_address, postal_code, city, country, 
                                         latitude, longitude, traffic_source, created_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        int(row['id']),
                        row['first_name'],
                        row['last_name'],
                        row['email'],
                        int(row['age']) if row['age'] else None,
                        row['gender'],
                        row['state'],
                        row['street_address'],
                        row['postal_code'],
                        row['city'],
                        row['country'],
                        float(row['latitude']) if row['latitude'] else None,
                        float(row['longitude']) if row['longitude'] else None,
                        row['traffic_source'],
                        row['created_at']
                    ))
                except sqlite3.IntegrityError:
                    # Skip duplicate entries
                    continue
        
        conn.commit()
        print(f"✅ Loaded users data successfully")
    else:
        print(f"❌ Error: {users_csv} not found!")
        return False
    
    # Load orders.csv
    orders_csv = os.path.join(csv_dir, "orders.csv")
    if os.path.exists(orders_csv):
        print(f"📊 Loading orders data from {orders_csv}...")
        
        # Clear existing data
        cursor.execute("DELETE FROM orders")
        
        with open(orders_csv, 'r') as file:
            csv_reader = csv.DictReader(file)
            for row in csv_reader:
                try:
                    cursor.execute('''
                        INSERT INTO orders (order_id, user_id, status, gender, created_at, 
                                          returned_at, shipped_at, delivered_at, num_of_item)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        int(row['order_id']),
                        int(row['user_id']),
                        row['status'],
                        row['gender'],
                        row['created_at'],
                        row['returned_at'] if row['returned_at'] else None,
                        row['shipped_at'] if row['shipped_at'] else None,
                        row['delivered_at'] if row['delivered_at'] else None,
                        int(row['num_of_item']) if row['num_of_item'] else 1
                    ))
                except sqlite3.IntegrityError:
                    # Skip duplicate entries
                    continue
        
        conn.commit()
        print(f"✅ Loaded orders data successfully")
    else:
        print(f"❌ Error: {orders_csv} not found!")
        return False
    
    return True

def verify_data(conn):
    """Step 5: Verify the data was loaded correctly by querying both tables"""
    cursor = conn.cursor()
    
    print("\n🔍 Verifying data...")
    
    # Count records in both tables
    cursor.execute("SELECT COUNT(*) FROM users")
    user_count = cursor.fetchone()[0]
    print(f"📈 Users table: {user_count} records")
    
    cursor.execute("SELECT COUNT(*) FROM orders")
    order_count = cursor.fetchone()[0]
    print(f"📈 Orders table: {order_count} records")
    
    # Show sample data from both tables
    print("\n📋 Sample Users:")
    cursor.execute("SELECT id, first_name, last_name, email FROM users LIMIT 3")
    for row in cursor.fetchall():
        print(f"  ID: {row[0]}, Name: {row[1]} {row[2]}, Email: {row[3]}")
    
    print("\n📋 Sample Orders:")
    cursor.execute("SELECT order_id, user_id, status, num_of_item FROM orders LIMIT 3")
    for row in cursor.fetchall():
        print(f"  Order ID: {row[0]}, User ID: {row[1]}, Status: {row[2]}, Items: {row[3]}")
    
    # Check foreign key relationship
    print("\n🔗 Foreign Key Relationship Check:")
    cursor.execute("""
        SELECT COUNT(*) FROM orders o 
        JOIN users u ON o.user_id = u.id
    """)
    valid_orders = cursor.fetchone()[0]
    print(f"  Orders with valid user references: {valid_orders}/{order_count}")

def main():
    """Main function to complete Milestone 1"""
    print("🚀 Milestone 1: Database Design and Loading Customer & Order Data")
    print("=" * 60)
    
    try:
        # Step 3: Create database and tables
        conn = create_database()
        
        # Step 4: Load CSV data
        if load_csv_data(conn):
            # Step 5: Verify data
            verify_data(conn)
            
            print("\n🎉 Milestone 1 completed successfully!")
            print("📊 Database: SQLite (ecommerce.db)")
            print("📁 Tables: users, orders")
        
        conn.close()
        
    except Exception as e:
        print(f"❌ Error: {str(e)}")

if __name__ == "__main__":
    main() 