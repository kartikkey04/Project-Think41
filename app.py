#!/usr/bin/env python3
"""
Milestone 2: Build Customer API
RESTful API that provides customer data and basic order statistics
"""

from flask import Flask, jsonify, request
from flask_cors import CORS
import sqlite3
from datetime import datetime

app = Flask(__name__)
CORS(app)  # Enable CORS for frontend integration

# Database configuration
DATABASE = 'ecommerce.db'

def get_db_connection():
    """Create a database connection"""
    conn = sqlite3.connect(DATABASE)
    conn.row_factory = sqlite3.Row  # This enables column access by name
    return conn

@app.route('/api/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({
        'status': 'OK',
        'message': 'Customer API is running',
        'timestamp': datetime.now().isoformat()
    })

@app.route('/api/customers', methods=['GET'])
def list_customers():
    """
    List all customers with pagination
    Query parameters:
    - page: page number (default: 1)
    - limit: items per page (default: 10, max: 100)
    - search: search by name or email (optional)
    """
    try:
        # Get query parameters
        page = max(1, int(request.args.get('page', 1)))
        limit = min(100, max(1, int(request.args.get('limit', 10))))
        search = request.args.get('search', '').strip()
        
        # Calculate offset
        offset = (page - 1) * limit
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Build query with optional search
        if search:
            query = """
                SELECT 
                    u.id, u.first_name, u.last_name, u.email, u.age, u.gender,
                    u.country, u.city, u.created_at,
                    COUNT(o.order_id) as order_count
                FROM users u
                LEFT JOIN orders o ON u.id = o.user_id
                WHERE (u.first_name LIKE ? OR u.last_name LIKE ? OR u.email LIKE ?)
                GROUP BY u.id, u.first_name, u.last_name, u.email, u.age, u.gender, u.country, u.city, u.created_at
                ORDER BY u.created_at DESC
                LIMIT ? OFFSET ?
            """
            search_param = f'%{search}%'
            cursor.execute(query, (search_param, search_param, search_param, limit, offset))
        else:
            query = """
                SELECT 
                    u.id, u.first_name, u.last_name, u.email, u.age, u.gender,
                    u.country, u.city, u.created_at,
                    COUNT(o.order_id) as order_count
                FROM users u
                LEFT JOIN orders o ON u.id = o.user_id
                GROUP BY u.id, u.first_name, u.last_name, u.email, u.age, u.gender, u.country, u.city, u.created_at
                ORDER BY u.created_at DESC
                LIMIT ? OFFSET ?
            """
            cursor.execute(query, (limit, offset))
        
        customers = []
        for row in cursor.fetchall():
            customer = dict(row)
            # Convert datetime to string for JSON serialization
            if customer['created_at']:
                customer['created_at'] = customer['created_at']
            customers.append(customer)
        
        # Get total count for pagination
        if search:
            count_query = """
                SELECT COUNT(DISTINCT u.id) 
                FROM users u 
                WHERE (u.first_name LIKE ? OR u.last_name LIKE ? OR u.email LIKE ?)
            """
            cursor.execute(count_query, (search_param, search_param, search_param))
        else:
            cursor.execute("SELECT COUNT(*) FROM users")
        
        total_count = cursor.fetchone()[0]
        total_pages = (total_count + limit - 1) // limit
        
        conn.close()
        
        return jsonify({
            'success': True,
            'data': customers,
            'pagination': {
                'page': page,
                'limit': limit,
                'total': total_count,
                'total_pages': total_pages,
                'has_next': page < total_pages,
                'has_prev': page > 1
            },
            'search': search if search else None
        }), 200
        
    except ValueError as e:
        return jsonify({
            'success': False,
            'error': 'Invalid pagination parameters',
            'message': str(e)
        }), 400
    except Exception as e:
        return jsonify({
            'success': False,
            'error': 'Internal server error',
            'message': str(e)
        }), 500

@app.route('/api/customers/<int:customer_id>', methods=['GET'])
def get_customer_details(customer_id):
    """
    Get specific customer details including order count
    Path parameter: customer_id (integer)
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get customer details with order count
        query = """
            SELECT 
                u.id, u.first_name, u.last_name, u.email, u.age, u.gender,
                u.state, u.street_address, u.postal_code, u.city, u.country,
                u.latitude, u.longitude, u.traffic_source, u.created_at,
                COUNT(o.order_id) as order_count,
                SUM(o.num_of_item) as total_items_ordered
            FROM users u
            LEFT JOIN orders o ON u.id = o.user_id
            WHERE u.id = ?
            GROUP BY u.id, u.first_name, u.last_name, u.email, u.age, u.gender,
                     u.state, u.street_address, u.postal_code, u.city, u.country,
                     u.latitude, u.longitude, u.traffic_source, u.created_at
        """
        
        cursor.execute(query, (customer_id,))
        customer = cursor.fetchone()
        
        if not customer:
            conn.close()
            return jsonify({
                'success': False,
                'error': 'Customer not found',
                'message': f'Customer with ID {customer_id} does not exist'
            }), 404
        
        # Convert to dictionary
        customer_data = dict(customer)
        
        # Get recent orders for this customer
        orders_query = """
            SELECT 
                order_id, status, num_of_item, created_at, 
                shipped_at, delivered_at
            FROM orders 
            WHERE user_id = ? 
            ORDER BY created_at DESC 
            LIMIT 5
        """
        
        cursor.execute(orders_query, (customer_id,))
        recent_orders = []
        for row in cursor.fetchall():
            order = dict(row)
            recent_orders.append(order)
        
        # Get order statistics
        stats_query = """
            SELECT 
                COUNT(*) as total_orders,
                SUM(num_of_item) as total_items,
                AVG(num_of_item) as avg_items_per_order,
                COUNT(CASE WHEN status = 'Complete' THEN 1 END) as completed_orders,
                COUNT(CASE WHEN status = 'Shipped' THEN 1 END) as shipped_orders,
                COUNT(CASE WHEN status = 'Cancelled' THEN 1 END) as cancelled_orders
            FROM orders 
            WHERE user_id = ?
        """
        
        cursor.execute(stats_query, (customer_id,))
        stats = dict(cursor.fetchone())
        
        conn.close()
        
        # Prepare response
        response_data = {
            'customer': customer_data,
            'recent_orders': recent_orders,
            'order_statistics': {
                'total_orders': stats['total_orders'],
                'total_items': stats['total_items'] or 0,
                'avg_items_per_order': round(stats['avg_items_per_order'] or 0, 2),
                'completed_orders': stats['completed_orders'],
                'shipped_orders': stats['shipped_orders'],
                'cancelled_orders': stats['cancelled_orders']
            }
        }
        
        return jsonify({
            'success': True,
            'data': response_data
        }), 200
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': 'Internal server error',
            'message': str(e)
        }), 500

@app.route('/api/customers/<int:customer_id>/orders', methods=['GET'])
def get_customer_orders(customer_id):
    """
    Get all orders for a specific customer with pagination
    Path parameter: customer_id (integer)
    Query parameters: page, limit
    """
    try:
        # Get query parameters
        page = max(1, int(request.args.get('page', 1)))
        limit = min(100, max(1, int(request.args.get('limit', 10))))
        offset = (page - 1) * limit
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # First check if customer exists
        cursor.execute("SELECT id FROM users WHERE id = ?", (customer_id,))
        if not cursor.fetchone():
            conn.close()
            return jsonify({
                'success': False,
                'error': 'Customer not found',
                'message': f'Customer with ID {customer_id} does not exist'
            }), 404
        
        # Get orders for this customer
        query = """
            SELECT 
                order_id, status, num_of_item, created_at, 
                shipped_at, delivered_at, returned_at
            FROM orders 
            WHERE user_id = ? 
            ORDER BY created_at DESC 
            LIMIT ? OFFSET ?
        """
        
        cursor.execute(query, (customer_id, limit, offset))
        orders = []
        for row in cursor.fetchall():
            order = dict(row)
            orders.append(order)
        
        # Get total count for pagination
        cursor.execute("SELECT COUNT(*) FROM orders WHERE user_id = ?", (customer_id,))
        total_count = cursor.fetchone()[0]
        total_pages = (total_count + limit - 1) // limit
        
        conn.close()
        
        return jsonify({
            'success': True,
            'data': orders,
            'pagination': {
                'page': page,
                'limit': limit,
                'total': total_count,
                'total_pages': total_pages,
                'has_next': page < total_pages,
                'has_prev': page > 1
            }
        }), 200
        
    except ValueError as e:
        return jsonify({
            'success': False,
            'error': 'Invalid pagination parameters',
            'message': str(e)
        }), 400
    except Exception as e:
        return jsonify({
            'success': False,
            'error': 'Internal server error',
            'message': str(e)
        }), 500

@app.route('/api/statistics', methods=['GET'])
def get_statistics():
    """
    Get basic order statistics
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get overall statistics
        stats_query = """
            SELECT 
                COUNT(DISTINCT u.id) as total_customers,
                COUNT(o.order_id) as total_orders,
                SUM(o.num_of_item) as total_items,
                AVG(o.num_of_item) as avg_items_per_order,
                COUNT(CASE WHEN o.status = 'Complete' THEN 1 END) as completed_orders,
                COUNT(CASE WHEN o.status = 'Shipped' THEN 1 END) as shipped_orders,
                COUNT(CASE WHEN o.status = 'Cancelled' THEN 1 END) as cancelled_orders
            FROM users u
            LEFT JOIN orders o ON u.id = o.user_id
        """
        
        cursor.execute(stats_query)
        stats = dict(cursor.fetchone())
        
        # Get top countries
        countries_query = """
            SELECT 
                u.country,
                COUNT(DISTINCT u.id) as customer_count,
                COUNT(o.order_id) as order_count
            FROM users u
            LEFT JOIN orders o ON u.id = o.user_id
            WHERE u.country IS NOT NULL
            GROUP BY u.country
            ORDER BY order_count DESC
            LIMIT 5
        """
        
        cursor.execute(countries_query)
        top_countries = []
        for row in cursor.fetchall():
            top_countries.append(dict(row))
        
        conn.close()
        
        return jsonify({
            'success': True,
            'data': {
                'overall_statistics': {
                    'total_customers': stats['total_customers'],
                    'total_orders': stats['total_orders'],
                    'total_items': stats['total_items'] or 0,
                    'avg_items_per_order': round(stats['avg_items_per_order'] or 0, 2),
                    'completed_orders': stats['completed_orders'],
                    'shipped_orders': stats['shipped_orders'],
                    'cancelled_orders': stats['cancelled_orders']
                },
                'top_countries': top_countries
            }
        }), 200
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': 'Internal server error',
            'message': str(e)
        }), 500

# Error handlers
@app.errorhandler(404)
def not_found(error):
    return jsonify({
        'success': False,
        'error': 'Not found',
        'message': 'The requested resource was not found'
    }), 404

@app.errorhandler(500)
def internal_error(error):
    return jsonify({
        'success': False,
        'error': 'Internal server error',
        'message': 'An unexpected error occurred'
    }), 500

if __name__ == '__main__':
    print("🚀 Starting Customer API...")
    print("📊 API Endpoints:")
    print("  GET /api/health - Health check")
    print("  GET /api/customers - List all customers (with pagination)")
    print("  GET /api/customers/<id> - Get customer details")
    print("  GET /api/customers/<id>/orders - Get customer orders")
    print("  GET /api/statistics - Get basic statistics")
    print("=" * 50)
    app.run(debug=True, host='0.0.0.0', port=5000) 