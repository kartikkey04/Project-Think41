#!/usr/bin/env python3
"""
Milestone 2: Build Customer API
Simple RESTful API using only built-in Python libraries
"""

import sqlite3
import json
import urllib.parse
from http.server import HTTPServer, BaseHTTPRequestHandler
from datetime import datetime
import re

# Database configuration
DATABASE = 'ecommerce.db'

def get_db_connection():
    """Create a database connection"""
    conn = sqlite3.connect(DATABASE)
    conn.row_factory = sqlite3.Row
    return conn

class CustomerAPIHandler(BaseHTTPRequestHandler):
    """HTTP request handler for Customer API"""
    
    def _set_headers(self, status_code=200, content_type='application/json'):
        """Set response headers"""
        self.send_response(status_code)
        self.send_header('Content-type', content_type)
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        self.end_headers()
    
    def _send_json_response(self, data, status_code=200):
        """Send JSON response"""
        self._set_headers(status_code)
        self.wfile.write(json.dumps(data, indent=2).encode())
    
    def _send_error_response(self, error, message, status_code=400):
        """Send error response"""
        self._send_json_response({
            'success': False,
            'error': error,
            'message': message
        }, status_code)
    
    def _parse_query_params(self, query_string):
        """Parse query parameters"""
        params = {}
        if query_string:
            for param in query_string.split('&'):
                if '=' in param:
                    key, value = param.split('=', 1)
                    params[urllib.parse.unquote(key)] = urllib.parse.unquote(value)
        return params
    
    def do_OPTIONS(self):
        """Handle CORS preflight requests"""
        self._set_headers()
    
    def do_GET(self):
        """Handle GET requests"""
        try:
            # Parse URL path
            path = self.path
            
            # Health check endpoint
            if path == '/api/health':
                self._send_json_response({
                    'status': 'OK',
                    'message': 'Customer API is running',
                    'timestamp': datetime.now().isoformat()
                })
                return
            
            # List customers endpoint
            if path.startswith('/api/customers') and '?' in path:
                path, query_string = path.split('?', 1)
                if path == '/api/customers':
                    self._handle_list_customers(query_string)
                    return
            
            # Get customer details endpoint
            customer_match = re.match(r'/api/customers/(\d+)$', path)
            if customer_match:
                customer_id = int(customer_match.group(1))
                self._handle_get_customer_details(customer_id)
                return
            
            # Get customer orders endpoint
            customer_orders_match = re.match(r'/api/customers/(\d+)/orders$', path)
            if customer_orders_match:
                customer_id = int(customer_orders_match.group(1))
                query_string = path.split('?')[1] if '?' in path else ''
                self._handle_get_customer_orders(customer_id, query_string)
                return
            
            # Statistics endpoint
            if path == '/api/statistics':
                self._handle_get_statistics()
                return
            
            # 404 for unknown endpoints
            self._send_error_response('Not found', 'The requested resource was not found', 404)
            
        except Exception as e:
            self._send_error_response('Internal server error', str(e), 500)
    
    def _handle_list_customers(self, query_string):
        """Handle list customers request"""
        params = self._parse_query_params(query_string)
        
        # Get query parameters
        page = max(1, int(params.get('page', 1)))
        limit = min(100, max(1, int(params.get('limit', 10))))
        search = params.get('search', '').strip()
        
        # Calculate offset
        offset = (page - 1) * limit
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        try:
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
            
            self._send_json_response({
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
            })
            
        finally:
            conn.close()
    
    def _handle_get_customer_details(self, customer_id):
        """Handle get customer details request"""
        conn = get_db_connection()
        cursor = conn.cursor()
        
        try:
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
                self._send_error_response('Customer not found', f'Customer with ID {customer_id} does not exist', 404)
                return
            
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
            
            self._send_json_response({
                'success': True,
                'data': response_data
            })
            
        finally:
            conn.close()
    
    def _handle_get_customer_orders(self, customer_id, query_string):
        """Handle get customer orders request"""
        params = self._parse_query_params(query_string)
        
        # Get query parameters
        page = max(1, int(params.get('page', 1)))
        limit = min(100, max(1, int(params.get('limit', 10))))
        offset = (page - 1) * limit
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        try:
            # First check if customer exists
            cursor.execute("SELECT id FROM users WHERE id = ?", (customer_id,))
            if not cursor.fetchone():
                self._send_error_response('Customer not found', f'Customer with ID {customer_id} does not exist', 404)
                return
            
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
            
            self._send_json_response({
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
            })
            
        finally:
            conn.close()
    
    def _handle_get_statistics(self):
        """Handle get statistics request"""
        conn = get_db_connection()
        cursor = conn.cursor()
        
        try:
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
            
            self._send_json_response({
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
            })
            
        finally:
            conn.close()

def main():
    """Start the API server"""
    server_address = ('', 5000)
    httpd = HTTPServer(server_address, CustomerAPIHandler)
    
    print("🚀 Starting Customer API...")
    print("📊 API Endpoints:")
    print("  GET /api/health - Health check")
    print("  GET /api/customers - List all customers (with pagination)")
    print("  GET /api/customers/<id> - Get customer details")
    print("  GET /api/customers/<id>/orders - Get customer orders")
    print("  GET /api/statistics - Get basic statistics")
    print("=" * 50)
    print("🌐 Server running on http://localhost:5000")
    print("Press Ctrl+C to stop the server")
    
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\n👋 Server stopped")

if __name__ == '__main__':
    main() 