from django.core.management.base import BaseCommand
from django.db import connections
import threading

# Sample data
USERS = [
    (1, "Alice", "alice@example.com"),
    (2, "Bob", "bob@example.com"),
    (3, "Charlie", "charlie@example.com"),
    (4, "David", "david@example.com"),
    (5, "Eve", "eve@example.com"),
    (6, "Frank", "frank@example.com"),
    (7, "Grace", "grace@example.com"),
    (8, "Alice", "alice@example.com"),
    (9, "Henry", "henry@example.com"),
    (10, "", "jane@example.com"),
]

PRODUCTS = [
    (1, "Laptop", 1000.00),
    (2, "Smartphone", 700.00),
    (3, "Headphones", 150.00),
    (4, "Monitor", 300.00),
    (5, "Keyboard", 50.00),
    (6, "Mouse", 30.00),
    (7, "Laptop", 1000.00),
    (8, "Smartwatch", 250.00),
    (9, "Gaming Chair", 500.00),
    (10, "Earbuds", -50.00),  # Invalid price
]

ORDERS = [
    (1, 1, 1, 2),
    (2, 2, 2, 1),
    (3, 3, 3, 5),
    (4, 4, 4, 1),
    (5, 5, 5, 3),
    (6, 6, 6, 4),
    (7, 7, 7, 2),
    (8, 8, 8, 0),
    (9, 9, 1, -1),
    (10, 10, 11, 2),
]

def create_tables():
    cursor_users = connections['users'].cursor()
    cursor_products = connections['products'].cursor()
    cursor_orders = connections['orders'].cursor()

    cursor_users.execute("""
        CREATE TABLE IF NOT EXISTS user (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT NOT NULL
        )
    """)

    cursor_products.execute("""
        CREATE TABLE IF NOT EXISTS product (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            price REAL NOT NULL
        )
    """)


    cursor_orders.execute("""
        CREATE TABLE IF NOT EXISTS order_table (
            id INTEGER PRIMARY KEY,
            user_id INTEGER,
            product_id INTEGER,
            quantity INTEGER
        )
    """)

def insert_users():
    cursor = connections['users'].cursor()
    print("\nInserting Users:")
    for u in USERS:
        try:
            if not u[1] or not u[2]:
                raise ValueError("Missing name or email")
            cursor.execute("INSERT INTO user (id, name, email) VALUES (?, ?, ?)", u)
            print(f"Inserted user: {u}")
        except Exception as e:
            print(f"Failed to insert user {u}: {e}")

def insert_products():
    cursor = connections['products'].cursor()
    print("\nInserting Products:")
    for p in PRODUCTS:
        try:
            if p[2] < 0:
                raise ValueError("Price cannot be negative")
            cursor.execute("INSERT INTO product (id, name, price) VALUES (?, ?, ?)", p)
            print(f"Inserted product: {p}")
        except Exception as e:
            print(f"Failed to insert product {p}: {e}")

def insert_orders():
    cursor = connections['orders'].cursor()
    print("\nInserting Orders:")
    for o in ORDERS:
        try:
            if o[3] <= 0:
                raise ValueError("Invalid quantity")
            if o[2] > 10:
                raise ValueError("Invalid product_id")

            cursor.execute("INSERT INTO order_table (id, user_id, product_id, quantity) VALUES (?, ?, ?, ?)", o)
            print(f"Inserted order: {o}")
        except Exception as e:
            print(f"Failed to insert order {o}: {e}")

class Command(BaseCommand):
    help = 'Insert data concurrently'

    def handle(self, *args, **kwargs):
        create_tables()

        threads = [
            threading.Thread(target=insert_users),
            threading.Thread(target=insert_products),
            threading.Thread(target=insert_orders),
        ]

        for t in threads:
            t.start()

        for t in threads:
            t.join()

        print("\ All insertions completed.")
