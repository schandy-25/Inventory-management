# dashboard.py
import sqlite3
from flask import Blueprint, render_template

dashboard_bp = Blueprint("dashboard", __name__, template_folder="templates")

def query_db(query):
    conn = sqlite3.connect("inventory.db")
    conn.row_factory = sqlite3.Row
    rows = conn.execute(query).fetchall()
    conn.close()
    return [dict(r) for r in rows]

@dashboard_bp.route("/dashboard")
def dashboard():
    stock_data = query_db("""
        SELECT p.ProductName, SUM(il.Quantity) AS Stock
        FROM InvoiceLines il
        JOIN Products p ON il.ProductId = p.ProductId
        GROUP BY p.ProductName
        ORDER BY Stock DESC;
    """)

    sales_data = query_db("""
        SELECT p.ProductName, SUM(s.TotalAmount) AS Revenue
        FROM Sales s
        JOIN Products p ON s.ProductId = p.ProductId
        GROUP BY p.ProductName
        ORDER BY Revenue DESC LIMIT 5;
    """)

    city_data = query_db("""
        SELECT c.CityName, SUM(il.Quantity) AS Stock
        FROM InvoiceLines il
        JOIN Invoices i ON il.InvoiceId = i.InvoiceId
        JOIN Stores s ON i.StoreId = s.StoreId
        JOIN Cities c ON s.CityId = c.CityId
        GROUP BY c.CityName;
    """)

    low_stock = query_db("""
        SELECT p.ProductName, p.Size, SUM(il.Quantity) AS Remaining
        FROM InvoiceLines il
        JOIN Products p ON il.ProductId = p.ProductId
        GROUP BY p.ProductName, p.Size
        HAVING Remaining < 10;
    """)

    # Compute summary KPIs
    total_products = len(stock_data)
    total_revenue = sum([r["Revenue"] for r in sales_data]) if sales_data else 0
    total_stock = sum([r["Stock"] for r in stock_data]) if stock_data else 0

    return render_template(
        "dashboard.html",
        stock_data=stock_data,
        sales_data=sales_data,
        city_data=city_data,
        low_stock=low_stock,
        total_products=total_products,
        total_revenue=total_revenue,
        total_stock=total_stock
    )
