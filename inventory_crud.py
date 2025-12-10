import sqlite3
import os

# Configurable DB path
DB_PATH = os.getenv("DB_PATH", "inventory.db")

# ---------- Helper: Get Connection ----------
def get_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn


# ---------- 1️⃣ Add New Product Purchase ----------
def add_new_product_purchase(store_id, product_id, product_name, size,
                             vendor_number, vendor_name,
                             invoice_date, purchase_price, quantity):
    """
    Adds a new product purchase transaction to the database.
    Creates Product, Vendor, Invoice, and InvoiceLine if needed.
    ❗ Throws an error if ProductId already exists.
    Returns the inserted purchase row as dict (JSON-ready).
    """

    conn = get_connection()
    cur = conn.cursor()

    # Check for existing ProductId
    cur.execute("SELECT COUNT(*) FROM Products WHERE ProductId = ?;", (product_id,))
    if cur.fetchone()[0] > 0:
        conn.close()
        return {"error": f"ProductId {product_id} already exists. Please use a new ProductId."}

    # Ensure Vendor exists
    cur.execute("""
        INSERT OR IGNORE INTO Vendors (VendorNumber, VendorName)
        VALUES (?, ?)
    """, (vendor_number, vendor_name))

    # Insert Product
    cur.execute("""
        INSERT INTO Products (ProductId, Brand, ProductName, Size)
        VALUES (?, ?, ?, ?)
    """, (product_id, vendor_number, product_name, size))

    # Create Invoice
    cur.execute("""
        INSERT INTO Invoices (StoreId, VendorNumber, InvoiceDate)
        VALUES (?, ?, ?)
    """, (store_id, vendor_number, invoice_date))
    invoice_id = cur.lastrowid

    # Insert Invoice Line
    line_total = float(purchase_price) * float(quantity)
    cur.execute("""
        INSERT INTO InvoiceLines (InvoiceId, ProductId, PurchasePrice, Quantity, LineTotal)
        VALUES (?, ?, ?, ?, ?);
    """, (invoice_id, product_id, float(purchase_price), quantity, line_total))

    conn.commit()

    # Retrieve and return the joined record
    cur.execute("""
        SELECT 
            c.CityName, s.StoreId, v.VendorName, p.ProductName, p.Size,
            i.InvoiceDate, ROUND(il.PurchasePrice, 2) AS PurchasePrice,
            il.Quantity, ROUND(il.LineTotal, 2) AS LineTotal
        FROM InvoiceLines il
        JOIN Invoices i ON il.InvoiceId = i.InvoiceId
        JOIN Vendors v ON i.VendorNumber = v.VendorNumber
        JOIN Products p ON il.ProductId = p.ProductId
        JOIN Stores s ON i.StoreId = s.StoreId
        JOIN Cities c ON s.CityId = c.CityId
        WHERE il.InvoiceId = ? AND il.ProductId = ?;
    """, (invoice_id, product_id))

    row = cur.fetchone()
    conn.close()

    if not row:
        return {"error": "No matching inserted record found."}
    return dict(row)


# ---------- 2️⃣ Update Existing Purchase ----------
def update_existing_purchase(city, store_id, vendor_name, product_name, size, invoice_date, quantity):
    """
    Updates purchase quantity only if City, Store, and Product already exist.
    Uses the latest PurchasePrice from an existing record.
    Returns updated record as dict.
    """

    conn = get_connection()
    cur = conn.cursor()

    # Verify City
    cur.execute("SELECT CityId FROM Cities WHERE CityName = ?;", (city,))
    row = cur.fetchone()
    if not row:
        conn.close()
        return {"error": f"City '{city}' does not exist."}
    city_id = row[0]

    # Verify Store
    cur.execute("SELECT CityId FROM Stores WHERE StoreId = ?;", (store_id,))
    row = cur.fetchone()
    if not row or row[0] != city_id:
        conn.close()
        return {"error": f"Store {store_id} not found in city '{city}'."}

    # Verify Product
    cur.execute("SELECT ProductId, Brand FROM Products WHERE ProductName = ? AND Size = ?;", (product_name, size))
    row = cur.fetchone()
    if not row:
        conn.close()
        return {"error": f"Product '{product_name}' ({size}) not found."}
    product_id, brand = row

    # Ensure Vendor
    cur.execute("SELECT VendorNumber FROM Vendors WHERE VendorName = ?;", (vendor_name,))
    vendor_row = cur.fetchone()
    if vendor_row:
        vendor_number = vendor_row[0]
    else:
        cur.execute("INSERT INTO Vendors (VendorName) VALUES (?);", (vendor_name,))
        vendor_number = cur.lastrowid

    # Check or Create Invoice
    cur.execute("""
        SELECT InvoiceId FROM Invoices
        WHERE StoreId = ? AND VendorNumber = ? AND InvoiceDate = ?;
    """, (store_id, vendor_number, invoice_date))
    invoice_row = cur.fetchone()
    if invoice_row:
        invoice_id = invoice_row[0]
    else:
        cur.execute("""
            INSERT INTO Invoices (StoreId, VendorNumber, InvoiceDate)
            VALUES (?, ?, ?);
        """, (store_id, vendor_number, invoice_date))
        invoice_id = cur.lastrowid

    # Get latest PurchasePrice
    cur.execute("""
        SELECT CAST(PurchasePrice AS REAL)
        FROM InvoiceLines il
        JOIN Products p ON il.ProductId = p.ProductId
        WHERE p.ProductName = ?
        ORDER BY il.InvoiceLineId DESC
        LIMIT 1;
    """, (product_name,))
    price_row = cur.fetchone()
    if not price_row:
        conn.close()
        return {"error": f"No previous purchase price for '{product_name}'."}
    purchase_price = float(price_row[0])

    # Update or Insert InvoiceLine
    cur.execute("SELECT InvoiceLineId, Quantity FROM InvoiceLines WHERE InvoiceId = ? AND ProductId = ?;",
                (invoice_id, product_id))
    line = cur.fetchone()

    if line:
        line_id, old_qty = line
        new_qty = old_qty + quantity
        new_total = new_qty * purchase_price
        cur.execute("""
            UPDATE InvoiceLines
            SET Quantity = ?, PurchasePrice = ?, LineTotal = ?
            WHERE InvoiceLineId = ?;
        """, (new_qty, purchase_price, new_total, line_id))
    else:
        line_total = purchase_price * quantity
        cur.execute("""
            INSERT INTO InvoiceLines (InvoiceId, ProductId, PurchasePrice, Quantity, LineTotal)
            VALUES (?, ?, ?, ?, ?);
        """, (invoice_id, product_id, purchase_price, quantity, line_total))

    conn.commit()

    # Return updated joined view
    cur.execute("""
        SELECT c.CityName, s.StoreId, v.VendorName, p.ProductName, p.Size,
               i.InvoiceDate, il.Quantity,
               ROUND(il.PurchasePrice, 2) AS PurchasePrice,
               ROUND(il.LineTotal, 2) AS LineTotal
        FROM InvoiceLines il
        JOIN Invoices i ON il.InvoiceId = i.InvoiceId
        JOIN Vendors v ON i.VendorNumber = v.VendorNumber
        JOIN Products p ON il.ProductId = p.ProductId
        JOIN Stores s ON i.StoreId = s.StoreId
        JOIN Cities c ON s.CityId = c.CityId
        WHERE c.CityName = ? AND s.StoreId = ? AND p.ProductName = ? AND p.Size = ?
              AND v.VendorName = ? AND i.InvoiceDate = ?;
    """, (city, store_id, product_name, size, vendor_name, invoice_date))
    row = cur.fetchone()
    conn.close()

    return dict(row) if row else {"error": "No updated row found."}


# ---------- 3️⃣ Update Sales ----------
def update_sales(city, store_id, product_name, size, sale_date, quantity, sale_price=None):
    """
    Records or updates a sale entry, deducting from inventory.
    Returns sale info + updated inventory.
    """

    conn = get_connection()
    cur = conn.cursor()

    # Validate City and Store
    cur.execute("SELECT CityId FROM Cities WHERE CityName = ?;", (city,))
    city_row = cur.fetchone()
    if not city_row:
        conn.close()
        return {"error": f"City '{city}' not found."}
    city_id = city_row[0]

    cur.execute("SELECT CityId FROM Stores WHERE StoreId = ?;", (store_id,))
    store_row = cur.fetchone()
    if not store_row or store_row[0] != city_id:
        conn.close()
        return {"error": f"Store {store_id} invalid for city '{city}'."}

    # Validate Product
    cur.execute("SELECT ProductId FROM Products WHERE ProductName = ? AND Size = ?;", (product_name, size))
    product_row = cur.fetchone()
    if not product_row:
        conn.close()
        return {"error": f"Product '{product_name}' ({size}) not found."}
    product_id = product_row[0]

    # Determine Sale Price
    if sale_price is None:
        cur.execute("""
            SELECT SalePrice FROM Sales s
            JOIN Products p ON s.ProductId = p.ProductId
            WHERE p.ProductName = ?
            ORDER BY s.SaleId DESC LIMIT 1;
        """, (product_name,))
        row = cur.fetchone()
        if not row:
            conn.close()
            return {"error": f"No previous sale price found for '{product_name}'."}
        sale_price = float(row[0])
    else:
        sale_price = float(sale_price)

    total_amount = float(quantity) * sale_price

    # Check Inventory
    cur.execute("""
        SELECT SUM(Quantity) FROM InvoiceLines il
        JOIN Products p ON il.ProductId = p.ProductId
        JOIN Invoices i ON il.InvoiceId = i.InvoiceId
        WHERE i.StoreId = ? AND p.ProductName = ?;
    """, (store_id, product_name))
    available = cur.fetchone()[0] or 0
    if available < quantity:
        conn.close()
        return {"error": f"Not enough stock. Available: {available}, Requested: {quantity}"}

    # Upsert Sale
    cur.execute("SELECT SaleId, Quantity FROM Sales WHERE StoreId = ? AND ProductId = ? AND SaleDate = ?;",
                (store_id, product_id, sale_date))
    sale_row = cur.fetchone()
    if sale_row:
        sale_id, old_qty = sale_row
        new_qty = old_qty + quantity
        new_total = new_qty * sale_price
        cur.execute("""
            UPDATE Sales SET Quantity = ?, SalePrice = ?, TotalAmount = ?
            WHERE SaleId = ?;
        """, (new_qty, sale_price, new_total, sale_id))
    else:
        cur.execute("""
            INSERT INTO Sales (StoreId, ProductId, SaleDate, Quantity, SalePrice, TotalAmount)
            VALUES (?, ?, ?, ?, ?, ?);
        """, (store_id, product_id, sale_date, quantity, sale_price, total_amount))

    # Deduct Inventory (FIFO)
    cur.execute("SELECT InvoiceLineId, Quantity FROM InvoiceLines WHERE ProductId = ? ORDER BY InvoiceLineId ASC;",
                (product_id,))
    lines = cur.fetchall()
    remaining = quantity
    for inv_id, inv_qty in lines:
        if remaining <= 0:
            break
        if inv_qty <= remaining:
            cur.execute("UPDATE InvoiceLines SET Quantity = 0 WHERE InvoiceLineId = ?;", (inv_id,))
            remaining -= inv_qty
        else:
            new_qty = inv_qty - remaining
            cur.execute("UPDATE InvoiceLines SET Quantity = ? WHERE InvoiceLineId = ?;", (new_qty, inv_id))
            remaining = 0

    conn.commit()

    # Prepare Response
    cur.execute("""
        SELECT c.CityName, s.StoreId, p.ProductName, p.Size,
               sa.SaleDate, sa.Quantity, ROUND(sa.SalePrice, 2) AS SalePrice,
               ROUND(sa.TotalAmount, 2) AS TotalAmount
        FROM Sales sa
        JOIN Stores s ON sa.StoreId = s.StoreId
        JOIN Cities c ON s.CityId = c.CityId
        JOIN Products p ON sa.ProductId = p.ProductId
        WHERE c.CityName = ? AND s.StoreId = ? AND p.ProductName = ? AND p.Size = ? AND sa.SaleDate = ?;
    """, (city, store_id, product_name, size, sale_date))
    sale = cur.fetchone()

    cur.execute("""
        SELECT p.ProductName, p.Size, SUM(il.Quantity) AS RemainingStock
        FROM InvoiceLines il
        JOIN Products p ON il.ProductId = p.ProductId
        JOIN Invoices i ON il.InvoiceId = i.InvoiceId
        WHERE i.StoreId = ? AND p.ProductName = ?
        GROUP BY p.ProductName, p.Size;
    """, (store_id, product_name))
    inv = cur.fetchone()
    conn.close()

    return {
        "sale_record": dict(sale) if sale else None,
        "inventory_after_sale": dict(inv) if inv else None
    }


# ---------- 4️⃣ Read Product Across All Locations ----------
def read_product_across_locations(product_name):
    """
    Returns all stores & cities where a product exists (inventory).
    """

    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT s.StoreId AS Store, c.CityName AS City,
               p.ProductName AS Product, p.Size AS Size,
               SUM(il.Quantity) AS Quantity
        FROM InvoiceLines il
        JOIN Invoices i ON il.InvoiceId = i.InvoiceId
        JOIN Stores s ON i.StoreId = s.StoreId
        JOIN Cities c ON s.CityId = c.CityId
        JOIN Products p ON il.ProductId = p.ProductId
        WHERE p.ProductName = ?
        GROUP BY s.StoreId, c.CityName, p.ProductName, p.Size
        ORDER BY c.CityName, s.StoreId;
    """, (product_name,))
    rows = cur.fetchall()
    conn.close()

    return [dict(r) for r in rows] if rows else []
    
# ---------- 5️⃣ Delete Purchase Line (by city, store, vendor, product, size, invoice_date) ----------
def delete_purchase_line(city, store_id, vendor_name, product_name, size, invoice_date):
    """
    Deletes a specific purchase line (an inventory record).
    If the invoice has no lines left afterward, the invoice itself is deleted.
    """
    conn = get_connection()
    cur = conn.cursor()

    # City
    cur.execute("SELECT CityId FROM Cities WHERE CityName = ?;", (city,))
    row = cur.fetchone()
    if not row:
        conn.close()
        return {"error": f"City '{city}' not found."}
    city_id = row[0]

    # Store belongs to city
    cur.execute("SELECT CityId FROM Stores WHERE StoreId = ?;", (store_id,))
    row = cur.fetchone()
    if not row or row[0] != city_id:
        conn.close()
        return {"error": f"Store {store_id} is not in city '{city}'."}

    # Vendor
    cur.execute("SELECT VendorNumber FROM Vendors WHERE VendorName = ?;", (vendor_name,))
    row = cur.fetchone()
    if not row:
        conn.close()
        return {"error": f"Vendor '{vendor_name}' not found."}
    vendor_number = row[0]

    # Product
    cur.execute("SELECT ProductId FROM Products WHERE ProductName = ? AND Size = ?;", (product_name, size))
    row = cur.fetchone()
    if not row:
        conn.close()
        return {"error": f"Product '{product_name}' ({size}) not found."}
    product_id = row[0]

    # Invoice
    cur.execute("""
        SELECT InvoiceId FROM Invoices
        WHERE StoreId = ? AND VendorNumber = ? AND InvoiceDate = ?;
    """, (store_id, vendor_number, invoice_date))
    row = cur.fetchone()
    if not row:
        conn.close()
        return {"error": "Invoice not found for the given store, vendor, and date."}
    invoice_id = row[0]

    # Purchase line
    cur.execute("""
        SELECT InvoiceLineId, Quantity, PurchasePrice, LineTotal
        FROM InvoiceLines
        WHERE InvoiceId = ? AND ProductId = ?;
    """, (invoice_id, product_id))
    line = cur.fetchone()
    if not line:
        conn.close()
        return {"error": "No matching invoice line found for that product on the invoice."}

    line_id, qty, price, total = line

    # Delete line
    cur.execute("DELETE FROM InvoiceLines WHERE InvoiceLineId = ?;", (line_id,))

    # If invoice now empty, delete it
    cur.execute("SELECT COUNT(*) FROM InvoiceLines WHERE InvoiceId = ?;", (invoice_id,))
    remaining_lines = cur.fetchone()[0]
    invoice_deleted = False
    if remaining_lines == 0:
        cur.execute("DELETE FROM Invoices WHERE InvoiceId = ?;", (invoice_id,))
        invoice_deleted = True

    conn.commit()
    conn.close()

    return {
        "deleted_line": {
            "City": city,
            "StoreId": store_id,
            "VendorName": vendor_name,
            "ProductName": product_name,
            "Size": size,
            "InvoiceDate": invoice_date,
            "QuantityRemoved": qty,
            "PurchasePrice": round(float(price), 2) if price is not None else None,
            "LineTotal": round(float(total), 2) if total is not None else None,
        },
        "invoice_deleted": invoice_deleted
    }


# ---------- 6️⃣ Delete Product (only if not referenced by sales or invoices) ----------
def delete_product_safe(product_id):
    """
    Deletes a product ONLY if it has no references in InvoiceLines or Sales.
    """
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("SELECT ProductId, ProductName, Size FROM Products WHERE ProductId = ?;", (product_id,))
    prod = cur.fetchone()
    if not prod:
        conn.close()
        return {"error": f"ProductId {product_id} not found."}
    _, product_name, size = prod

    cur.execute("SELECT COUNT(*) FROM InvoiceLines WHERE ProductId = ?;", (product_id,))
    inv_refs = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM Sales WHERE ProductId = ?;", (product_id,))
    sale_refs = cur.fetchone()[0]

    if inv_refs > 0 or sale_refs > 0:
        conn.close()
        return {"error": f"Cannot delete ProductId {product_id}: referenced by invoices={inv_refs}, sales={sale_refs}."}

    cur.execute("DELETE FROM Products WHERE ProductId = ?;", (product_id,))
    conn.commit()
    conn.close()

    return {"deleted_product": {"ProductId": product_id, "ProductName": product_name, "Size": size}}

