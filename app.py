from flask import Flask, render_template, request
from inventory_crud import (
    add_new_product_purchase,
    update_existing_purchase,
    update_sales,
    read_product_across_locations,
    delete_purchase_line,        
    delete_product_safe 
)
from dashboard import dashboard_bp 

app = Flask(__name__)

app.register_blueprint(dashboard_bp)

@app.route("/")
def home():
    return render_template("index.html")

# Add Purchase
@app.route("/purchase", methods=["GET"])
def add_purchase_form():
    return render_template("add_purchase.html")

@app.route("/purchase", methods=["POST"])
def add_purchase_submit():
    data = request.form.to_dict(flat=True)
    data["store_id"] = int(data["store_id"])
    data["product_id"] = int(data["product_id"])
    data["vendor_number"] = int(data["vendor_number"])
    data["purchase_price"] = float(data["purchase_price"])
    data["quantity"] = int(data["quantity"])
    result = add_new_product_purchase(**data)
    return render_template("add_purchase.html", result=result)

# Update Purchase
@app.route("/purchase/update", methods=["GET"])
def update_purchase_form():
    return render_template("update_purchase.html")

@app.route("/purchase/update", methods=["POST"])
def update_purchase_submit():
    data = request.form.to_dict(flat=True)
    data["store_id"] = int(data["store_id"])
    data["quantity"] = int(data["quantity"])
    result = update_existing_purchase(**data)
    return render_template("update_purchase.html", result=result)

# Record Sale
@app.route("/sale", methods=["GET"])
def record_sale_form():
    return render_template("record_sale.html")

@app.route("/sale", methods=["POST"])
def record_sale_submit():
    data = request.form.to_dict(flat=True)
    data["store_id"] = int(data["store_id"])
    data["quantity"] = int(data["quantity"])
    data["sale_price"] = float(data["sale_price"]) if data["sale_price"] else None
    result = update_sales(**data)
    return render_template("record_sale.html", result=result)

# View Inventory
@app.route("/inventory", methods=["GET"])
def view_inventory_form():
    return render_template("view_inventory.html")

@app.route("/inventory", methods=["POST"])
def view_inventory_submit():
    product_name = request.form["product_name"]
    results = read_product_across_locations(product_name)
    return render_template("view_inventory.html", results=results)
    
@app.route("/purchase/delete", methods=["GET"])
def delete_purchase_form():
    return render_template("delete_purchase.html", title="Delete Purchase Line")

@app.route("/purchase/delete", methods=["POST"])
def delete_purchase_submit():
    data = request.form.to_dict(flat=True)
    data["store_id"] = int(data["store_id"])
    result = delete_purchase_line(
        city=data["city"],
        store_id=data["store_id"],
        vendor_name=data["vendor_name"],
        product_name=data["product_name"],
        size=data["size"],
        invoice_date=data["invoice_date"],
    )
    return render_template("delete_purchase.html", title="Delete Purchase Line", result=result)

@app.route("/product/delete", methods=["GET"])
def delete_product_form():
    return render_template("delete_product.html", title="Delete Product")

@app.route("/product/delete", methods=["POST"])
def delete_product_submit():
    try:
        product_id = int(request.form["product_id"])
    except ValueError:
        return render_template("delete_product.html", title="Delete Product",
                               result={"error": "product_id must be an integer."})
    result = delete_product_safe(product_id)
    return render_template("delete_product.html", title="Delete Product", result=result)
    
if __name__ == "__main__":
    app.run(debug=True)
