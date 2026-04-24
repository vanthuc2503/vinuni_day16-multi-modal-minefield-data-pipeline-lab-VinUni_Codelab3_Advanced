from bs4 import BeautifulSoup
import re

# ==========================================
# ROLE 2: ETL/ELT BUILDER
# ==========================================
# Task: Extract product data from the HTML table, ignoring boilerplate.

def _parse_vnd_price(value: str):
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None
    if s.lower() in {"n/a", "na", "liên hệ", "lien he"}:
        return None
    # "28,500,000 VND" -> 28500000
    digits = re.sub(r"[^\d]", "", s)
    if not digits:
        return None
    try:
        return int(digits)
    except ValueError:
        return None


def parse_html_catalog(file_path):
    # --- FILE READING (Handled for students) ---
    with open(file_path, 'r', encoding='utf-8') as f:
        soup = BeautifulSoup(f, 'html.parser')
    # ------------------------------------------
    
    # TODO: Use BeautifulSoup to find the table with id 'main-catalog'
    # TODO: Extract rows, handling 'N/A' or 'Liên hệ' in the price column.
    # TODO: Return a list of dictionaries for the UnifiedDocument schema.

    table = soup.find("table", {"id": "main-catalog"})
    if not table:
        return []

    docs = []
    rows = table.find("tbody").find_all("tr") if table.find("tbody") else table.find_all("tr")
    for i, tr in enumerate(rows):
        cols = [c.get_text(strip=True) for c in tr.find_all(["td", "th"])]
        if len(cols) < 6 or cols[0].lower().startswith("mã"):
            continue

        product_code, name, category, price_raw, stock_raw, rating_raw = cols[:6]
        price_vnd = _parse_vnd_price(price_raw)
        try:
            stock = int(stock_raw)
        except Exception:
            stock = None

        content = (
            f"Product {product_code}: {name} ({category}). "
            f"Listed price (VND): {price_vnd}. Stock: {stock}. Rating: {rating_raw}."
        )

        docs.append(
            {
                "document_id": f"html-{product_code}",
                "content": content,
                "source_type": "HTML",
                "author": "Unknown",
                "timestamp": None,
                "source_metadata": {
                    "product_code": product_code,
                    "name": name,
                    "category": category,
                    "price_vnd": price_vnd,
                    "price_raw": price_raw,
                    "stock": stock,
                    "rating": rating_raw,
                    "original_file": "product_catalog.html",
                    "table_id": "main-catalog",
                },
            }
        )

    return docs

