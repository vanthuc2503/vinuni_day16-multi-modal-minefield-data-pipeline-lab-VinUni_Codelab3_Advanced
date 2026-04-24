import pandas as pd
import re
from datetime import datetime

# ==========================================
# ROLE 2: ETL/ELT BUILDER
# ==========================================
# Task: Process sales records, handling type traps and duplicates.

_WORD_NUM = {
    "zero": 0,
    "one": 1,
    "two": 2,
    "three": 3,
    "four": 4,
    "five": 5,
    "six": 6,
    "seven": 7,
    "eight": 8,
    "nine": 9,
    "ten": 10,
}


def _parse_price_to_float(value):
    if value is None:
        return None
    if isinstance(value, (int, float)):
        if pd.isna(value):
            return None
        return float(value)

    s = str(value).strip()
    if not s or s.lower() in {"n/a", "na", "null", "none"}:
        return None
    if s.lower() in {"liên hệ", "lien he", "contact"}:
        return None

    # "$1200" / "1,200.50"
    s2 = s.replace(",", "")
    s2 = re.sub(r"^\$", "", s2)
    try:
        return float(s2)
    except ValueError:
        pass

    # "five dollars"
    m = re.search(r"\b(" + "|".join(_WORD_NUM.keys()) + r")\b", s.lower())
    if m:
        return float(_WORD_NUM[m.group(1)])

    # fallback: first signed number in the string
    m = re.search(r"[-+]?\d+(?:\.\d+)?", s2)
    if m:
        try:
            return float(m.group(0))
        except ValueError:
            return None
    return None


def _normalize_date(value):
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    s = str(value).strip()
    if not s:
        return None

    # Try robust parsing; dayfirst handles dd/mm/yyyy and dd-mm-yyyy
    dt = pd.to_datetime(s, errors="coerce", dayfirst=True)
    if pd.isna(dt):
        return None
    if isinstance(dt, pd.Timestamp):
        dt = dt.to_pydatetime()
    if isinstance(dt, datetime):
        return dt.date().isoformat()
    return None


def process_sales_csv(file_path):
    # --- FILE READING (Handled for students) ---
    df = pd.read_csv(file_path)
    # ------------------------------------------
    
    # TODO: Remove duplicate rows based on 'id'
    # TODO: Clean 'price' column: convert "$1200", "250000", "five dollars" to floats
    # TODO: Normalize 'date_of_sale' into a single format (YYYY-MM-DD)
    # TODO: Return a list of dictionaries for the UnifiedDocument schema.

    if "id" in df.columns:
        df = df.drop_duplicates(subset=["id"], keep="first")

    records = []
    for _, row in df.iterrows():
        sale_id = str(row.get("id")).strip()
        product_name = str(row.get("product_name", "")).strip()
        category = str(row.get("category", "")).strip()
        currency = str(row.get("currency", "")).strip() or "Unknown"
        seller_id = str(row.get("seller_id", "")).strip()

        price = _parse_price_to_float(row.get("price"))
        date_norm = _normalize_date(row.get("date_of_sale"))

        stock_raw = row.get("stock_quantity")
        stock_qty = None
        if stock_raw is not None and not (isinstance(stock_raw, float) and pd.isna(stock_raw)):
            try:
                stock_qty = int(float(stock_raw))
            except Exception:
                stock_qty = None

        content = (
            f"Sale record for '{product_name}' in category '{category}'. "
            f"Price: {price} {currency}. Date of sale: {date_norm}. Seller: {seller_id}."
        )

        records.append(
            {
                "document_id": f"csv-{sale_id}",
                "content": content,
                "source_type": "CSV",
                "author": "Unknown",
                "timestamp": None,
                "source_metadata": {
                    "sale_id": sale_id,
                    "product_name": product_name,
                    "category": category,
                    "price": price,
                    "currency": currency,
                    "date_of_sale": date_norm,
                    "seller_id": seller_id,
                    "stock_quantity": stock_qty,
                    "original_file": "sales_records.csv",
                },
            }
        )

    return records

