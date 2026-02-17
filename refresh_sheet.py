import os, json, re
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import requests
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials

SHOP_DOMAIN = os.environ["SHOP_DOMAIN"].strip().replace("https://", "").replace("http://", "").strip("/")
SHOP_TOKEN = os.environ["SHOPIFY_TOKEN"]
API_VERSION = os.getenv("API_VERSION", "2025-01")

SHEET_ID = os.environ["SHEET_ID"]
TAB_NAME = os.getenv("TAB_NAME", "Pedidos")
DAYS_BACK = int(os.getenv("DAYS_BACK", "7"))

BASE_URL = f"https://{SHOP_DOMAIN}/admin/api/{API_VERSION}"
HEADERS = {"X-Shopify-Access-Token": SHOP_TOKEN}


def format_datetime(date_string: Optional[str]) -> str:
    if not date_string:
        return ""
    try:
        dt = datetime.fromisoformat(date_string.replace("Z", "+00:00"))
        return dt.strftime("%d/%m/%Y %H:%M:%S")
    except Exception:
        return str(date_string)

def to_float(x: Any) -> float:
    try:
        if x is None or x == "":
            return 0.0
        return float(str(x).replace(",", "."))
    except Exception:
        return 0.0

def parse_trace_dt(value: Optional[str]) -> str:
    if not value:
        return ""
    t = str(value).strip()
    try:
        dt = datetime.fromisoformat(t.replace("Z", "+00:00"))
        return dt.strftime("%d/%m/%Y %H:%M:%S")
    except Exception:
        pass

    s = t.lower().replace("\u00a0", " ").replace("\u202f", " ").strip()
    m = re.search(
        r"(\d{2})-(\d{2})-(\d{4}).*?"
        r"(\d{1,2}):(\d{2}):(\d{2})"
        r"(?:\s*([ap])\s*\.?\s*m\s*\.?)?",
        s,
    )
    if not m:
        return t

    dd, mm, yyyy, hh, mi, ss, ap = m.groups()
    hh_i = int(hh)
    if ap == "p" and hh_i != 12:
        hh_i += 12
    if ap == "a" and hh_i == 12:
        hh_i = 0

    try:
        dt = datetime(int(yyyy), int(mm), int(dd), hh_i, int(mi), int(ss))
        return dt.strftime("%d/%m/%Y %H:%M:%S")
    except Exception:
        return t

def extract_traceability(note_attrs: Optional[List[Dict[str, Any]]]) -> Dict[str, str]:
    trace = {
        "Embalado": "",
        "Transferido_a_tienda": "",
        "Listo_para_retiro": "",
        "Retirado": "",
        "Entregado_a_transportista": "",
    }
    if not note_attrs:
        return trace

    for attr in note_attrs:
        name = str(attr.get("name", "")).strip().lower()
        value = str(attr.get("value", "")).strip()

        if name == "embalado":
            trace["Embalado"] = parse_trace_dt(value)
        elif name == "transferido_a_tienda":
            trace["Transferido_a_tienda"] = parse_trace_dt(value)
        elif name == "listo_para_retiro":
            trace["Listo_para_retiro"] = parse_trace_dt(value)
        elif name == "retirado_por_cliente":
            trace["Retirado"] = parse_trace_dt(value)
        else:
            name_norm = name.replace("-", "_").replace(" ", "_")
            if "transportista" in name_norm or "carrier" in name_norm or "entregado_a_transportista" in name_norm:
                trace["Entregado_a_transportista"] = parse_trace_dt(value)

    return trace

def get_fulfilled_at(order: Dict[str, Any]) -> Optional[str]:
    fulf = order.get("fulfillments") or []
    if not fulf:
        return None
    dates = [f.get("created_at") for f in fulf if f.get("created_at")]
    return max(dates) if dates else None


def _get_next_page_url(link_header: Optional[str]) -> Optional[str]:
    if not link_header:
        return None
    parts = [p.strip() for p in link_header.split(",")]
    for p in parts:
        if 'rel="next"' in p:
            start = p.find("<") + 1
            end = p.find(">")
            return p[start:end]
    return None


def fetch_all_orders(status="any", limit=250, created_at_min: Optional[str] = None) -> List[Dict[str, Any]]:
    orders: List[Dict[str, Any]] = []
    url = f"{BASE_URL}/orders.json"
    params: Optional[Dict[str, Any]] = {"limit": int(limit), "status": status}
    if created_at_min:
        params["created_at_min"] = created_at_min

    while True:
        r = requests.get(url, headers=HEADERS, params=params, timeout=90)
        if r.status_code != 200:
            raise RuntimeError(f"Shopify {r.status_code}: {r.text[:900]}")

        data = r.json()
        orders.extend(data.get("orders", []))

        next_url = _get_next_page_url(r.headers.get("Link"))
        if not next_url:
            break
        url = next_url
        params = None

    return orders


def orders_to_rows(orders: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for o in orders:
        billing = o.get("billing_address") or {}
        shipping = o.get("shipping_address") or {}
        line_items = o.get("line_items", []) or []

        skus = [li.get("sku") for li in line_items if li.get("sku")]
        total_skus_unicos = len(set(skus))
        total_productos = sum(int(li.get("quantity", 0) or 0) for li in line_items)

        lineitem_statuses = [li.get("fulfillment_status") for li in line_items if li.get("fulfillment_status")]
        lineitem_fulfillment = ", ".join(sorted(set(lineitem_statuses))) if lineitem_statuses else ""

        requires_shipping = any(bool(li.get("requires_shipping")) for li in line_items)

        shipping_lines = o.get("shipping_lines", []) or []
        shipping_method = shipping_lines[0].get("title") if shipping_lines else ""

        trace = extract_traceability(o.get("note_attributes") or [])

        rows.append({
            "Order_ID": o.get("id"),
            "KC": o.get("name"),
            "Created_at": format_datetime(o.get("created_at")),
            "Updated_at": format_datetime(o.get("updated_at")),
            "Financial_Status": o.get("financial_status"),
            "Paid at": format_datetime(o.get("processed_at")),
            "Fulfillment Status": o.get("fulfillment_status"),
            "Fulfilled at": format_datetime(get_fulfilled_at(o)),
            "Subtotal": to_float(o.get("subtotal_price")),
            "Shipping": to_float((o.get("total_shipping_price_set") or {}).get("shop_money", {}).get("amount")),
            "Total": to_float(o.get("total_price")),
            "Discount_Amount": to_float(o.get("total_discounts")),
            "Metodo_envio": shipping_method,
            "SKUs": total_skus_unicos,
            "Total_Productos": total_productos,
            "Billing City": billing.get("city"),
            "Billing Province": billing.get("province"),
            "Shipping City": shipping.get("city"),
            "Lineitem requires shipping": requires_shipping,
            "Lineitem fulfillment status": lineitem_fulfillment,
            "Embalado": trace["Embalado"],
            "Transferido_a_tienda": trace["Transferido_a_tienda"],
            "Listo_para_retiro": trace["Listo_para_retiro"],
            "Retirado": trace["Retirado"],
            "Entregado_a_transportista": trace["Entregado_a_transportista"],
            "Cancelled at": format_datetime(o.get("cancelled_at")),
            "Tags": o.get("tags"),
        })
    return rows


def connect_gsheets():
    creds_info = json.loads(os.environ["GOOGLE_CREDS_JSON"])
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_info(creds_info, scopes=scopes)
    return gspread.authorize(creds)

def write_df_to_sheet(gc, sheet_id: str, tab_name: str, df: pd.DataFrame):
    sh = gc.open_by_key(sheet_id)
    try:
        ws = sh.worksheet(tab_name)
    except Exception:
        ws = sh.add_worksheet(title=tab_name, rows=1000, cols=40)

    df = df.fillna("")
    values = [df.columns.tolist()] + df.astype(str).values.tolist()

    ws.clear()
    ws.update(values, value_input_option="RAW")


def main():
    created_at_min = (datetime.now(timezone.utc) - timedelta(days=DAYS_BACK)).isoformat(timespec="seconds")

    orders = fetch_all_orders(status="any", created_at_min=created_at_min, limit=250)
    df = pd.DataFrame(orders_to_rows(orders))

    for c in ["Subtotal", "Shipping", "Total", "Discount_Amount"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0).round(2)

    gc = connect_gsheets()
    write_df_to_sheet(gc, SHEET_ID, TAB_NAME, df)

    print(f"OK: {len(df)} filas escritas en '{TAB_NAME}' (últimos {DAYS_BACK} días).")

if __name__ == "__main__":
    main()
