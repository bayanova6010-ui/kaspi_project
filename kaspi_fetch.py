import os
import json
import time
import requests

# ==== Баптаулар ====
TOKEN = "uaAmPg6FsQmEV69sVTl+LcA+rSTGc0vf5/I9Sk02NLI="
TIME_WINDOW_HOURS = 2         # соңғы 2 сағат
OFFSET_HOURS = 1              # Kaspi server time offset керек болса
PAGE_SIZE = 100

ARTICLES_FILE = "123.txt"     # артикулдар тізімі
OUTPUT_FILE = "orders.json"
STORE_NAME = "Bio-Farm"
DEFAULT_STATUS = "new"

HEADERS = {
    "X-Auth-Token": TOKEN,
    "Accept": "application/vnd.api+json;charset=UTF-8",
    "Content-Type": "application/vnd.api+json",
    "User-Agent": "Mozilla/5.0"
}

# ===== Көмекшілер =====
def server_now_ms() -> int:
    return int(time.time() * 1000) + OFFSET_HOURS * 3600 * 1000

def build_orders_url(page: int, start_ms: int, end_ms: int) -> str:
    return (
        "https://kaspi.kz/shop/api/v2/orders?"
        f"page[number]={page}"
        f"&page[size]={PAGE_SIZE}"
        f"&filter[orders][creationDate][$ge]={start_ms}"
        f"&filter[orders][creationDate][$le]={end_ms}"
        f"&filter[orders][status]=COMPLETED"
        f"&filter[orders][state]=ARCHIVE"
        "&include[orders]=user"
    )

def normalize_phone(raw: str) -> str:
    if not raw:
        return ""
    digits = "".join(ch for ch in raw if ch.isdigit())
    # 7004... -> 77004...
    if len(digits) == 10 and digits.startswith("7"):
        return "7" + digits
    return digits

def load_articles(path: str) -> tuple[set, set]:
    """Файлдан артикулдар. Қайта салыстыру үшін full және base ( '_' дейін ) екі сет жасаймыз."""
    full = set()
    base = set()
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                code = line.strip()
                if not code:
                    continue
                full.add(code)
                base.add(code.split("_", 1)[0])
    return full, base

def load_existing(path: str) -> list:
    if not os.path.exists(path):
        return []
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
            return data if isinstance(data, list) else []
    except Exception:
        return []

def save_list(path: str, data: list):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def safe_get(d: dict, keys: list, default=None):
    cur = d
    for k in keys:
        if not isinstance(cur, dict) or k not in cur:
            return default
        cur = cur[k]
    return cur

# ===== Product fetch + cache =====
_product_cache: dict[str, dict | None] = {}

def fetch_product(session: requests.Session, related_url: str | None, ptype: str | None, pid: str | None):
    """relationships.product.links.related басымды, болмаса /{type}/{id}. Кэшпен."""
    key = related_url or (f"{ptype}:{pid}" if ptype and pid else None)
    if not key:
        return None
    if key in _product_cache:
        return _product_cache[key]

    url = related_url or f"https://kaspi.kz/shop/api/v2/{ptype}/{pid}"
    r = session.get(url, headers=HEADERS, timeout=30)
    if r.status_code != 200:
        _product_cache[key] = None
        return None

    data = r.json()
    prod = data.get("data", data)  # кейде {"data": {...}}
    _product_cache[key] = prod
    return prod

def extract_product_fields(prod: dict) -> tuple[str | None, str | None]:
    """Product объектісінен (attributes) name/title және code/productCode/... іздеу."""
    if not isinstance(prod, dict):
        return None, None
    attrs = prod.get("attributes", {}) if "attributes" in prod else prod
    name = attrs.get("name") or attrs.get("title")
    # код кандидаттары
    code_candidates = [
        attrs.get("productCode"),
        attrs.get("code"),
        safe_get(attrs, ["defaultSku", "code"]),
        attrs.get("sku"),
        attrs.get("shopSku"),
    ]
    code = next((str(c) for c in code_candidates if c), None)
    return name, code

# ===== Entries (барлығы) =====
def fetch_entries_all(session: requests.Session, order_id: str):
    """
    Берілген заказдағы БАРЛЫҚ entries.
    Әр entry үшін product-ты тартып, (name, code) қайтарамыз.
    """
    url = f"https://kaspi.kz/shop/api/v2/orders/{order_id}/entries"
    r = session.get(url, headers=HEADERS, timeout=30)
    if r.status_code != 200:
        print(f"⚠️ Entries error {order_id}: {r.status_code}")
        return []

    items = []
    for entry in (r.json().get("data") or []):
        attrs = entry.get("attributes", {}) or {}
        rels = entry.get("relationships", {}) or {}

        related_url = safe_get(rels, ["product", "links", "related"])
        ptype = safe_get(rels, ["product", "data", "type"])
        pid   = safe_get(rels, ["product", "data", "id"])

        prod = fetch_product(session, related_url, ptype, pid)
        pname, pcode = extract_product_fields(prod)

        # Егер product-та да табылмаса — ең болмағанда категория атауын береміз
        if not pname:
            pname = safe_get(attrs, ["category", "title"]) or "Атауы жоқ"

        # Кей жағдай: entry-де productCode бар болуы мүмкін
        if not pcode:
            pcandidates = [
                attrs.get("productCode"),
                attrs.get("shopSku"),
                attrs.get("sku"),
            ]
            pcode = next((str(c) for c in pcandidates if c), "")

        items.append((pname, pcode))
    return items

# ===== Негізгі =====
def main():
    articles_full, articles_base = load_articles(ARTICLES_FILE)
    print(f"📑 Фильтрге арналған {len(articles_full)} артикул оқылды.")

    end_ms = server_now_ms()
    start_ms = end_ms - TIME_WINDOW_HOURS * 60 * 60 * 1000

    session = requests.Session()
    session.headers.update(HEADERS)

    existing = load_existing(OUTPUT_FILE)
    existing_codes = {str(x.get("order_code")) for x in existing if isinstance(x, dict)}

    new_records = []
    page = 0

    while True:
        url = build_orders_url(page, start_ms, end_ms)
        resp = session.get(url, timeout=30)
        if resp.status_code != 200:
            print("❌ HTTP", resp.status_code, resp.text[:200])
            break

        orders = resp.json().get("data") or []
        if not orders:
            if page == 0:
                page = 1  # кейбір аккаунтта 1-ден басталу мүмкін
                continue
            break

        for order in orders:
            attrs = order.get("attributes", {}) or {}
            if attrs.get("status") != "COMPLETED" or attrs.get("state") != "ARCHIVE":
                continue

            order_code = str(attrs.get("code"))
            if order_code in existing_codes:
                continue  # файлда бар

            order_id = order.get("id")
            cust = attrs.get("customer", {}) or {}

            entries = fetch_entries_all(session, order_id)
            # Кемінде бір тауар фильтрден өтсе — заказды жазамыз
            matched = None
            for pname, pcode in entries:
                base = pcode.split("_", 1)[0] if pcode else ""
                if (pcode and pcode in articles_full) or (base and base in articles_base):
                    matched = (pname, pcode if pcode else base)
                    break

            if matched:
                pname, pcode = matched
                rec = {
                    "order_code": order_code,
                    "store": STORE_NAME,
                    "product_name": pname or "Атауы жоқ",
                    "article": pcode or "",
                    "name": cust.get("name"),
                    "phone": normalize_phone(cust.get("cellPhone")),
                    "status": DEFAULT_STATUS
                }
                new_records.append(rec)
                existing_codes.add(order_code)  # дедуп

        if len(orders) < PAGE_SIZE:
            break
        page += 1

    if new_records:
        merged = existing + new_records
        save_list(OUTPUT_FILE, merged)
        print(f"\n✅ Қосылды: {len(new_records)} жаңа заказ")
    else:
        print("\nℹ️ Жаңа заказ табылмады")

    print(f"💾 Файл: {OUTPUT_FILE} | Барлығы: {len(load_existing(OUTPUT_FILE))}")

def main_loop():
    while True:
        try:
            main()   # сіздің негізгі функцияңыз
        except Exception as e:
            print("⚠️ Қате:", e)
        time.sleep(60)  # 60 секунд күту (әр минут сайын)

if __name__ == "__main__":
    main_loop()