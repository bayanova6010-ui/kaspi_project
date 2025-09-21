import os, re, json, time, random, urllib.parse, shutil, tempfile, sys
from typing import Any, Dict, Optional
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright, TimeoutError as PlayTimeout

load_dotenv()
HEADLESS = os.getenv("HEADLESS", "false").lower() == "true"
USER_DATA_DIR = os.path.join(os.getcwd(), "wa_user_data")

# -------------------- Утилиттер --------------------
def human_delay(a=0.8, b=1.8):
    time.sleep(round(random.uniform(a, b), 2))

def to_e164_kz(phone_raw: str) -> Optional[str]:
    digits = re.sub(r"\D+", "", phone_raw or "")
    if len(digits) == 11 and digits.startswith("8"):
        return "+7" + digits[1:]
    if len(digits) == 11 and digits.startswith("7"):
        return "+" + digits
    if len(digits) == 10:
        return "+7" + digits
    if len(digits) == 12 and digits.startswith("77"):
        return "+" + digits
    return None

def read_json(path: str) -> Any:
    if not os.path.exists(path):
        return None
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def safe_write_json(path: str, data: Any):
    backup = path + ".bak"
    try:
        if os.path.exists(path):
            shutil.copy2(path, backup)
    except Exception:
        pass
    tmp_fd, tmp_path = tempfile.mkstemp(prefix="tmp_json_", suffix=".json")
    with os.fdopen(tmp_fd, "w", encoding="utf-8") as tmpf:
        json.dump(data, tmpf, ensure_ascii=False, indent=2)
    shutil.move(tmp_path, path)

def get_product_code(order: Dict[str, Any]) -> str:
    art = str(order.get("article", "")).strip()
    if "_" in art or not art:
        return art
    suf = str(order.get("article_suffix", "") or order.get("product_code_suffix", "")).strip()
    return f"{art}_{suf}" if suf else art

def build_combined_message(order: Dict[str, Any]) -> str:
    name = order.get("name", "")
    store = order.get("store", "")
    product = order.get("product_name", "")
    order_code = order.get("order_code", "")
    article_code = get_product_code(order)

    review_url = (
        "https://kaspi.kz/shop/review/productreview"
        f"?orderCode={order_code}&productCode={article_code}&rating=5"
    )

    kk = f"""Сәлеметсіз бе, {name}!

{store} дүкенінде:
{product}

сатып алуыңызбен құттықтаймыз. Сізге бәрі ұнады деп үміттенеміз.
Біз үшін әрбір тұтынушымыздың пікірі өте маңызды, сондықтан осында біздің дүкеннің атын көрсете отырып, пікір қалдыруыңызды сұраймыз:
{review_url}

Алдын-ала рахмет!
Ізгі ниетпен, {store}!"""

    ru = f"""Здравствуйте, {name}!

Поздравляем с покупкой!
Мы очень рады, что вы приобрели:
{product}

именно в магазине {store} и надеемся, что вам все понравилось!
Пожалуйста, оставьте отзыв, для нас это крайне важно:
{review_url}

Заранее вам признательны!
С уважением, компания {store}!"""

    return f"""{kk}

= = = = = = = = = = = = = = = = = = = =

{ru}"""

# -------------------- WhatsApp жіберу --------------------
def ensure_context():
    pw = sync_playwright().start()
    browser = pw.chromium.launch_persistent_context(
        USER_DATA_DIR,
        headless=HEADLESS,
        args=["--disable-blink-features=AutomationControlled"]
    )
    page = browser.new_page()
    return pw, browser, page

def _get_msg_box(page):
    candidates = [
        'footer div[contenteditable="true"]',
        'div[contenteditable="true"][data-tab]',
        'div[data-testid="conversation-compose-box-input"]',
    ]
    for sel in candidates:
        loc = page.locator(sel).last
        try:
            if loc.is_visible():
                return loc
        except Exception:
            pass
    return page.locator('div[contenteditable="true"]').last

def _click_send_button(page) -> bool:
    send_selectors = [
        'button[aria-label="Send"]',
        'span[data-icon="send"]',
    ]
    for sel in send_selectors:
        try:
            btn = page.locator(sel).first
            if btn.is_visible():
                if btn.evaluate("el => el.tagName.toLowerCase() === 'span'"):
                    btn.locator("xpath=ancestor::button[1]").click(timeout=2000)
                else:
                    btn.click(timeout=2000)
                return True
        except Exception:
            continue
    return False

def send_whatsapp_message(page, phone_e164: str, text: str) -> bool:
    phone_digits = "".join(ch for ch in phone_e164 if ch.isdigit())
    url = f"https://web.whatsapp.com/send?phone={phone_digits}&text={urllib.parse.quote(text)}"

    for attempt in range(1, 4):
        page.goto(url, wait_until="domcontentloaded")

        # Чаттың негізгі grid-ін күтеміз (болмаса — жалғастыра береміз)
        try:
            page.wait_for_selector('div[role="grid"]', timeout=15000)
        except PlayTimeout:
            pass

        # Нөмір жарамсыз болса
        try:
            if page.locator("text=Phone number shared via url is invalid").first.is_visible():
                print(f"❌ Нөмір WhatsApp-та жоқ: {phone_e164}")
                return False
        except Exception:
            pass

        # Мәтін енгізу боксын күтеміз
        try:
            msg_box = _get_msg_box(page)
            msg_box.wait_for(state="visible", timeout=45000)
        except PlayTimeout:
            print("⌛ Чатты күту таймауты.")
            continue

        # Фокус береміз
        try:
            msg_box.click()
            human_delay()
        except Exception:
            pass

        # Егер URL арқылы мәтін алдын ала түспесе — өзіміз саламыз
        try:
            current = ""
            try:
                current = msg_box.inner_text().strip()
            except Exception:
                current = ""

            if not current:
                # .type орнына .fill әлдеқайда тұрақты
                try:
                    msg_box.fill(text)
                except Exception:
                    # Ең сенімді жол: locator.evaluate арқылы insertText
                    msg_box.evaluate(
                        "(el, val) => { el.focus(); document.execCommand('selectAll', false, null); document.execCommand('insertText', false, val); }",
                        text
                    )
        except Exception as e:
            print("⚠️ Мәтін енгізу кезінде қате:", e)

        human_delay()

        # 1) Батырмамен жіберу талабы
        if _click_send_button(page):
            print(f"✅ Жіберілді → {phone_e164} (attempt {attempt})")
            return True

        # 2) Enter арқылы жіберу (батырма табылмаса)
        try:
            msg_box.press("Enter")
            print(f"✅ Жіберілді → {phone_e164} (Enter, attempt {attempt})")
            return True
        except Exception as e:
            print(f"⚠️ Enter жіберілмеді (attempt {attempt}):", e)

        time.sleep(2 + attempt)

    print("❌ Жіберу сәтсіз (3 рет тырыстық).")
    return False

# -------------------- Негізгі цикл --------------------
def process_orders(path: str, page) -> int:
    data = read_json(path)
    if not data:
        return 0

    updated = False
    sent_count = 0

    orders = data if isinstance(data, list) else [data]

    for order in orders:
        if str(order.get("status", "")).lower() != "new":
            continue
        e164 = to_e164_kz(order.get("phone", ""))
        if not e164:
            continue
        msg = build_combined_message(order)
        if send_whatsapp_message(page, e164, msg):
            order["status"] = "отправлен"
            updated = True
            sent_count += 1
            human_delay(2.0, 4.0)

    if updated:
        safe_write_json(path, data)

    return sent_count

def main():
    if len(sys.argv) < 2:
        print("Қолдану: python wa_send_from_json.py orders.json")
        return
    path = sys.argv[1]

    pw, browser, page = ensure_context()
    try:
        while True:
            count = process_orders(path, page)
            if count > 0:
                print(f"📝 {count} заказ жаңартылды (отправлен).")
            time.sleep(3)
    finally:
        browser.close()
        pw.stop()

if __name__ == "__main__":
    main()