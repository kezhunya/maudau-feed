#!/usr/bin/env python3
import os
import re
import sys
import time
from datetime import datetime
from pathlib import Path

import requests
from lxml import etree as ET

BASE_FEED_URL = "https://aqua-favorit.com.ua/content/export/b0026fd850ce11bb0cb7610e252d7dae.xml"
ROZETKA_FEED_URL = "http://parser.biz.ua/Aqua/api/export.aspx?action=rozetka&key=ui82P2VotQQamFTj512NQJK3HOlKvyv7"
OUTPUT_XML = Path("update_maudau.xml")

TMP_DIR = Path("/tmp/maudau_feed")
TMP_DIR.mkdir(parents=True, exist_ok=True)
BASE_XML = TMP_DIR / "base.xml"
ROZETKA_XML = TMP_DIR / "rozetka.xml"

ALLOWED_VENDORS = {"мойдодыр", "dusel"}
OLD_PRICE_TAGS = ("old_price", "oldprice", "price_old", "old", "priceold")
ID_CLEAN_RE = re.compile(r"[^A-Za-z0-9]")
HTML_TAG_RE = re.compile(r"<[^>]+>")

TG_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TG_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")


def send_telegram(message: str) -> None:
    if not TG_BOT_TOKEN or not TG_CHAT_ID:
        print("[WARN] Telegram secrets are not set; skip notify")
        return
    url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TG_CHAT_ID, "text": message}
    try:
        resp = requests.post(url, data=payload, timeout=20)
        resp.raise_for_status()
    except Exception as exc:
        print(f"[WARN] Telegram send failed: {exc}")


def download_file(url: str, path: Path, title: str, retries: int = 5, timeout: int = 180) -> None:
    print(f"[LOAD] {title}")
    for attempt in range(1, retries + 1):
        try:
            r = requests.get(url, stream=True, timeout=timeout)
            r.raise_for_status()
            with open(path, "wb") as f:
                for chunk in r.iter_content(1024 * 1024):
                    if chunk:
                        f.write(chunk)
            print(f"[OK] {title}")
            return
        except Exception as exc:
            print(f"[WARN] {title} attempt {attempt}/{retries}: {exc}")
            if attempt == retries:
                raise
            time.sleep(5)


def normalize_text(value: str | None) -> str:
    return (value or "").strip()


def normalize_key(value: str | None) -> str:
    return normalize_text(value).casefold()


def child_text(offer: ET._Element, tag: str) -> str:
    node = offer.find(tag)
    return normalize_text(node.text if node is not None else "")


def find_param_value(offer: ET._Element, param_name: str) -> str:
    target = normalize_key(param_name)
    for param in offer.findall("param"):
        if normalize_key(param.get("name")) == target:
            value = normalize_text(param.text)
            if value:
                return value
    return ""


def resolve_offer_id_raw(offer: ET._Element) -> str:
    article = find_param_value(offer, "Артикул")
    if article:
        return article
    vendor_code = child_text(offer, "vendorCode")
    if vendor_code:
        return vendor_code
    return normalize_text(offer.get("id"))


def resolve_offer_id_key(offer: ET._Element) -> str:
    return normalize_key(resolve_offer_id_raw(offer))


def extract_old_price(offer: ET._Element) -> str:
    for tag in OLD_PRICE_TAGS:
        value = child_text(offer, tag)
        if value:
            return value
    return ""


def extract_available(offer: ET._Element) -> str:
    return normalize_text(offer.get("available"))


def set_or_create(offer: ET._Element, tag: str, value: str) -> bool:
    value = normalize_text(value)
    if not value:
        return False
    node = offer.find(tag)
    if node is None:
        node = ET.SubElement(offer, tag)
        node.text = value
        return True
    if normalize_text(node.text) != value:
        node.text = value
        return True
    return False


def set_available(offer: ET._Element, value: str) -> bool:
    value = normalize_text(value)
    if value not in {"true", "false"}:
        value = "false"
    if normalize_text(offer.get("available")) == value:
        return False
    offer.set("available", value)
    return True


def normalize_name_description(offer: ET._Element) -> None:
    name = offer.find("name")
    name_ru = offer.find("name_ru")
    name_ua = offer.find("name_ua")

    if name is not None and name_ru is None:
        name_ru = ET.SubElement(offer, "name_ru")
        name_ru.text = normalize_text(name.text)
    if name is not None:
        offer.remove(name)

    if name_ru is not None:
        name_ru.text = normalize_text(HTML_TAG_RE.sub("", name_ru.text or ""))
    if name_ua is not None:
        name_ua.text = normalize_text(HTML_TAG_RE.sub("", name_ua.text or ""))

    desc = offer.find("description")
    desc_ru = offer.find("description_ru")
    desc_ua = offer.find("description_ua")

    if desc is not None and desc_ru is None:
        desc_ru = ET.SubElement(offer, "description_ru")
        desc_ru.text = normalize_text(desc.text)
    if desc is not None:
        offer.remove(desc)

    if desc_ru is not None:
        desc_ru.text = normalize_text(desc_ru.text)
    if desc_ua is not None:
        desc_ua.text = normalize_text(desc_ua.text)


def normalize_old_price(offer: ET._Element) -> None:
    values = {}
    for child in list(offer):
        tag = child.tag
        if tag in OLD_PRICE_TAGS:
            value = normalize_text(child.text)
            if value and tag not in values:
                values[tag] = value
            offer.remove(child)

    old_value = ""
    for tag in OLD_PRICE_TAGS:
        if values.get(tag):
            old_value = values[tag]
            break

    if old_value:
        node = ET.SubElement(offer, "old_price")
        node.text = old_value


def cleanup_params(offer: ET._Element) -> None:
    for p in list(offer.findall("param")):
        pname = normalize_text(HTML_TAG_RE.sub("", p.get("name") or ""))
        pval = normalize_text(p.text)
        if not pname or not pval:
            offer.remove(p)
            continue
        p.set("name", pname)


def normalize_offer_id(offer: ET._Element) -> bool:
    raw = resolve_offer_id_raw(offer)
    clean = ID_CLEAN_RE.sub("", raw)
    if not clean:
        return False
    offer.set("id", clean)
    return True


def has_required_fields(offer: ET._Element) -> bool:
    required_tags = ["name_ua", "name_ru", "description_ua", "description_ru", "price", "categoryId"]
    for tag in required_tags:
        if not child_text(offer, tag):
            return False

    pics = [p for p in offer.findall("picture") if normalize_text(p.text)]
    return len(pics) > 0


def normalize_offer(offer: ET._Element) -> bool:
    normalize_name_description(offer)
    normalize_old_price(offer)
    cleanup_params(offer)

    if not normalize_offer_id(offer):
        return False

    set_available(offer, extract_available(offer))

    pictures = [p for p in offer.findall("picture") if normalize_text(p.text)]
    for extra in pictures[12:]:
        offer.remove(extra)

    return has_required_fields(offer)


def ensure_root_date(root: ET._Element) -> None:
    if not normalize_text(root.get("date")):
        root.set("date", datetime.now().strftime("%Y-%m-%d %H:%M"))


def ensure_categories(root: ET._Element) -> None:
    shop = root.find("shop")
    if shop is None:
        shop = ET.SubElement(root, "shop")

    categories = shop.find("categories")
    offers = shop.find("offers")
    if categories is not None:
        return

    categories = ET.Element("categories")
    known = set()
    if offers is not None:
        for offer in offers.findall("offer"):
            cid = child_text(offer, "categoryId")
            if cid and cid not in known:
                c = ET.SubElement(categories, "category", id=cid)
                c.text = cid
                known.add(cid)

    shop.insert(0, categories)


def build_rozetka_index(tree: ET._ElementTree) -> dict[str, dict[str, str]]:
    index = {}
    for offer in tree.xpath("//offer"):
        key = resolve_offer_id_key(offer)
        if not key or key in index:
            continue
        index[key] = {
            "price": child_text(offer, "price"),
            "old_price": extract_old_price(offer),
            "available": extract_available(offer),
        }
    return index


def main() -> int:
    try:
        print("=== START MAUDAU FEED ===")
        download_file(ROZETKA_FEED_URL, ROZETKA_XML, "Rozetka feed")
        download_file(BASE_FEED_URL, BASE_XML, "Base feed")

        rozetka_tree = ET.parse(str(ROZETKA_XML))
        rozetka_idx = build_rozetka_index(rozetka_tree)

        tree = ET.parse(str(BASE_XML))
        root = tree.getroot()

        total = 0
        kept = 0
        removed_missing = 0
        removed_invalid = 0
        changed_price = 0
        changed_other = 0

        offers = root.xpath("//offer")
        for offer in list(offers):
            total += 1
            vendor = normalize_key(child_text(offer, "vendor"))
            key = resolve_offer_id_key(offer)
            rz = rozetka_idx.get(key)

            if rz is None and vendor not in ALLOWED_VENDORS:
                offer.getparent().remove(offer)
                removed_missing += 1
                continue

            if rz:
                if set_or_create(offer, "price", rz.get("price", "")):
                    changed_price += 1
                if set_or_create(offer, "old_price", rz.get("old_price", "")):
                    changed_other += 1
                if set_available(offer, rz.get("available", "")):
                    changed_other += 1
            else:
                set_available(offer, extract_available(offer))

            if not normalize_offer(offer):
                offer.getparent().remove(offer)
                removed_invalid += 1
                continue

            kept += 1

        ensure_root_date(root)
        ensure_categories(root)

        tree.write(str(OUTPUT_XML), encoding="UTF-8", xml_declaration=True, pretty_print=True)

        report = (
            "MAUDAU feed updated\n"
            f"Total offers: {total}\n"
            f"Kept offers: {kept}\n"
            f"Removed missing in Rozetka: {removed_missing}\n"
            f"Removed invalid for Maudau: {removed_invalid}\n"
            f"Price updates: {changed_price}\n"
            f"Old price/availability updates: {changed_other}"
        )

        print(report)
        send_telegram(report)
        return 0
    except Exception as exc:
        error_msg = f"MAUDAU feed failed: {exc}"
        print(error_msg, file=sys.stderr)
        send_telegram(error_msg)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
