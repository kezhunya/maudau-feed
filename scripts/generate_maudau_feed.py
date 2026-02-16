#!/usr/bin/env python3
"""Generate Maudau feed by merging base feed with Rozetka prices/assortment."""

from __future__ import annotations

import argparse
import re
import sys
import xml.etree.ElementTree as ET
from datetime import datetime
from pathlib import Path
from urllib.request import Request, urlopen

DEFAULT_BASE_FEED = "https://aqua-favorit.com.ua/content/export/b0026fd850ce11bb0cb7610e252d7dae.xml"
DEFAULT_ROZETKA_FEED = "http://parser.biz.ua/Aqua/api/export.aspx?action=rozetka&key=ui82P2VotQQamFTj512NQJK3HOlKvyv7"
DEFAULT_OUTPUT = "update_maudau.xml"

ALLOWED_VENDORS = {"мойдодыр", "dusel"}
OLD_PRICE_TAGS = ("old_price", "oldprice", "price_old", "old", "priceold")
RE_HTML_TAG = re.compile(r"<[^>]+>")
RE_ID_ALLOWED = re.compile(r"[^A-Za-z0-9]")


def fetch_xml(source: str) -> ET.ElementTree:
    if source.startswith("http://") or source.startswith("https://"):
        req = Request(source, headers={"User-Agent": "Mozilla/5.0 (compatible; maudau-feed-bot/1.0)"})
        with urlopen(req, timeout=180) as resp:
            data = resp.read()
        return ET.ElementTree(ET.fromstring(data))
    return ET.parse(source)


def normalize_text(value: str | None) -> str:
    return (value or "").strip()


def normalize_key(value: str | None) -> str:
    return normalize_text(value).casefold()


def local_name(tag: str) -> str:
    if "}" in tag:
        return tag.split("}", 1)[1]
    return tag


def child_text(offer: ET.Element, tag: str) -> str:
    for child in list(offer):
        if local_name(child.tag) == tag:
            return normalize_text(child.text)
    return ""


def find_first_child(offer: ET.Element, tag: str) -> ET.Element | None:
    for child in list(offer):
        if local_name(child.tag) == tag:
            return child
    return None


def find_param_value(offer: ET.Element, param_name: str) -> str:
    target = normalize_key(param_name)
    for child in list(offer):
        if local_name(child.tag) != "param":
            continue
        if normalize_key(child.attrib.get("name")) == target:
            value = normalize_text(child.text)
            if value:
                return value
    return ""


def resolve_offer_id_raw(offer: ET.Element) -> str:
    article = find_param_value(offer, "Артикул")
    if article:
        return article

    vendor_code = child_text(offer, "vendorCode")
    if vendor_code:
        return vendor_code

    return normalize_text(offer.attrib.get("id"))


def resolve_offer_id_key(offer: ET.Element) -> str:
    return normalize_key(resolve_offer_id_raw(offer))


def extract_vendor(offer: ET.Element) -> str:
    return normalize_key(child_text(offer, "vendor"))


def extract_price(offer: ET.Element) -> str:
    return child_text(offer, "price")


def extract_old_price(offer: ET.Element) -> str:
    for tag in OLD_PRICE_TAGS:
        value = child_text(offer, tag)
        if value:
            return value
    return ""


def extract_available(offer: ET.Element) -> str:
    return normalize_text(offer.attrib.get("available"))


def set_or_create_child(offer: ET.Element, tag: str, value: str) -> bool:
    if not value:
        return False
    node = find_first_child(offer, tag)
    if node is None:
        node = ET.SubElement(offer, tag)
        node.text = value
        return True
    if normalize_text(node.text) != value:
        node.text = value
        return True
    return False


def set_available(offer: ET.Element, value: str) -> bool:
    value = normalize_text(value)
    if value not in {"true", "false"}:
        value = "false"
    if normalize_text(offer.attrib.get("available")) == value:
        return False
    offer.set("available", value)
    return True


def build_rozetka_index(rozetka_tree: ET.ElementTree) -> dict[str, dict[str, str]]:
    root = rozetka_tree.getroot()
    index: dict[str, dict[str, str]] = {}

    for offer in root.findall(".//offer"):
        offer_id = resolve_offer_id_key(offer)
        if not offer_id or offer_id in index:
            continue
        index[offer_id] = {
            "price": extract_price(offer),
            "old_price": extract_old_price(offer),
            "available": extract_available(offer),
        }
    return index


def remove_offer(parent_map: dict[ET.Element, ET.Element], offer: ET.Element) -> None:
    parent = parent_map.get(offer)
    if parent is not None:
        parent.remove(offer)


def strip_html(text: str) -> str:
    return normalize_text(RE_HTML_TAG.sub("", text))


def normalize_required_texts(offer: ET.Element) -> None:
    name_node = find_first_child(offer, "name")
    name_ru_node = find_first_child(offer, "name_ru")
    name_ua_node = find_first_child(offer, "name_ua")

    name_value = strip_html(name_node.text if name_node is not None else "")
    if name_ru_node is None and name_value:
        name_ru_node = ET.SubElement(offer, "name_ru")
        name_ru_node.text = name_value
    elif name_ru_node is not None:
        name_ru_node.text = strip_html(name_ru_node.text)

    if name_ua_node is not None:
        name_ua_node.text = strip_html(name_ua_node.text)

    if name_node is not None:
        offer.remove(name_node)

    desc_node = find_first_child(offer, "description")
    desc_ru_node = find_first_child(offer, "description_ru")
    desc_ua_node = find_first_child(offer, "description_ua")

    desc_value = normalize_text(desc_node.text if desc_node is not None else "")
    if desc_ru_node is None and desc_value:
        desc_ru_node = ET.SubElement(offer, "description_ru")
        desc_ru_node.text = desc_value

    if desc_node is not None:
        offer.remove(desc_node)


def normalize_old_price_tag(offer: ET.Element) -> None:
    values_by_tag: dict[str, str] = {}
    nodes_to_remove: list[ET.Element] = []

    for child in list(offer):
        tag = local_name(child.tag)
        if tag in OLD_PRICE_TAGS:
            nodes_to_remove.append(child)
            current = normalize_text(child.text)
            if current and tag not in values_by_tag:
                values_by_tag[tag] = current

    for node in nodes_to_remove:
        offer.remove(node)

    value = ""
    for tag in OLD_PRICE_TAGS:
        candidate = values_by_tag.get(tag, "")
        if candidate:
            value = candidate
            break

    if value:
        node = ET.SubElement(offer, "old_price")
        node.text = value


def cleanup_params(offer: ET.Element) -> None:
    seen_names: set[str] = set()
    for child in list(offer):
        if local_name(child.tag) != "param":
            continue
        name = strip_html(child.attrib.get("name") or "")
        value = normalize_text(child.text)
        key = normalize_key(name)
        if not name or not value or key in seen_names:
            offer.remove(child)
            continue
        child.attrib["name"] = name
        seen_names.add(key)


def normalize_offer_id(offer: ET.Element) -> bool:
    raw = resolve_offer_id_raw(offer)
    cleaned = RE_ID_ALLOWED.sub("", raw)
    if not cleaned:
        return False
    offer.set("id", cleaned)
    return True


def has_required_fields(offer: ET.Element) -> bool:
    required = [
        child_text(offer, "name_ua"),
        child_text(offer, "name_ru"),
        child_text(offer, "description_ua"),
        child_text(offer, "description_ru"),
        child_text(offer, "price"),
        child_text(offer, "categoryId"),
    ]
    if any(not item for item in required):
        return False

    picture_count = 0
    for child in list(offer):
        if local_name(child.tag) == "picture" and normalize_text(child.text):
            picture_count += 1
    return picture_count > 0


def normalize_offer(offer: ET.Element) -> bool:
    normalize_required_texts(offer)
    normalize_old_price_tag(offer)
    cleanup_params(offer)
    if not normalize_offer_id(offer):
        return False

    # enforce boolean availability
    set_available(offer, extract_available(offer))

    # keep max 12 pictures
    pictures = [child for child in list(offer) if local_name(child.tag) == "picture"]
    for extra in pictures[12:]:
        offer.remove(extra)

    return has_required_fields(offer)


def ensure_root_date(root: ET.Element) -> None:
    current = normalize_text(root.attrib.get("date"))
    if current:
        return
    root.set("date", datetime.now().strftime("%Y-%m-%d %H:%M"))


def ensure_categories(root: ET.Element) -> None:
    shop = root.find(".//shop")
    if shop is None:
        shop = ET.SubElement(root, "shop")

    categories = shop.find("categories")
    offers_parent = shop.find("offers")
    if categories is not None:
        return

    categories = ET.Element("categories")
    known_ids: set[str] = set()

    if offers_parent is not None:
        for offer in list(offers_parent):
            cat_id = child_text(offer, "categoryId")
            if not cat_id or cat_id in known_ids:
                continue
            node = ET.SubElement(categories, "category", {"id": cat_id})
            node.text = cat_id
            known_ids.add(cat_id)

    shop.insert(0, categories)


def indent_tree(elem: ET.Element, level: int = 0) -> None:
    i = "\n" + level * "  "
    if len(elem):
        if not elem.text or not elem.text.strip():
            elem.text = i + "  "
        for child in elem:
            indent_tree(child, level + 1)
        if not elem[-1].tail or not elem[-1].tail.strip():
            elem[-1].tail = i
    if level and (not elem.tail or not elem.tail.strip()):
        elem.tail = i


def generate(base_source: str, rozetka_source: str, output: Path) -> tuple[int, int, int, int, int, int]:
    base_tree = fetch_xml(base_source)
    rozetka_tree = fetch_xml(rozetka_source)

    rozetka_index = build_rozetka_index(rozetka_tree)
    root = base_tree.getroot()
    parent_map = {child: parent for parent in root.iter() for child in parent}

    total = 0
    kept = 0
    removed_missing = 0
    removed_invalid = 0
    price_changed = 0
    old_price_changed = 0
    available_changed = 0

    for offer in list(root.findall(".//offer")):
        total += 1
        vendor = extract_vendor(offer)
        key = resolve_offer_id_key(offer)
        rozetka_data = rozetka_index.get(key, {})

        if not rozetka_data and vendor not in ALLOWED_VENDORS:
            remove_offer(parent_map, offer)
            removed_missing += 1
            continue

        if set_or_create_child(offer, "price", rozetka_data.get("price", "")):
            price_changed += 1
        if set_or_create_child(offer, "old_price", rozetka_data.get("old_price", "")):
            old_price_changed += 1
        if rozetka_data and set_available(offer, rozetka_data.get("available", "")):
            available_changed += 1
        elif not rozetka_data:
            set_available(offer, extract_available(offer))

        if not normalize_offer(offer):
            remove_offer(parent_map, offer)
            removed_invalid += 1
            continue

        kept += 1

    ensure_root_date(root)
    ensure_categories(root)

    indent_tree(root)
    output.parent.mkdir(parents=True, exist_ok=True)
    base_tree.write(output, encoding="utf-8", xml_declaration=True)

    return total, kept, removed_missing, removed_invalid, price_changed, old_price_changed + available_changed


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate Maudau feed")
    parser.add_argument("--base-feed", default=DEFAULT_BASE_FEED, help="Base XML URL/path")
    parser.add_argument("--rozetka-feed", default=DEFAULT_ROZETKA_FEED, help="Rozetka XML URL/path")
    parser.add_argument("--output", default=DEFAULT_OUTPUT, help="Output XML file")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    output = Path(args.output)

    try:
        total, kept, removed_missing, removed_invalid, price_changed, other_updates = generate(
            args.base_feed,
            args.rozetka_feed,
            output,
        )
    except Exception as exc:  # noqa: BLE001
        print(f"Error: {exc}", file=sys.stderr)
        return 1

    print(f"Generated: {output}")
    print(f"Offers total: {total}")
    print(f"Offers kept: {kept}")
    print(f"Removed (not in Rozetka and not allowed vendor): {removed_missing}")
    print(f"Removed (invalid for Maudau required fields): {removed_invalid}")
    print(f"Prices updated: {price_changed}")
    print(f"Old price/availability updates: {other_updates}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
