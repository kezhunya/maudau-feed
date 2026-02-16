#!/usr/bin/env python3
import json
import os
import sys
from typing import Any

import requests

TG_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
GH_TOKEN = os.environ["GH_DISPATCH_TOKEN"]

# Optional guard. If set, commands from other chats are ignored.
ALLOWED_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "").strip()

FEEDS = {
    "maudau": {
        "title": "MAUDAU",
        "owner": "kezhunya",
        "repo": "maudau-feed",
        "workflow": "update-maudau-feed.yml",
        "ref": "main",
    },
    "epicenter": {
        "title": "EPICENTER",
        "owner": "kezhunya",
        "repo": "epicenter-feed",
        "workflow": "update.yml",
        "ref": "main",
    },
}


def tg_api(method: str, payload: dict[str, Any]) -> dict[str, Any]:
    url = f"https://api.telegram.org/bot{TG_TOKEN}/{method}"
    resp = requests.post(url, data=payload, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    if not data.get("ok"):
        raise RuntimeError(f"Telegram {method} failed: {data}")
    return data


def get_updates() -> list[dict[str, Any]]:
    url = f"https://api.telegram.org/bot{TG_TOKEN}/getUpdates"
    resp = requests.get(
        url,
        params={"timeout": 0, "allowed_updates": json.dumps(["message", "callback_query"])},
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json()
    if not data.get("ok"):
        raise RuntimeError(f"Telegram getUpdates failed: {data}")
    return data.get("result", [])


def ack_updates(last_update_id: int) -> None:
    url = f"https://api.telegram.org/bot{TG_TOKEN}/getUpdates"
    # Confirms all updates with update_id <= last_update_id.
    requests.get(url, params={"offset": last_update_id + 1, "timeout": 0}, timeout=30).raise_for_status()


def keyboard() -> str:
    buttons = [
        [{"text": "Обновить MAUDAU", "callback_data": "run:maudau"}],
        [{"text": "Обновить EPICENTER", "callback_data": "run:epicenter"}],
    ]
    return json.dumps({"inline_keyboard": buttons}, ensure_ascii=False)


def send_controls(chat_id: str) -> None:
    tg_api(
        "sendMessage",
        {
            "chat_id": chat_id,
            "text": "Выберите фид для обновления:",
            "reply_markup": keyboard(),
        },
    )


def answer_callback(callback_id: str, text: str) -> None:
    tg_api("answerCallbackQuery", {"callback_query_id": callback_id, "text": text, "show_alert": "false"})


def dispatch_workflow(feed_key: str) -> tuple[bool, str]:
    feed = FEEDS.get(feed_key)
    if not feed:
        return False, "Неизвестная команда"

    url = (
        f"https://api.github.com/repos/{feed['owner']}/{feed['repo']}/actions/workflows/"
        f"{feed['workflow']}/dispatches"
    )
    headers = {
        "Authorization": f"Bearer {GH_TOKEN}",
        "Accept": "application/vnd.github+json",
    }
    body = {"ref": feed["ref"]}
    resp = requests.post(url, headers=headers, json=body, timeout=30)

    if resp.status_code == 204:
        return True, f"Запущено: {feed['title']}"

    try:
        err = resp.json()
    except Exception:
        err = {"raw": resp.text}
    return False, f"Ошибка запуска {feed['title']}: {err}"


def is_allowed_chat(chat_id: str) -> bool:
    if not ALLOWED_CHAT_ID:
        return True
    return str(chat_id) == ALLOWED_CHAT_ID


def process_message(update: dict[str, Any]) -> None:
    message = update.get("message") or {}
    text = (message.get("text") or "").strip()
    chat_id = str(((message.get("chat") or {}).get("id", "")))

    if not chat_id or not is_allowed_chat(chat_id):
        return

    cmd = text.lower()
    if cmd.startswith("/"):
        cmd = cmd.split()[0]
        cmd = cmd.split("@", 1)[0]
    else:
        cmd = cmd.split()[0] if cmd else ""

    if cmd in {"/start", "/feeds", "/update", "start", "feeds", "update"}:
        send_controls(chat_id)


def process_callback(update: dict[str, Any]) -> None:
    cq = update.get("callback_query") or {}
    callback_id = cq.get("id", "")
    data = (cq.get("data") or "").strip()
    message = cq.get("message") or {}
    chat_id = str(((message.get("chat") or {}).get("id", "")))

    if not callback_id:
        return
    if not chat_id or not is_allowed_chat(chat_id):
        answer_callback(callback_id, "Нет доступа")
        return

    if not data.startswith("run:"):
        answer_callback(callback_id, "Неизвестная команда")
        return

    feed_key = data.split(":", 1)[1]
    ok, text = dispatch_workflow(feed_key)
    answer_callback(callback_id, text if ok else "Ошибка запуска")
    tg_api("sendMessage", {"chat_id": chat_id, "text": text})


def main() -> int:
    try:
        updates = get_updates()
        if not updates:
            return 0

        last_update_id = updates[-1]["update_id"]

        for upd in updates:
            if "message" in upd:
                process_message(upd)
            elif "callback_query" in upd:
                process_callback(upd)

        ack_updates(last_update_id)
        return 0
    except Exception as exc:
        print(f"telegram_feed_control error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
