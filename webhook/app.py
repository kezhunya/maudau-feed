#!/usr/bin/env python3
import json
import os
import time
from typing import Any

import requests
from fastapi import FastAPI, Header, HTTPException, Request

TG_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
GH_TOKEN = os.environ.get("GH_DISPATCH_TOKEN", "")
ALLOWED_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "").strip()
WEBHOOK_SECRET = os.environ.get("TELEGRAM_WEBHOOK_SECRET", "")

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

DIRECT_FEEDS = {
    "hotline": {
        "title": "HOTLINE",
        "url": os.environ.get(
            "HOTLINE_FEED_URL",
            "https://aqua-favorit.com.ua/marketplace-integration/generate-feed/hotline",
        ),
        "auth_type": os.environ.get("HOTLINE_AUTH_TYPE", "none").strip().lower(),
        "method": os.environ.get("HOTLINE_METHOD", "GET").strip().upper(),
        "api_body": os.environ.get("HOTLINE_API_BODY", "").strip(),
        "api_token_placement": os.environ.get("HOTLINE_API_TOKEN_PLACEMENT", "bearer").strip().lower(),
        "api_token_key": os.environ.get("HOTLINE_API_TOKEN_KEY", "token").strip(),
        "basic_user": os.environ.get("HOTLINE_BASIC_USER", ""),
        "basic_pass": os.environ.get("HOTLINE_BASIC_PASS", ""),
        "bearer_token": os.environ.get("HOTLINE_BEARER_TOKEN", ""),
        "header_name": os.environ.get("HOTLINE_HEADER_NAME", ""),
        "header_value": os.environ.get("HOTLINE_HEADER_VALUE", ""),
        "cookie": os.environ.get("HOTLINE_COOKIE", ""),
    },
    "rozetka_direct": {
        "title": "ROZETKA",
        "url": os.environ.get(
            "ROZETKA_FEED_URL",
            "https://aqua-favorit.com.ua/marketplace-integration/generate-feed/rozetka-feed",
        ),
        "auth_type": os.environ.get("ROZETKA_AUTH_TYPE", "none").strip().lower(),
        "method": os.environ.get("ROZETKA_METHOD", "GET").strip().upper(),
        "api_body": os.environ.get("ROZETKA_API_BODY", "").strip(),
        "api_token_placement": os.environ.get("ROZETKA_API_TOKEN_PLACEMENT", "bearer").strip().lower(),
        "api_token_key": os.environ.get("ROZETKA_API_TOKEN_KEY", "token").strip(),
        "basic_user": os.environ.get("ROZETKA_BASIC_USER", ""),
        "basic_pass": os.environ.get("ROZETKA_BASIC_PASS", ""),
        "bearer_token": os.environ.get("ROZETKA_BEARER_TOKEN", ""),
        "header_name": os.environ.get("ROZETKA_HEADER_NAME", ""),
        "header_value": os.environ.get("ROZETKA_HEADER_VALUE", ""),
        "cookie": os.environ.get("ROZETKA_COOKIE", ""),
    },
}

app = FastAPI(title="Telegram Feed Webhook")

API_AUTH_URL = os.environ.get("API_AUTH_URL", "https://aqua-favorit.com.ua/api/auth/").strip()
API_LOGIN = os.environ.get("API_LOGIN", "").strip()
API_PASSWORD = os.environ.get("API_PASSWORD", "").strip()

_api_token_cache: dict[str, Any] = {"token": "", "exp_ts": 0.0}


def missing_env() -> list[str]:
    missing: list[str] = []
    if not TG_TOKEN:
        missing.append("TELEGRAM_BOT_TOKEN")
    if not GH_TOKEN:
        missing.append("GH_DISPATCH_TOKEN")
    return missing


def get_api_token() -> str:
    now = time.time()
    cached_token = str(_api_token_cache.get("token") or "")
    exp_ts = float(_api_token_cache.get("exp_ts") or 0.0)
    if cached_token and now < exp_ts:
        return cached_token

    if not API_AUTH_URL or not API_LOGIN or not API_PASSWORD:
        raise RuntimeError("Missing API auth env: API_AUTH_URL/API_LOGIN/API_PASSWORD")

    payload = {"login": API_LOGIN, "password": API_PASSWORD}
    headers = {"Content-Type": "application/json"}
    resp = requests.post(API_AUTH_URL, json=payload, headers=headers, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    status = (data.get("status") or "").upper()
    token = str(((data.get("response") or {}).get("token") or "")).strip()
    if status != "OK" or not token:
        msg = (data.get("response") or {}).get("message") or "unknown auth error"
        raise RuntimeError(f"API auth failed: {msg}")

    # token lifetime is 600s; refresh slightly earlier.
    _api_token_cache["token"] = token
    _api_token_cache["exp_ts"] = now + 540
    return token


def tg_api(method: str, payload: dict[str, Any]) -> dict[str, Any]:
    miss = missing_env()
    if miss:
        raise RuntimeError(f"Missing env: {', '.join(miss)}")
    url = f"https://api.telegram.org/bot{TG_TOKEN}/{method}"
    resp = requests.post(url, data=payload, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    if not data.get("ok"):
        raise RuntimeError(f"Telegram {method} failed: {data}")
    return data


def keyboard() -> str:
    buttons = [
        [{"text": "Обновить MAUDAU", "callback_data": "run:maudau"}],
        [{"text": "Обновить EPICENTER", "callback_data": "run:epicenter"}],
        [{"text": "Обновить HOTLINE", "callback_data": "run_direct:hotline"}],
        [{"text": "Обновить ROZETKA", "callback_data": "run_direct:rozetka_direct"}],
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
    miss = missing_env()
    if miss:
        return False, f"Не заданы переменные окружения: {', '.join(miss)}"

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


def trigger_direct_feed(feed_key: str) -> tuple[bool, str]:
    feed = DIRECT_FEEDS.get(feed_key)
    if not feed:
        return False, "Неизвестная команда"

    try:
        headers: dict[str, str] = {}
        auth = None
        params: dict[str, Any] | None = None
        json_body: dict[str, Any] | None = None

        auth_type = (feed.get("auth_type") or "none").lower()
        if auth_type == "basic":
            auth = (feed.get("basic_user") or "", feed.get("basic_pass") or "")
        elif auth_type == "bearer":
            token = (feed.get("bearer_token") or "").strip()
            if token:
                headers["Authorization"] = f"Bearer {token}"
        elif auth_type == "header":
            name = (feed.get("header_name") or "").strip()
            value = (feed.get("header_value") or "").strip()
            if name and value:
                headers[name] = value
        elif auth_type == "cookie":
            cookie_str = (feed.get("cookie") or "").strip()
            if cookie_str:
                headers["Cookie"] = cookie_str
        elif auth_type == "api":
            token = get_api_token()
            placement = (feed.get("api_token_placement") or "bearer").lower()
            token_key = (feed.get("api_token_key") or "token").strip()

            if placement == "bearer":
                headers["Authorization"] = f"Bearer {token}"
            elif placement == "header":
                headers[token_key or "X-Auth-Token"] = token
            elif placement == "query":
                params = {token_key or "token": token}
            elif placement == "body":
                json_body = {token_key or "token": token}
            else:
                headers["Authorization"] = f"Bearer {token}"

            body_raw = (feed.get("api_body") or "").strip()
            if body_raw:
                try:
                    extra = json.loads(body_raw)
                    if isinstance(extra, dict):
                        json_body = {**(json_body or {}), **extra}
                except Exception:
                    pass

        method = (feed.get("method") or "GET").upper()
        if method == "POST":
            headers.setdefault("Content-Type", "application/json")
            resp = requests.post(feed["url"], headers=headers, auth=auth, params=params, json=json_body, timeout=60)
        else:
            resp = requests.get(feed["url"], headers=headers, auth=auth, params=params, timeout=60)
        if 200 <= resp.status_code < 300:
            return True, f"Запущено: {feed['title']}"
        return False, f"Ошибка запуска {feed['title']}: HTTP {resp.status_code}"
    except Exception as exc:
        return False, f"Ошибка запуска {feed['title']}: {exc}"


def is_allowed_chat(chat_id: str) -> bool:
    if not ALLOWED_CHAT_ID:
        return True
    return str(chat_id) == ALLOWED_CHAT_ID


def normalize_cmd(text: str) -> str:
    cmd = text.lower().strip()
    if cmd.startswith("/"):
        cmd = cmd.split()[0]
        cmd = cmd.split("@", 1)[0]
    else:
        cmd = cmd.split()[0] if cmd else ""
    return cmd


@app.get("/health")
def health() -> dict[str, str]:
    miss = missing_env()
    if miss:
        return {"status": "degraded", "missing_env": ", ".join(miss)}
    return {"status": "ok"}


@app.post("/telegram/webhook")
async def telegram_webhook(
    request: Request,
    x_telegram_bot_api_secret_token: str | None = Header(default=None),
) -> dict[str, bool]:
    if WEBHOOK_SECRET:
        if x_telegram_bot_api_secret_token != WEBHOOK_SECRET:
            raise HTTPException(status_code=403, detail="invalid secret")

    update = await request.json()

    if "message" in update:
        message = update.get("message") or {}
        text = (message.get("text") or "").strip()
        chat_id = str(((message.get("chat") or {}).get("id", "")))

        if chat_id:
            if not is_allowed_chat(chat_id):
                tg_api("sendMessage", {"chat_id": chat_id, "text": f"Нет доступа. chat_id={chat_id}"})
                return {"ok": True}

            cmd = normalize_cmd(text)
            if cmd in {"/start", "/feeds", "/update", "start", "feeds", "update"}:
                send_controls(chat_id)

    elif "callback_query" in update:
        cq = update.get("callback_query") or {}
        callback_id = cq.get("id", "")
        data = (cq.get("data") or "").strip()
        message = cq.get("message") or {}
        chat_id = str(((message.get("chat") or {}).get("id", "")))

        if callback_id:
            if not chat_id or not is_allowed_chat(chat_id):
                answer_callback(callback_id, "Нет доступа")
                return {"ok": True}

            if not data.startswith("run:") and not data.startswith("run_direct:"):
                answer_callback(callback_id, "Неизвестная команда")
                return {"ok": True}

            prefix, feed_key = data.split(":", 1)
            if prefix == "run":
                ok, text = dispatch_workflow(feed_key)
            else:
                ok, text = trigger_direct_feed(feed_key)

            answer_callback(callback_id, text if ok else "Ошибка запуска")
            tg_api("sendMessage", {"chat_id": chat_id, "text": text})

    return {"ok": True}
