#!/usr/bin/env python3
import argparse
import os
import requests


def main() -> int:
    parser = argparse.ArgumentParser(description="Set Telegram webhook URL")
    parser.add_argument("--url", required=True, help="Public webhook URL, e.g. https://example.com/telegram/webhook")
    parser.add_argument("--secret", default=os.environ.get("TELEGRAM_WEBHOOK_SECRET", ""), help="Optional secret token")
    parser.add_argument("--drop-pending", action="store_true", help="Drop pending updates")
    args = parser.parse_args()

    token = os.environ.get("TELEGRAM_BOT_TOKEN", "")
    if not token:
        raise SystemExit("TELEGRAM_BOT_TOKEN is required")

    payload = {
        "url": args.url,
        "allowed_updates": ["message", "callback_query"],
        "drop_pending_updates": args.drop_pending,
    }
    if args.secret:
        payload["secret_token"] = args.secret

    resp = requests.post(f"https://api.telegram.org/bot{token}/setWebhook", json=payload, timeout=30)
    resp.raise_for_status()
    print(resp.text)

    info = requests.get(f"https://api.telegram.org/bot{token}/getWebhookInfo", timeout=30)
    info.raise_for_status()
    print(info.text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
