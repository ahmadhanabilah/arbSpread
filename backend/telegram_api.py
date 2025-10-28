import os
import aiohttp
import asyncio
from dotenv import load_dotenv

load_dotenv()

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID")


async def send_telegram_message(message: str):
    """
    Send a plain text message to a Telegram chat.
    """
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        raise RuntimeError("TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID must be set in .env")

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": "html"
    }

    async with aiohttp.ClientSession() as session:
        async with session.post(url, json=payload) as resp:
            if resp.status != 200:
                error_text = await resp.text()
                raise RuntimeError(f"Telegram send failed: {resp.status} {error_text}")


def format_position_info(pos: dict, name: str) -> str:
    """
    Format inventory/position details for Telegram message.
    Example:
    ⚡ Lighter = 1.234 @ 27350.00
    """
    qty = pos.get("position", 0)
    price = pos.get("avg_entry_price", 0)
    return f"{name} = {qty:.4f} @ {price:.2f}"
