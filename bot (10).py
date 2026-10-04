#!/usr/bin/env python3
"""Monitor Tazkarti matches and publish new alerts to a Telegram newsletter.

The service intentionally uses Python's standard library only, so it can run
in a clean Replit workspace without installing Flask or requests.
"""

from __future__ import annotations

import html
import json
import logging
import os
import signal
import threading
import time
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo


MATCHES_URL = os.getenv(
    "TAZKARTI_MATCHES_URL",
    "https://www.tazkarti.com/data/matches-list-json.json",
)
TICKETS_URL = os.getenv(
    "TAZKARTI_TICKETS_URL",
    "https://www.tazkarti.com/data/TicketPrice-AvailableSeats-{}.json",
)
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()
TELEGRAM_THREAD_ID = os.getenv("TELEGRAM_THREAD_ID", "").strip()
TELEGRAM_COMMANDS_ENABLED = os.getenv(
    "TELEGRAM_COMMANDS_ENABLED", "true"
).casefold() not in {"0", "false", "no", "off"}
TELEGRAM_POLL_TIMEOUT_SECONDS = max(
    5, min(50, int(os.getenv("TELEGRAM_POLL_TIMEOUT_SECONDS", "20")))
)
CHECK_INTERVAL_SECONDS = max(
    10, int(os.getenv("CHECK_INTERVAL_SECONDS", "60"))
)
HEALTH_PORT = int(os.getenv("PORT", "8000"))
REQUEST_TIMEOUT_SECONDS = max(
    5, int(os.getenv("REQUEST_TIMEOUT_SECONDS", "20"))
)
SEEN_MATCHES_FILE = os.getenv(
    "SEEN_MATCHES_FILE", ".tazkarti_seen_matches.json"
)

TARGET_TEAM_IDS = {
    int(value.strip())
    for value in os.getenv("TARGET_TEAM_IDS", "77,79").split(",")
    if value.strip().isdigit()
}
TARGET_TEAM_KEYWORDS = tuple(
    value.strip().casefold()
    for value in os.getenv(
        "TARGET_TEAM_KEYWORDS", "الأهلي,الاهلي,الزمالك,زمالك"
    ).split(",")
    if value.strip()
)

TEAM_NAMES = {
    77: "النادي الأهلي",
    79: "نادي الزمالك",
    171: "نادي البنك الأهلي",
    172: "طلائع الجيش",
    173: "سموحة",
    174: "فاركو",
    175: "الاتحاد السكندري",
    176: "مودرن سبورت",
    177: "المقاولون العرب",
    178: "الجونة",
    180: "بيراميدز",
    181: "إنبي",
    182: "الإسماعيلي",
    183: "سيراميكا كليوباترا",
    184: "غزل المحلة",
    186: "المصري",
    224: "حرس الحدود",
    290: "نادي زد",
    310: "بتروجت",
    385: "كهرباء الإسماعيلية",
    386: "وادي دجلة",
    393: "نادي مسار",
    396: "غابورون يونايتد",
}

STADIUM_NAMES = {
    1: "استاد القاهرة الدولي",
    2: "استاد 30 يونيو",
    3: "استاد السلام",
    4: "استاد الإسكندرية",
    5: "استاد السويس",
    6: "استاد الإسماعيلية",
    7: "استاد القاهرة الدولي",
    8: "صالة المدينة الرياضية بالعاصمة الإدارية الجديدة",
    9: "صالة حسن مصطفى (مدينة 6 أكتوبر)",
    10: "مجمع الصالات باستاد القاهرة",
    13: "استاد المحلة",
    14: "استاد برج العرب",
    15: "استاد المقاولون العرب",
    16: "استاد خالد بشارة",
    17: "استاد بتروسبورت",
    18: "استاد الكلية الحربية",
    20: "صالة حسن مصطفى",
    21: "استاد القاهرة",
    23: "استاد هيئة قناة السويس",
    25: "استاد أسوان",
    26: "استاد حرس الحدود",
    29: "لم يحدد بعد",
    46: "استاد مصر بالعاصمة الإدارية",
    49: "استاد القاهرة الدولي",
}

LOGGER = logging.getLogger("tazkarti-newsletter")
STOP_EVENT = threading.Event()
STATE_LOCK = threading.Lock()
STATE: dict[str, Any] = {
    "started_at": datetime.now(timezone.utc).isoformat(),
    "last_check_at": None,
    "last_success_at": None,
    "last_error": None,
    "checks": 0,
    "matches_sent": 0,
}


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def json_request(url: str) -> Any:
    request = Request(
        url,
        headers={
            "Accept": "application/json, text/plain, */*",
            "User-Agent": "TazkartiNewsletterMonitor/1.0",
            "Cache-Control": "no-cache",
        },
    )
    with urlopen(request, timeout=REQUEST_TIMEOUT_SECONDS) as response:
        if response.status != 200:
            raise RuntimeError(f"HTTP {response.status} from {url}")
        return json.loads(response.read().decode("utf-8"))


def nested_values(value: Any) -> list[Any]:
    """Flatten nested JSON values for tolerant API field extraction."""
    if isinstance(value, dict):
        values: list[Any] = list(value.values())
        for child in value.values():
            values.extend(nested_values(child))
        return values
    if isinstance(value, list):
        values = list(value)
        for child in value:
            values.extend(nested_values(child))
        return values
    return [value]


def int_value(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def contains_target_team(match: dict[str, Any]) -> bool:
    team_id_keys = {
        "teamid",
        "teamid1",
        "teamid2",
        "hometeamid",
        "awayteamid",
        "homeid",
        "awayid",
        "firstteamid",
        "secondteamid",
    }
    for key, value in match.items():
        normalized_key = str(key).replace("_", "").casefold()
        if normalized_key in team_id_keys and int_value(value) in TARGET_TEAM_IDS:
            return True

        if isinstance(value, dict):
            if contains_target_team(value):
                return True
        elif isinstance(value, list):
            if any(
                isinstance(child, dict) and contains_target_team(child)
                for child in value
            ):
                return True

    title = match_title(match).casefold()
    return any(keyword in title for keyword in TARGET_TEAM_KEYWORDS)


def first_value(data: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        value = data.get(key)
        if value not in (None, ""):
            return value
    return ""


def match_title(match: dict[str, Any]) -> str:
    return str(
        first_value(match, "matchTitle", "name", "title", "matchName")
        or "مباراة غير معروفة"
    )


def match_id(match: dict[str, Any]) -> str:
    return str(first_value(match, "id", "matchId", "matchID") or "").strip()


def extract_team_names(match: dict[str, Any]) -> list[str]:
    numbered_names = [
        str(
            first_value(
                match,
                "teamNameAr1",
                "teamName1",
                "teamNameFr1",
            )
            or TEAM_NAMES.get(int_value(match.get("teamId1")) or -1)
            or ""
        ).strip(),
        str(
            first_value(
                match,
                "teamNameAr2",
                "teamName2",
                "teamNameFr2",
            )
            or TEAM_NAMES.get(int_value(match.get("teamId2")) or -1)
            or ""
        ).strip(),
    ]
    numbered_names = list(dict.fromkeys(name for name in numbered_names if name))
    if len(numbered_names) >= 2:
        return numbered_names

    names: list[str] = []

    def visit(value: Any, key: str = "") -> None:
        normalized_key = key.replace("_", "").casefold()
        if normalized_key in {
            "teamid",
            "teamid1",
            "teamid2",
            "hometeamid",
            "awayteamid",
            "homeid",
            "awayid",
            "firstteamid",
            "secondteamid",
        }:
            team_name = TEAM_NAMES.get(int_value(value) or -1)
            if team_name and team_name not in names:
                names.append(team_name)
        elif normalized_key in {
            "teamname",
            "teamname1",
            "teamname2",
            "teamnamear1",
            "teamnamear2",
            "teamnamefr1",
            "teamnamefr2",
            "hometeamname",
            "awayteamname",
            "firstteamname",
            "secondteamname",
        } and value:
            text = str(value).strip()
            if text and text not in names:
                names.append(text)

        if isinstance(value, dict):
            for child_key, child_value in value.items():
                visit(child_value, str(child_key))
        elif isinstance(value, list):
            for child in value:
                visit(child, key)

    visit(match)
    return names


ARABIC_MONTHS = (
    "يناير",
    "فبراير",
    "مارس",
    "أبريل",
    "مايو",
    "يونيو",
    "يوليو",
    "أغسطس",
    "سبتمبر",
    "أكتوبر",
    "نوفمبر",
    "ديسمبر",
)


def format_date(value: Any) -> str:
    if not value:
        return ""
    text = str(value).strip()
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        if parsed.tzinfo is not None:
            parsed = parsed.astimezone(ZoneInfo("Africa/Cairo"))
        hour = parsed.hour % 12 or 12
        period = "صباحًا" if parsed.hour < 12 else "مساءً"
        return (
            f"{parsed.day} {ARABIC_MONTHS[parsed.month - 1]} {parsed.year}"
            f" — {hour:02d}:{parsed.minute:02d} {period}"
        )
    except ValueError:
        return text


def get_ticket_details(match_identifier: str) -> list[dict[str, Any]]:
    if not match_identifier:
        return []

    cache_buster = int(time.time() * 1000)
    url = f"{TICKETS_URL.format(match_identifier)}?_={cache_buster}"
    try:
        data = json_request(url)
        if isinstance(data, dict):
            payload = data.get("data", data)
            return payload if isinstance(payload, list) else []
        return data if isinstance(data, list) else []
    except (HTTPError, URLError, TimeoutError, RuntimeError, json.JSONDecodeError):
        LOGGER.info("Ticket details unavailable for match %s", match_identifier)
        return []


def available_tickets(match_identifier: str) -> list[str]:
    results: list[str] = []
    for ticket in get_ticket_details(match_identifier):
        team_id = int_value(
            first_value(ticket, "teamId", "teamID", "team_id")
        )
        if team_id not in TARGET_TEAM_IDS or ticket.get("soldOut") is True:
            continue
        category = str(
            first_value(ticket, "categoryNameAr", "categoryName", "name")
            or "فئة غير معروفة"
        )
        price = first_value(ticket, "price", "ticketPrice")
        price_text = f"{price} ج.م" if price not in (None, "") else "غير محدد"
        results.append(f"{category} — {price_text}")
    return results


def telegram_api_call(
    method: str, payload: dict[str, Any], timeout: int | None = None
) -> dict[str, Any]:
    if not TELEGRAM_BOT_TOKEN:
        raise RuntimeError("TELEGRAM_BOT_TOKEN is not configured")

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/{method}"
    request = Request(
        url,
        data=urlencode(payload).encode("utf-8"),
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        method="POST",
    )
    with urlopen(request, timeout=timeout or REQUEST_TIMEOUT_SECONDS) as response:
        body = json.loads(response.read().decode("utf-8"))
        if response.status != 200 or not body.get("ok"):
            raise RuntimeError(f"Telegram API rejected {method}")
        return body


def split_telegram_message(message: str) -> list[str]:
    chunks: list[str] = []
    current = ""
    for line in message.splitlines():
        if current and len(current) + len(line) + 1 > 3900:
            chunks.append(current)
            current = ""
        current = f"{current}\n{line}".strip()
    if current:
        chunks.append(current)
    return chunks


def send_telegram_message_to_chat(
    chat_id: Any, message: str, thread_id: str = ""
) -> bool:
    if not TELEGRAM_BOT_TOKEN or not chat_id:
        LOGGER.error(
            "Telegram is not configured. Set TELEGRAM_BOT_TOKEN and a chat ID."
        )
        return False

    for chunk in split_telegram_message(message):
        payload = {
            "chat_id": str(chat_id),
            "text": chunk,
            "parse_mode": "HTML",
            "disable_web_page_preview": "true",
        }
        if thread_id:
            payload["message_thread_id"] = thread_id
        try:
            telegram_api_call("sendMessage", payload)
        except (
            HTTPError,
            URLError,
            TimeoutError,
            RuntimeError,
            json.JSONDecodeError,
        ) as error:
            LOGGER.error("Could not publish to Telegram: %s", error)
            return False

    return True


def telegram_messages(message: str) -> bool:
    return send_telegram_message_to_chat(
        TELEGRAM_CHAT_ID, message, TELEGRAM_THREAD_ID
    )


def telegram_command_loop() -> None:
    """Receive basic bot commands through Telegram long polling."""
    if not TELEGRAM_COMMANDS_ENABLED:
        LOGGER.info("Telegram command listener is disabled.")
        return

    offset: int | None = None
    LOGGER.info("Telegram command listener is enabled.")

    while not STOP_EVENT.is_set():
        payload: dict[str, Any] = {
            "timeout": str(TELEGRAM_POLL_TIMEOUT_SECONDS),
            "allowed_updates": json.dumps(["message"]),
        }
        if offset is not None:
            payload["offset"] = str(offset)

        try:
            response = telegram_api_call(
                "getUpdates",
                payload,
                timeout=TELEGRAM_POLL_TIMEOUT_SECONDS + 10,
            )
            for update in response.get("result", []):
                update_id = int_value(update.get("update_id"))
                if update_id is not None:
                    offset = update_id + 1

                message = update.get("message") or {}
                text = str(message.get("text") or "").strip()
                chat_id = (message.get("chat") or {}).get("id")
                if not text or chat_id is None or not text.startswith("/"):
                    continue

                command = text.split()[0].split("@", 1)[0].casefold()
                with STATE_LOCK:
                    state = dict(STATE)

                if command in {"/start", "/help"}:
                    reply = (
                        "👋 <b>أهلًا بيك في نشرة تذكرتي</b>\n\n"
                        "✨ أنا هتابع لك مباريات الأهلي والزمالك، "
                        "وأبعت لك التنبيه أول ما الحجز يفتح.\n\n"
                        "📌 <b>الأوامر المتاحة</b>\n"
                        "• /status — معرفة حالة المراقبة\n"
                        "• /help — عرض هذه الرسالة"
                    )
                elif command == "/status":
                    status_text = "يعمل بشكل طبيعي" if state["last_success_at"] else "ينتظر أول فحص"
                    last_check = state["last_success_at"] or "لم يتم بعد"
                    reply = (
                        "<b>حالة مراقب تذكرتي</b>\n"
                        f"الحالة: {status_text}\n"
                        f"آخر فحص ناجح: {html.escape(str(last_check))}\n"
                        f"عدد الفحوصات: {state['checks']}\n"
                        f"التنبيهات المنشورة: {state['matches_sent']}"
                    )
                else:
                    continue

                send_telegram_message_to_chat(chat_id, reply)
        except (
            HTTPError,
            URLError,
            TimeoutError,
            RuntimeError,
            json.JSONDecodeError,
        ) as error:
            LOGGER.warning("Telegram command listener retrying: %s", error)
            STOP_EVENT.wait(5)


def load_seen_matches() -> set[str]:
    try:
        with open(SEEN_MATCHES_FILE, encoding="utf-8") as file:
            data = json.load(file)
        return {str(item) for item in data if item}
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return set()


def save_seen_matches(seen: set[str]) -> None:
    try:
        with open(SEEN_MATCHES_FILE, "w", encoding="utf-8") as file:
            json.dump(sorted(seen)[-2000:], file, ensure_ascii=False, indent=2)
    except OSError as error:
        LOGGER.warning("Could not save seen matches: %s", error)


def build_match_message(match: dict[str, Any]) -> str:
    names = extract_team_names(match)
    title = " 🆚 ".join(names) if len(names) >= 2 else match_title(match)
    tournament = str(
        first_value(
            match, "championshipName", "tournamentName", "competitionName"
        )
        or ""
    ).strip()
    if not tournament and isinstance(match.get("tournament"), dict):
        tournament = str(
            first_value(
                match["tournament"],
                "nameAr",
                "name",
                "nameEn",
            )
            or ""
        ).strip()
    round_name = str(first_value(match, "roundName", "round") or "").strip()
    stadium_id = int_value(first_value(match, "stadiumId", "stadiumID"))
    stadium = str(
        first_value(match, "stadiumNameAr", "stadiumName")
        or STADIUM_NAMES.get(stadium_id or -1, "")
    ).strip()
    match_date = format_date(
        first_value(match, "matchDate", "date", "startDate", "kickoff")
    )
    opening = format_date(
        first_value(
            match,
            "doorsOpenTime",
            "gateOpenTime",
            "gatesOpenTime",
            "openTime",
        )
    )
    closing = format_date(
        first_value(
            match,
            "doorsCloseTime",
            "gateCloseTime",
            "gatesCloseTime",
            "closeTime",
            "matchEndTime",
        )
    )

    lines = [
        "🆕 <b>متاح الآن للحجز</b>",
        "",
        f"<b>{html.escape(title)}</b>",
    ]
    if tournament or round_name:
        lines.append(
            ""
        )
        lines.append(
            f"🏅 {html.escape(' — '.join(item for item in (tournament, round_name) if item))}"
        )
    if stadium:
        lines.append("")
        lines.append(f"🏟 {html.escape(stadium)}")
    if opening:
        lines.append("")
        lines.append(f"🚪 فتح البوابات : {html.escape(opening)}")
    if closing:
        lines.append(f"🕐 قفل البوابات : {html.escape(closing)}")

    tickets = available_tickets(match_id(match))
    if tickets:
        lines.append("")
        lines.append("<b>🎟️ التذاكر المتاحة:</b>")
        lines.extend(f"• {html.escape(ticket)}" for ticket in tickets)
    else:
        lines.append("")
        lines.append("🎟️ تفاصيل المقاعد والأسعار تظهر عند فتح الحجز.")

    return "\n".join(lines)


def check_tazkarti(seen_matches: set[str]) -> None:
    now = utc_now()
    with STATE_LOCK:
        STATE["last_check_at"] = now
        STATE["checks"] += 1

    cache_buster = int(time.time() * 1000)
    try:
        data = json_request(f"{MATCHES_URL}?_={cache_buster}")
        matches = data if isinstance(data, list) else data.get("data", [])
        if not isinstance(matches, list):
            raise RuntimeError("Unexpected matches response shape")

        new_matches = [
            match
            for match in matches
            if isinstance(match, dict)
            and match_id(match)
            and contains_target_team(match)
            and match_id(match) not in seen_matches
        ]

        for match in new_matches:
            identifier = match_id(match)
            if telegram_messages(build_match_message(match)):
                seen_matches.add(identifier)
                with STATE_LOCK:
                    STATE["matches_sent"] += 1
                save_seen_matches(seen_matches)
                LOGGER.info("Published new match %s", identifier)

        with STATE_LOCK:
            STATE["last_success_at"] = utc_now()
            STATE["last_error"] = None
    except (
        HTTPError,
        URLError,
        TimeoutError,
        RuntimeError,
        json.JSONDecodeError,
    ) as error:
        with STATE_LOCK:
            STATE["last_error"] = str(error)
        LOGGER.error("Tazkarti check failed: %s", error)


class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:  # noqa: N802
        if self.path not in ("/", "/healthz"):
            self.send_response(404)
            self.end_headers()
            return

        with STATE_LOCK:
            state = dict(STATE)
        healthy = bool(state["last_success_at"]) and not state["last_error"]
        body = {
            "ok": healthy,
            "service": "tazkarti-newsletter-monitor",
            **state,
        }
        encoded = json.dumps(body, ensure_ascii=False).encode("utf-8")
        self.send_response(200 if healthy else 503)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)

    def log_message(self, format: str, *args: Any) -> None:
        return


def start_health_server() -> ThreadingHTTPServer:
    server = ThreadingHTTPServer(("0.0.0.0", HEALTH_PORT), HealthHandler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    LOGGER.info("Health endpoint listening on port %s (/healthz)", HEALTH_PORT)
    return server


def validate_configuration() -> None:
    missing = [
        name
        for name, value in (
            ("TELEGRAM_BOT_TOKEN", TELEGRAM_BOT_TOKEN),
            ("TELEGRAM_CHAT_ID", TELEGRAM_CHAT_ID),
        )
        if not value
    ]
    if missing:
        raise RuntimeError(f"Missing required configuration: {', '.join(missing)}")


def main() -> None:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(message)s",
    )
    validate_configuration()
    seen_matches = load_seen_matches()
    health_server = start_health_server()
    threading.Thread(
        target=telegram_command_loop,
        name="telegram-command-listener",
        daemon=True,
    ).start()

    def stop(*_: Any) -> None:
        LOGGER.info("Stopping monitor.")
        STOP_EVENT.set()

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)
    LOGGER.info(
        "Monitoring Tazkarti every %s seconds for team IDs: %s",
        CHECK_INTERVAL_SECONDS,
        sorted(TARGET_TEAM_IDS),
    )

    try:
        while not STOP_EVENT.is_set():
            check_tazkarti(seen_matches)
            STOP_EVENT.wait(CHECK_INTERVAL_SECONDS)
    finally:
        health_server.shutdown()
        health_server.server_close()


if __name__ == "__main__":
    main()
