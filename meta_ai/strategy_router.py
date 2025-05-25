# strategy_router.py
"""RokAi - AI Playbook Router
Generates daily playbooks (day-trade / swing / options) based on
market regime + scanner stats using OpenAI o4-mini.
"""

import os, json, datetime, time, re, logging
from dateutil import tz
import pymongo
import openai
import yaml

# ───────────────────────────────────────── configuration ──
MONGO_URI = os.getenv(
    "MONGODB_URI",
    "mongodb://rokai:SuperStrong123@db:27017/rokai?authSource=admin",
)
OPENAI_MODEL   = os.getenv("OPENAI_MODEL", "o4-mini")
REFRESH_SEC    = int(os.getenv("ROUTER_REFRESH_SEC", "60"))  # seconds
PROMPT_PATH    = os.path.join(os.path.dirname(__file__), "prompts", "router_prompt.txt")
# ───────────────────────────────────────────────────────────

logging.basicConfig(level=logging.INFO, format="%(asctime)s [Router] %(message)s")

client = pymongo.MongoClient(MONGO_URI)
db     = client["rokai"]

openai.api_key = os.getenv("OPENAI_API_KEY")

with open(PROMPT_PATH, "r", encoding="utf-8") as f:
    PROMPT_TEMPLATE = f.read()

# ───────────────────────────────────────── helpers ──

def latest_regime() -> str:
    """Return the most recent market regime stored by another service."""
    doc = db.market_regime.find_one(sort=[("timestamp", -1)])
    return doc.get("regime", "unknown") if doc else "unknown"

def scanners_stats() -> dict:
    """Lightweight example of pulling stats produced by the scanners."""
    count = db["signals.daytrade_raw"].count_documents({})
    return {"daytrade_signal_count": count}

def call_llm(context: dict) -> dict:
    """Call OpenAI, enforce JSON-only reply, parse with fallback."""
    prompt = PROMPT_TEMPLATE.format(**context)
    resp   = openai.ChatCompletion.create(
        model=OPENAI_MODEL,
        messages=[
            {"role": "system", "content": "You are ROKAI_ROUTER. Follow OUTPUT RULES exactly."},
            {"role": "user",   "content": prompt},
        ],
    )
    raw = resp.choices[0].message.content.strip()
    # remove leading text / markdown fences
    cleaned = re.sub(r"^[^\\{]*", "", raw, count=1).strip("`")

    for parser in (json.loads, yaml.safe_load):
        try:
            obj = parser(cleaned)
            if isinstance(obj, dict):
                return obj
        except Exception:
            continue

    logging.warning("Could not parse LLM response: %s…", raw[:120])
    return {"playbooks": {}, "explanation": "router_failed_to_parse"}

# ───────────────────────────────────────── main loop ──

def main_loop() -> None:
    logging.info("Router started – model=%s refresh=%ss", OPENAI_MODEL, REFRESH_SEC)
    while True:
        context = {
            "date": datetime.datetime.now(tz.tzlocal()).isoformat(),
            "regime": latest_regime(),
            "daytrade_stats": scanners_stats(),
        }
        result = call_llm(context)
        result["generated_at"] = datetime.datetime.utcnow()
        db["signals.playbooks"].insert_one(result)
        logging.info("🚀 new playbook: %s", result.get("playbooks", {}))
        time.sleep(REFRESH_SEC)

if __name__ == "__main__":
    main_loop()
