# meta_ai/strategy_router.py
import os
import json
import datetime
import time
import re
import logging
from dateutil import tz # Ensure this is imported if used for tz.tzlocal()
import pymongo
import openai # Keep the import
import yaml

# ───────────────────────────────────────── configuration ──
MONGO_URI = os.getenv(
    "MONGODB_URI",
    "mongodb://rokai:SuperStrong123@db:27017/rokai?authSource=admin",
)
# ---- MODIFICATION START ----
# Read USE_OPENAI flag and OPENAI_API_KEY at the start
USE_OPENAI_FLAG = os.getenv("USE_OPENAI", "false").lower() == "true"
OPENAI_API_KEY  = os.getenv("OPENAI_API_KEY")
OPENAI_MODEL    = os.getenv("OPENAI_MODEL", "o4-mini")
# ---- MODIFICATION END ----
REFRESH_SEC    = int(os.getenv("ROUTER_REFRESH_SEC", "60"))
PROMPT_PATH    = os.path.join(os.path.dirname(__file__), "prompts", "router_prompt.txt")
# ───────────────────────────────────────────────────────────

logging.basicConfig(level=logging.INFO, format="%(asctime)s [Router] %(message)s")

client = pymongo.MongoClient(MONGO_URI)
db     = client["rokai"]

# ---- MODIFICATION START ----
# Set openai.api_key only if the flag is true and key exists
if USE_OPENAI_FLAG and OPENAI_API_KEY:
    openai.api_key = OPENAI_API_KEY
elif USE_OPENAI_FLAG and not OPENAI_API_KEY:
    logging.warning("USE_OPENAI is true, but OPENAI_API_KEY is not set. OpenAI calls will fail.")
# If USE_OPENAI_FLAG is false, openai.api_key remains unset or as its default (None)
# ---- MODIFICATION END ----


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
    # ---- MODIFICATION START ----
    if not USE_OPENAI_FLAG:
        logging.info("OpenAI calls are disabled (USE_OPENAI_FLAG is false). Returning default playbook.")
        return {
            "playbooks": {
                "daytrade": {"enabled": False, "max_positions": 0},
                "swing": {"enabled": False},
                "options": {"enabled": False}
            },
            "explanation": "OpenAI disabled by configuration. Default playbook returned."
        }

    if not openai.api_key: # This will be true if OPENAI_API_KEY was not set in environment
        logging.error("OpenAI API key is not configured. Cannot make API call. Returning default playbook.")
        return {
            "playbooks": {
                "daytrade": {"enabled": False, "max_positions": 0},
                "swing": {"enabled": False},
                "options": {"enabled": False}
            },
            "explanation": "OpenAI API key not configured. Default playbook returned."
        }
    # ---- MODIFICATION END ----

    prompt = PROMPT_TEMPLATE.format(**context)
    try:
        resp   = openai.ChatCompletion.create(
            model=OPENAI_MODEL,
            messages=[
                {"role": "system", "content": "You are ROKAI_ROUTER. Follow OUTPUT RULES exactly."},
                {"role": "user",   "content": prompt},
            ],
        )
        raw = resp.choices[0].message.content.strip()
    # ---- MODIFICATION START ----
    except openai.error.OpenAIError as e: # Catch specific OpenAI errors (like RateLimitError, AuthenticationError)
        logging.error(f"OpenAI API error received in call_llm: {e}")
        return {
            "playbooks": { # Return a default valid structure on error
                "daytrade": {"enabled": False, "max_positions": 0},
                "swing": {"enabled": False},
                "options": {"enabled": False}
            },
            "explanation": f"OpenAI API error: {type(e).__name__}. Default playbook returned."
        }
    except Exception as e: # Catch any other unexpected errors
        logging.error(f"Unexpected error during OpenAI call or initial parsing: {e}")
        return {
            "playbooks": {
                "daytrade": {"enabled": False, "max_positions": 0},
                "swing": {"enabled": False},
                "options": {"enabled": False}
            },
            "explanation": f"Unexpected error during LLM call: {type(e).__name__}. Default playbook returned."
        }
    # ---- MODIFICATION END ----

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
    return {"playbooks": {}, "explanation": "router_failed_to_parse_llm_response"}


# ───────────────────────────────────────── main loop ──

def main_loop() -> None:
    logging.info("Router started – model=%s refresh=%ss", OPENAI_MODEL, REFRESH_SEC)
    while True:
        context = {
            "date": datetime.datetime.now(tz.tzlocal()).isoformat(),
            "regime": latest_regime(),
            "daytrade_stats": scanners_stats(),
        }
        result = call_llm(context) # This will now handle the USE_OPENAI_FLAG
        result["generated_at"] = datetime.datetime.utcnow()
        db["signals.playbooks"].insert_one(result)
        # Use .get for safer access to potentially missing keys in 'result'
        logging.info("🚀 new playbook: %s (Explanation: %s)", 
                     result.get("playbooks", {}), 
                     result.get("explanation", "N/A"))
        time.sleep(REFRESH_SEC)

if __name__ == "__main__":
    main_loop()