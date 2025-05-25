import os, time, json, logging, schedule, datetime
import pymongo
from alpaca.trading.client import TradingClient
from alpaca.trading.requests import MarketOrderRequest
from alpaca.trading.enums import OrderSide, TimeInForce

ROLE  = os.getenv("ROLE", "day")  # day | swing | options
API_KEY     = os.getenv("ALPACA_API_KEY")
API_SECRET  = os.getenv("ALPACA_SECRET_KEY")
MONGO_URI   = os.getenv("MONGODB_URI")
ACCOUNT_TAG = os.getenv("ACCOUNT_TAG", "ACC1")   # for logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s ["+ROLE+"] %(message)s")
mongo = pymongo.MongoClient(MONGO_URI)
db    = mongo["rokai"]
alpaca = TradingClient(API_KEY, API_SECRET, paper=True)

def latest_playbook() -> dict:
    pb = db["signals.playbooks"].find_one(sort=[("_id",-1)]) or {}
    return pb.get("playbooks", {})

def run_once():
    pb = latest_playbook()
    my_cfg = pb.get(ROLE, {"enabled": False})
    if not my_cfg.get("enabled"):
        logging.info("disabled in playbook")
        return

    # --- placeholder: demo buy 1 share of SPY ---
    try:
        order = alpaca.submit_order(
            MarketOrderRequest(symbol="SPY",
                               qty=1,
                               side=OrderSide.BUY,
                               time_in_force=TimeInForce.DAY))
        logging.info("Placed order %s", order.id)
        db.trades.insert_one({"role": ROLE, "order": order.id,
                              "timestamp": datetime.datetime.utcnow()})
    except Exception as e:
        logging.warning("order failed: %s", e)

schedule.every(15).seconds.do(run_once)

if __name__ == "__main__":
    logging.info("Executor started")
    while True:
        schedule.run_pending()
        time.sleep(1)
