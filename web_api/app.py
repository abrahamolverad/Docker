from fastapi import FastAPI
from pymongo import MongoClient
import os

app = FastAPI()

# use the same Mongo URI you set in .env (compose injects it)
MONGO = MongoClient(os.getenv("MONGODB_URI", "mongodb://db:27017/rokai"))
db = MONGO["rokai"]

@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/docs-test")
def docs_test():
    # simple round-trip to Mongo
    return {"collections": db.list_collection_names()}
