import sys
import os
import json
import logging
import datetime
import random
import requests
from typing import Any, Dict

sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

from config import Config
from offer_filter import filter_top_offers_by_popularity
from fetch_and_publish import create_kafka_producer, search_product_ids, fetch_product_prices

logger = logging.getLogger("backfill")
if not logger.handlers:
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))
    logger.addHandler(handler)

def format_fake_kafka_message(product_id: str, data: Dict[str, Any], hours_ago: int) -> str:
    # 1. Create a fake timestamp from the past
    fake_time = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(hours=hours_ago)
    data["fetched_at"] = fake_time.isoformat()
    
    # 2. Add some random noise to the price so the line chart moves up and down!
    if "offers" in data:
        for offer in data["offers"]:
            if "price" in offer and offer["price"]:
                # fluctuate the price by +/- 5% so it looks like a real active market
                fluctuation = random.uniform(0.95, 1.05)
                new_price = round(float(offer["price"]) * fluctuation, 2)
                offer["price"] = new_price

    return json.dumps({
        "product_id": product_id, 
        "data": data
    })

def delivery_report(err: Any, msg: Any) -> None:
    pass # Keep it quiet so we don't flood the terminal

def main() -> None:
    try:
        Config.validate()
    except ValueError as e:
        logger.error("Configuration error: %s", e)
        sys.exit(1)

    producer = create_kafka_producer()

    if Config.SEARCH_QUERY:
        product_ids = search_product_ids(Config.SEARCH_QUERY, Config.SEARCH_PRODUCT_LIMIT)
    else:
        product_ids = list(Config.TARGET_PRODUCT_IDS)

    logger.info("Starting 24-hour backfill for LinkedIn screenshot...")

    for product_id in product_ids:
        try:
            logger.info("Fetching base data for product %s...", product_id)
            data = fetch_product_prices(product_id)
            
            if data and data.get("success"):
                raw_block = data.get("data") or {}
                filtered = filter_top_offers_by_popularity(raw_block, Config.TOP_OFFERS_LIMIT)
                
                # Loop 24 times to generate 24 hours of history!
                for hours_ago in range(24, -1, -1):
                    # We copy the dictionary so we don't accidentally compound the price changes
                    payload = format_fake_kafka_message(product_id, json.loads(json.dumps(filtered)), hours_ago)
                    
                    producer.produce(
                        topic=Config.EVENTHUB_NAME,
                        key=str(product_id),
                        value=payload,
                        callback=delivery_report,
                    )
                logger.info("Successfully pushed 24 hours of fake history for product %s!", product_id)
            else:
                logger.warning("No successful data for product ID %s", product_id)
        except Exception as exc:
            logger.error("Failed product ID %s: %s", product_id, exc)

    logger.info("Flushing to Event Hub...")
    producer.flush()
    logger.info("Done! Wait 30 seconds, then refresh Power BI.")

if __name__ == "__main__":
    main()
