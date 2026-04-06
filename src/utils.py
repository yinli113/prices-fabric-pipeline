import json
import logging
import datetime
from typing import Any, Dict


def setup_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    if not logger.handlers:
        logger.setLevel(logging.INFO)
        handler = logging.StreamHandler()
        handler.setFormatter(
            logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        )
        logger.addHandler(handler)
    return logger


def format_kafka_message(product_id: str, data: Dict[str, Any]) -> str:
    # Inject the exact fetch time directly into the data payload
    # This prevents us from ever relying on database "hidden metadata" timestamps again
    data["fetched_at"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
    return json.dumps({
        "product_id": product_id, 
        "data": data
    })
