import sys
import requests
from confluent_kafka import Producer
from typing import Any, Dict, List

from config import Config
from offer_filter import filter_top_offers_by_popularity
from utils import format_kafka_message, setup_logger

logger = setup_logger(__name__)


def search_product_ids(query: str, limit: int) -> List[str]:
    """
    Resolve product IDs via GET /products/search (see PricesAPI docs).
    """
    url = f"{Config.PRICES_API_BASE_URL}/products/search"
    cap = max(1, min(limit, 100))
    params = {
        "q": query,
        "limit": cap,
        "api_key": Config.PRICES_API_KEY,
    }
    logger.info("Searching products: q=%r limit=%s", query, cap)
    response = requests.get(url, params=params, timeout=60)
    if response.status_code != 200:
        logger.error("Search HTTP %s: %s", response.status_code, response.text)
        raise RuntimeError(
            f"PricesAPI search HTTP {response.status_code} (check query and API key)"
        )
    body = response.json()
    if not body.get("success"):
        raise RuntimeError("PricesAPI search returned success=false")
    results = (body.get("data") or {}).get("results") or []
    ids: List[str] = []
    for row in results:
        pid = row.get("id")
        title = row.get("title", "")
        if pid is None:
            continue
        sid = str(pid)
        ids.append(sid)
        logger.info("  id=%s title=%s", sid, title[:80] + ("…" if len(title) > 80 else ""))
    if not ids:
        raise RuntimeError("Search returned no products; try a different SEARCH_QUERY")
    return ids


def fetch_product_prices(product_id: str) -> Dict[str, Any]:
    url = f"{Config.PRICES_API_BASE_URL}/products/{product_id}/offers"
    params = {"country": Config.TARGET_COUNTRY, "api_key": Config.PRICES_API_KEY}
    logger.info("Fetching data for product ID: %s", product_id)
    response = requests.get(url, params=params, timeout=60)
    if response.status_code == 200:
        return response.json()
    logger.error(
        "Error fetching data: HTTP %s - %s",
        response.status_code,
        response.text,
    )
    raise RuntimeError(
        f"PricesAPI HTTP {response.status_code} for product {product_id} "
        "(check country and product ID)"
    )


def parse_eventhub_broker(connection_string: str) -> str:
    for part in connection_string.split(";"):
        if part.startswith("Endpoint=sb://"):
            broker = part.replace("Endpoint=sb://", "").rstrip("/")
            if broker:
                return broker
    raise ValueError(
        "EVENTHUB_CONNECTION_STRING is invalid: could not extract Event Hubs broker"
    )


def create_kafka_producer() -> Producer:
    # Azure Event Hubs uses the exact same Kafka Producer library!
    # We just parse the connection string to find the broker.

    conn_str = Config.EVENTHUB_CONNECTION_STRING
    broker = parse_eventhub_broker(conn_str)

    conf: dict[str, str] = {
        "bootstrap.servers": f"{broker}:9093",
        "security.protocol": "SASL_SSL",
        "sasl.mechanisms": "PLAIN",
        "sasl.username": "$ConnectionString",  # Azure Event Hubs literally uses the string "$ConnectionString" as the username
        "sasl.password": conn_str,             # The password is the full connection string
        "client.id": "prices-api-producer",
    }
    return Producer(conf)


def delivery_report(err: Any, msg: Any) -> None:
    if err is not None:
        logger.error("Message delivery failed: %s", err)
    else:
        logger.info("Message delivered to %s [%s]", msg.topic(), msg.partition())


def main() -> None:
    try:
        Config.validate()
    except ValueError as e:
        logger.error("Configuration error: %s", e)
        sys.exit(1)

    producer = create_kafka_producer()

    if Config.SEARCH_QUERY:
        product_ids = search_product_ids(
            Config.SEARCH_QUERY,
            Config.SEARCH_PRODUCT_LIMIT,
        )
        logger.info(
            "Using %s product(s) from SEARCH_QUERY (TARGET_PRODUCT_IDS ignored)",
            len(product_ids),
        )
    else:
        product_ids = list(Config.TARGET_PRODUCT_IDS)

    for product_id in product_ids:
        try:
            data = fetch_product_prices(product_id)
            if data and data.get("success"):
                raw_block = data.get("data") or {}
                filtered = filter_top_offers_by_popularity(
                    raw_block,
                    Config.TOP_OFFERS_LIMIT,
                )
                payload = format_kafka_message(product_id, filtered)
                logger.info(
                    "Publishing product %s to Event Hub %s",
                    product_id,
                    Config.EVENTHUB_NAME,
                )
                producer.produce(
                    topic=Config.EVENTHUB_NAME,
                    key=str(product_id),
                    value=payload,
                    callback=delivery_report,
                )
                producer.poll(0)
            else:
                logger.warning("No successful data for product ID %s", product_id)
        except Exception as exc:
            logger.error("Failed product ID %s: %s", product_id, exc)

    producer.flush()
    logger.info("Finished fetching and publishing.")


if __name__ == "__main__":
    main()
