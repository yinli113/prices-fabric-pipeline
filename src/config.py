import os
from dotenv import load_dotenv

load_dotenv()


class Config:
    """Application settings loaded from the environment."""

    PRICES_API_KEY: str = os.getenv("PRICES_API_KEY", "")
    PRICES_API_BASE_URL: str = os.getenv(
        "PRICES_API_BASE_URL", "https://api.pricesapi.io/api/v1"
    )

    # Used when SEARCH_QUERY is empty. Comma-separated PricesAPI product IDs.
    # 3669, 3671, 3673 — drop any ID that 404s for your country.
    _DEFAULT_PRODUCT_IDS: str = "3669,3671,3673"
    TARGET_PRODUCT_IDS: list[str] = [
        x.strip()
        for x in os.getenv("TARGET_PRODUCT_IDS", _DEFAULT_PRODUCT_IDS).split(",")
        if x.strip()
    ]
    TARGET_COUNTRY: str = os.getenv("TARGET_COUNTRY", "au")

    # Optional: GET /products/search — discover IDs from a query (e.g. apple iphone).
    # When set, TARGET_PRODUCT_IDS is ignored for the run.
    SEARCH_QUERY: str = os.getenv("SEARCH_QUERY", "").strip()
    SEARCH_PRODUCT_LIMIT: int = int(os.getenv("SEARCH_PRODUCT_LIMIT", "5"))

    TOP_OFFERS_LIMIT: int = int(os.getenv("TOP_OFFERS_LIMIT", "10"))

    # Azure Event Hubs Settings
    EVENTHUB_CONNECTION_STRING: str = os.getenv("EVENTHUB_CONNECTION_STRING", "")
    EVENTHUB_NAME: str = os.getenv("EVENTHUB_NAME", "competition-prices")

    @classmethod
    def validate(cls) -> None:
        missing: list[str] = []
        if not cls.PRICES_API_KEY:
            missing.append("PRICES_API_KEY")
        if not cls.EVENTHUB_CONNECTION_STRING:
            missing.append("EVENTHUB_CONNECTION_STRING")

        if missing:
            raise ValueError(
                f"Missing required environment variables: {', '.join(missing)}"
            )
        if not cls.SEARCH_QUERY and not cls.TARGET_PRODUCT_IDS:
            raise ValueError(
                "Set SEARCH_QUERY (e.g. apple iphone) or TARGET_PRODUCT_IDS "
                "with at least one product ID"
            )
