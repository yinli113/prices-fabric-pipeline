# PricesAPI → Azure Event Hubs → Microsoft Fabric

1. `cp .env.example .env` and add your **PricesAPI** and **Azure Event Hubs** credentials (never commit `.env`).
2. `pip install -r requirements.txt`
3. `python src/fetch_and_publish.py`

## Finding product IDs (multi‑iPhone / realistic catalog)

PricesAPI documents **`GET /api/v1/products/search?q=...&limit=...`** — each hit includes an **`id`** you can pass to **`GET /api/v1/products/:id/offers`**. See [PricesAPI docs](https://pricesapi.io/docs).

This repo supports **search-driven runs** so you do not hand-pick IDs:

| Variable | Meaning |
|----------|--------|
| `SEARCH_QUERY` | e.g. `apple iphone` — discovers products from search |
| `SEARCH_PRODUCT_LIMIT` | How many search results to fetch offers for (max 100; default 5) |

When `SEARCH_QUERY` is non-empty, **`TARGET_PRODUCT_IDS` is ignored** for that run. Each run uses **1 search call + N offers calls** (watch your monthly API quota).

Alternatively leave `SEARCH_QUERY` empty and set **`TARGET_PRODUCT_IDS`** to comma-separated IDs (no spaces required). The project default is **`3669,3671,3673`** for AU; remove any ID that returns **404**, or add more the same way.

**Azure Event Hubs:** Create an Event Hub named `competition-prices` (or your `EVENTHUB_NAME`).

If keys were ever committed, rotate them in PricesAPI and Azure.
