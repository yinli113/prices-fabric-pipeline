"""Keep only the top-N offers by popularity (reviews, then rating)."""

from __future__ import annotations

from copy import deepcopy
from typing import Any, Dict, List


def _offer_popularity_score(offer: Dict[str, Any]) -> float:
    reviews = float(offer.get("reviewCount") or 0)
    rating = float(offer.get("rating") or 0)
    return reviews * 1_000_000.0 + rating


def filter_top_offers_by_popularity(
    data_block: Dict[str, Any],
    limit: int,
) -> Dict[str, Any]:
    if limit <= 0:
        return deepcopy(data_block)

    out: Dict[str, Any] = deepcopy(data_block)
    offers: List[Dict[str, Any]] = list(out.get("offers") or [])
    if not offers:
        return out

    sorted_offers = sorted(
        offers,
        key=lambda o: _offer_popularity_score(o),
        reverse=True,
    )
    top = sorted_offers[:limit]
    out["offers"] = top
    out["offerCount"] = len(top)
    return out
