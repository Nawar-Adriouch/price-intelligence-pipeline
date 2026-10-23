import logging
import time
from decimal import Decimal, ROUND_HALF_UP

import requests

log = logging.getLogger(__name__)
RETRYABLE = {429, 500, 502, 503, 504}


def http_get_json(url, params=None, retries=3, timeout=10):
    """GET avec timeout et retries à délai croissant (backoff exponentiel)."""
    for attempt in range(1, retries + 1):
        try:
            r = requests.get(url, params=params, timeout=timeout)
            if r.status_code in RETRYABLE:
                raise requests.HTTPError(f"HTTP {r.status_code}", response=r)
            r.raise_for_status()
            return r.json()
        except (requests.ConnectionError, requests.Timeout, requests.HTTPError) as exc:
            status = getattr(getattr(exc, "response", None), "status_code", None)
            if (status is not None and status not in RETRYABLE) or attempt == retries:
                raise
            wait = 2 ** attempt
            log.warning("Tentative %s/%s échouée (%s), nouvel essai dans %ss",
                        attempt, retries, exc, wait)
            time.sleep(wait)


def _money(value):
    """Montant en Decimal à 2 décimales (jamais de float pour l'argent)."""
    return Decimal(str(value)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


# ---------- Adaptateurs : un par source, même sortie ----------

def normalize_dummyjson(item, currency="USD"):
    list_price = _money(item["price"])
    discount = Decimal(str(item.get("discountPercentage", 0)))
    final = _money(list_price * (1 - discount / 100))
    return {
        "external_id": str(item["id"]),
        "title": item["title"],
        "list_price": list_price,
        "discount_pct": discount,
        "price": final,
        "currency": currency,
    }


def normalize_fakestore(item, currency="USD"):
    price = _money(item["price"])
    return {
        "external_id": str(item["id"]),
        "title": item["title"],
        "list_price": price,
        "discount_pct": Decimal("0"),
        "price": price,                 # pas de remise dans cette API
        "currency": currency,
    }


def normalize_platzi(item, currency="USD"):
    price = _money(item["price"])
    return {
        "external_id": str(item["id"]),
        "title": item["title"],
        "list_price": price,
        "discount_pct": Decimal("0"),
        "price": price,
        "currency": currency,
    }


# Le nom doit être identique à dim_source.name
SOURCES = {
    "dummyjson": normalize_dummyjson,
    "fakestoreapi": normalize_fakestore,
    "platzi": normalize_platzi,
}


def normalize_payload(source_name, payload):
    return SOURCES[source_name](payload)


def fetch_source(source_name, base_url, external_ids):
    """Retourne (payloads_bruts, lignes_normalisées) pour les produits demandés."""
    normalize = SOURCES[source_name]
    raw, rows = [], []
    for ext_id in external_ids:
        payload = http_get_json(f"{base_url}/products/{ext_id}")
        raw.append(payload)
        rows.append(normalize(payload))
    return raw, rows


def fetch_dummyjson(base_url, external_ids):
    """Raccourci conservé pour run_local.py et l'ancien DAG."""
    return fetch_source("dummyjson", base_url, external_ids)