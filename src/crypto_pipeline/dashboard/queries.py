"""BigQuery access for the dashboard.

A single parameterized query joins the three marts on (symbol, minute) so each
refresh is one BigQuery scan that powers every chart and KPI. Results are cached
briefly to avoid re-querying when multiple callbacks fire on the same interval.
"""

from __future__ import annotations

import time

import pandas as pd
from google.cloud import bigquery

from crypto_pipeline.bq import get_client
from crypto_pipeline.config import settings

_client: bigquery.Client | None = None
_cache: dict[int, tuple[float, pd.DataFrame]] = {}
_CACHE_TTL_SECONDS = 5.0


def client() -> bigquery.Client:
    global _client
    if _client is None:
        _client = get_client()
    return _client


def _marts_ref() -> str:
    return f"{settings.gcp_project_id}.{settings.bq_marts_dataset}"


def load_metrics(lookback_minutes: int = 180) -> pd.DataFrame:
    """Per-(symbol, minute) frame joining VWAP, volatility and imbalance marts.

    Cached for a few seconds so the interval's multiple chart callbacks share
    one BigQuery scan.
    """
    now = time.monotonic()
    hit = _cache.get(lookback_minutes)
    if hit and (now - hit[0]) < _CACHE_TTL_SECONDS:
        return hit[1]

    m = _marts_ref()
    sql = f"""
    SELECT
        v.symbol,
        v.minute,
        v.close,
        v.minute_vwap,
        v.session_vwap,
        v.volume,
        v.quote_volume,
        v.trade_count,
        rv.annualized_vol_pct,
        rv.realized_vol,
        rv.obs_in_window,
        ti.imbalance,
        ti.imbalance_smoothed,
        ti.buy_volume,
        ti.sell_volume
    FROM `{m}.mart_vwap` v
    LEFT JOIN `{m}.mart_realized_volatility` rv USING (symbol, minute)
    LEFT JOIN `{m}.mart_trade_imbalance`     ti USING (symbol, minute)
    WHERE v.minute >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @mins MINUTE)
    ORDER BY v.symbol, v.minute
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("mins", "INT64", lookback_minutes)]
    )
    df = client().query(sql, job_config=job_config).to_dataframe(create_bqstorage_client=False)
    _cache[lookback_minutes] = (now, df)
    return df


def latest_per_symbol(df: pd.DataFrame) -> pd.DataFrame:
    """Most recent row per symbol (for the KPI cards)."""
    if df.empty:
        return df
    return df.sort_values("minute").groupby("symbol", as_index=False).tail(1)
