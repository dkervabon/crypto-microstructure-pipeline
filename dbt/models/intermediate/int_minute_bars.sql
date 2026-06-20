-- 1-minute OHLCV bars per symbol, plus the aggregates every mart builds on:
-- minute VWAP, quote volume, and the buy/sell taker-volume split.
-- Ephemeral: inlined as a CTE into each mart (no standalone object created).

with ticks as (
    select * from {{ ref('stg_ticks') }}
)

select
    symbol,
    timestamp_trunc(trade_ts, minute)              as minute,

    -- OHLC (MIN_BY/MAX_BY pick the price at the first/last trade in the minute).
    min_by(price, trade_ts)                        as open,
    max(price)                                     as high,
    min(price)                                     as low,
    max_by(price, trade_ts)                        as close,

    sum(quantity)                                  as volume,
    sum(buy_quantity)                              as buy_volume,
    sum(sell_quantity)                             as sell_volume,
    sum(price_volume)                              as quote_volume,
    safe_divide(sum(price_volume), sum(quantity))  as vwap,
    count(*)                                       as trade_count
from ticks
group by symbol, minute
