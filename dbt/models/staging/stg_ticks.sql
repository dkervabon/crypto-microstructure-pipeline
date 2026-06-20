-- Cleaned, typed tick stream. Filters out malformed rows and pre-computes the
-- per-tick quantities the downstream marts aggregate (signed/quote/buy/sell).

with source as (
    select * from {{ source('raw', 'ticks') }}
)

select
    symbol,
    trade_id,
    price,
    quantity,
    quote_quantity,
    taker_side,
    is_buyer_maker,
    trade_ts,
    ingest_ts,

    -- VWAP numerator contribution.
    price * quantity                                              as price_volume,
    -- Taker-side split drives trade imbalance.
    if(taker_side = 'buy',  quantity, 0)                          as buy_quantity,
    if(taker_side = 'sell', quantity, 0)                          as sell_quantity
from source
where price > 0
  and quantity > 0
  and trade_ts is not null
