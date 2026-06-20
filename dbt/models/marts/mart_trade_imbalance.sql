-- Taker-side trade (order-flow) imbalance per symbol per minute.
--
-- imbalance = (buy_volume - sell_volume) / (buy_volume + sell_volume)
--   +1 => all aggressive buying, -1 => all aggressive selling, 0 => balanced.
-- A trailing-window smoothed version reduces single-minute noise.

{% set w = var('imbalance_window_minutes') %}

with bars as (
    select * from {{ ref('int_minute_bars') }}
)

select
    symbol,
    minute,
    buy_volume,
    sell_volume,
    volume,
    trade_count,

    safe_divide(buy_volume - sell_volume, buy_volume + sell_volume) as imbalance,
    safe_divide(buy_volume, volume)                                 as buy_ratio,

    {{ w }}                                                         as window_minutes,
    safe_divide(
        sum(buy_volume)  over w - sum(sell_volume) over w,
        sum(buy_volume)  over w + sum(sell_volume) over w
    )                                                               as imbalance_smoothed
from bars
window w as (
    partition by symbol
    order by minute
    rows between {{ w - 1 }} preceding and current row
)
