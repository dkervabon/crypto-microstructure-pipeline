-- VWAP per symbol per minute, plus a running session VWAP (cumulative within
-- each UTC day) — the canonical execution-quality benchmark.

with bars as (
    select * from {{ ref('int_minute_bars') }}
)

select
    symbol,
    minute,
    open,
    high,
    low,
    close,
    volume,
    quote_volume,
    trade_count,
    vwap                                                       as minute_vwap,

    -- Cumulative (session) VWAP: total quote volume / total base volume so far
    -- today, per symbol.
    safe_divide(
        sum(quote_volume) over (partition by symbol, date(minute) order by minute),
        sum(volume)       over (partition by symbol, date(minute) order by minute)
    )                                                          as session_vwap
from bars
