-- Rolling realized volatility from 1-minute log returns.
--
-- realized_variance = sum of squared 1-min log returns over a trailing window
-- realized_vol      = sqrt(realized_variance)            (vol over the window)
-- annualized_vol    = realized_vol * sqrt(min_per_year / window)   (24/7 market)

{% set w = var('vol_window_minutes') %}

with bars as (
    select * from {{ ref('int_minute_bars') }}
),

returns as (
    select
        symbol,
        minute,
        close,
        volume,
        -- Log return vs the previous minute's close (null on the first minute).
        safe.ln(
            safe_divide(close, lag(close) over (partition by symbol order by minute))
        ) as log_return
    from bars
),

windowed as (
    select
        symbol,
        minute,
        close,
        volume,
        log_return,
        count(log_return) over w        as obs_in_window,
        sum(pow(log_return, 2)) over w  as realized_variance
    from returns
    window w as (
        partition by symbol
        order by minute
        rows between {{ w - 1 }} preceding and current row
    )
)

select
    symbol,
    minute,
    close,
    volume,
    log_return,
    obs_in_window,
    {{ w }}                                                       as window_minutes,
    realized_variance,
    sqrt(realized_variance)                                       as realized_vol,
    -- Annualized, expressed as a percentage.
    sqrt(realized_variance) * sqrt({{ var('minutes_per_year') }} / {{ w }}) * 100
                                                                 as annualized_vol_pct
from windowed
