-- Grain guard: every mart must have exactly one row per (symbol, minute).
-- Returns offending rows (test fails if any).

{% set marts = ['mart_vwap', 'mart_realized_volatility', 'mart_trade_imbalance'] %}

{% for m in marts %}
select '{{ m }}' as model, symbol, minute, count(*) as n
from {{ ref(m) }}
group by symbol, minute
having count(*) > 1
{% if not loop.last %}union all{% endif %}
{% endfor %}
