"""Live-updating Dash dashboard for crypto microstructure metrics.

One BigQuery scan per refresh (cached) lands in a dcc.Store; the KPI cards and
three charts (price/VWAP, realized volatility, trade imbalance) derive from it.

Local:  python -m crypto_pipeline.dashboard.app
Render:  gunicorn wsgi:server
"""

from __future__ import annotations

import os
from datetime import datetime, timezone
from io import StringIO

import numpy as np
import pandas as pd
import plotly.graph_objects as go
from dash import Dash, Input, Output, dcc, html

from crypto_pipeline.config import settings
from crypto_pipeline.dashboard.queries import latest_per_symbol, load_metrics

REFRESH_MS = 15_000  # live refresh cadence
LOOKBACK_OPTIONS = [30, 60, 180, 360, 720]
DEFAULT_LOOKBACK = 180

# Brand-ish colors per asset; fallback palette for anything else.
SYMBOL_COLORS = {"BTCUSDT": "#f7931a", "ETHUSDT": "#627eea", "SOLUSDT": "#14f195"}
_FALLBACK = ["#e91e63", "#00bcd4", "#ffc107", "#9c27b0"]

BG = "#0e1117"
PANEL = "#161b22"
TEXT = "#c9d1d9"
MUTED = "#8b949e"


def _symbols() -> list[str]:
    return [s.upper() for s in settings.symbols]


def _color(symbol: str, i: int = 0) -> str:
    return SYMBOL_COLORS.get(symbol, _FALLBACK[i % len(_FALLBACK)])


app = Dash(__name__, title="Crypto Microstructure", update_title=None)
server = app.server  # WSGI entry point for gunicorn


# --- Layout -----------------------------------------------------------
def _dropdown_style() -> dict:
    return {"backgroundColor": PANEL, "color": "#000", "minWidth": "120px"}


# Per-chart (description, formula) captions rendered above each graph.
CAPTIONS = {
    "price": (
        "Each minute's close vs the session (cumulative) VWAP, in basis points — "
        "above 0 trades rich to the day's volume-weighted price, below 0 cheap.",
        "bps = (close / session_VWAP − 1) × 10,000",
    ),
    "vol": (
        "Annualized realized volatility from 1-minute log returns over a rolling "
        "15-minute window (24/7 market).",
        "σₐₙₙ = √(Σ rₜ²) × √(525,600 / 15) × 100,   rₜ = ln(closeₜ / closeₜ₋₁)",
    ),
    "imbalance": (
        "Taker-side order-flow imbalance, smoothed over 5 minutes. "
        "+1 = all aggressive buying, −1 = all aggressive selling.",
        "imbalance = (buy_vol − sell_vol) / (buy_vol + sell_vol)",
    ),
    "corr": (
        "Pearson correlation of BTC and ETH 1-minute log returns over a rolling "
        "5-minute window.",
        "ρ = corr(r_BTC, r_ETH),   rₜ = ln(Pₜ / Pₜ₋₁)",
    ),
    "cumvol": (
        "Running total of traded notional per symbol over the window. Quote "
        "volume is comparable across assets (unlike base-coin units).",
        "cumulative quote volume = Σ (price × quantity)",
    ),
}


def _caption(key: str) -> html.Div:
    desc, formula = CAPTIONS[key]
    return html.Div(
        style={"marginBottom": "6px"},
        children=[
            html.Div(desc, style={"color": MUTED, "fontSize": "12.5px",
                                  "lineHeight": "1.4"}),
            html.Code(formula, style={
                "display": "inline-block", "marginTop": "5px", "color": "#d2a8ff",
                "backgroundColor": "#0d1117", "border": "1px solid #21262d",
                "borderRadius": "5px", "padding": "3px 9px", "fontSize": "12.5px"}),
        ],
    )


def _chart_block(graph_id: str, caption_key: str) -> html.Div:
    """A caption stacked above its chart, as one flex column."""
    return html.Div(
        style={"flex": "1 1 480px", "display": "flex", "flexDirection": "column"},
        children=[_caption(caption_key),
                  dcc.Graph(id=graph_id, config={"displayModeBar": False})],
    )


app.layout = html.Div(
    style={"backgroundColor": BG, "color": TEXT, "minHeight": "100vh",
           "fontFamily": "Inter, system-ui, sans-serif", "padding": "18px 28px"},
    children=[
        html.Div(
            style={"display": "flex", "alignItems": "baseline", "gap": "16px",
                   "flexWrap": "wrap"},
            children=[
                html.H1("Real-Time Crypto Microstructure",
                        style={"margin": "0", "fontSize": "26px"}),
                html.Span("VWAP · Realized Volatility · Trade Imbalance",
                          style={"color": MUTED, "fontSize": "14px"}),
                html.Span(
                    f"↻ auto-refresh every {REFRESH_MS // 1000}s",
                    style={"marginLeft": "auto", "color": TEXT, "fontSize": "13px",
                           "backgroundColor": PANEL, "padding": "4px 10px",
                           "borderRadius": "6px", "border": "1px solid #21262d"},
                ),
                html.Span(id="last-updated", style={"color": MUTED, "fontSize": "13px"}),
            ],
        ),
        html.Div(
            style={"display": "flex", "gap": "20px", "margin": "16px 0",
                   "alignItems": "center", "flexWrap": "wrap"},
            children=[
                html.Div([
                    html.Label("Symbols", style={"color": MUTED, "fontSize": "12px",
                               "marginRight": "8px"}),
                    dcc.Dropdown(id="symbol-filter", options=_symbols(),
                                 value=_symbols(), multi=True,
                                 style={**_dropdown_style(), "minWidth": "320px"}),
                ], style={"display": "flex", "alignItems": "center", "gap": "8px"}),
                html.Div([
                    html.Label("Lookback (min)", style={"color": MUTED,
                               "fontSize": "12px", "marginRight": "8px"}),
                    dcc.Dropdown(id="lookback", options=LOOKBACK_OPTIONS,
                                 value=DEFAULT_LOOKBACK, clearable=False,
                                 style=_dropdown_style()),
                ], style={"display": "flex", "alignItems": "center", "gap": "8px"}),
            ],
        ),
        html.Div(id="kpi-row", style={"display": "flex", "gap": "14px",
                 "flexWrap": "wrap", "marginBottom": "22px"}),
        html.Div(style={"marginBottom": "26px"},
                 children=[_caption("price"),
                           dcc.Graph(id="price-vwap-chart",
                                     config={"displayModeBar": False})]),
        html.Div(style={"display": "flex", "gap": "26px", "rowGap": "26px",
                        "flexWrap": "wrap"},
                 children=[
                     _chart_block("vol-chart", "vol"),
                     _chart_block("imbalance-chart", "imbalance"),
                 ]),
        html.Div(style={"display": "flex", "gap": "26px", "rowGap": "26px",
                        "flexWrap": "wrap", "marginTop": "26px"},
                 children=[
                     _chart_block("corr-chart", "corr"),
                     _chart_block("cumvol-chart", "cumvol"),
                 ]),
        dcc.Store(id="metrics-store"),
        dcc.Interval(id="tick", interval=REFRESH_MS, n_intervals=0),
    ],
)


# --- Data refresh -> Store -------------------------------------------
@app.callback(
    Output("metrics-store", "data"),
    Output("last-updated", "children"),
    Input("tick", "n_intervals"),
    Input("lookback", "value"),
)
def refresh(_n: int, lookback: int):
    try:
        df = load_metrics(int(lookback or DEFAULT_LOOKBACK))
    except Exception as exc:  # surface query errors in the UI rather than 500
        return None, f"⚠ query error: {str(exc)[:120]}"
    now = datetime.now(timezone.utc).strftime("%H:%M:%S UTC")
    label = f"updated {now} · {len(df)} rows" if not df.empty else f"no data · {now}"
    return df.to_json(date_format="iso", orient="split"), label


def _read(data) -> pd.DataFrame:
    if not data:
        return pd.DataFrame()
    df = pd.read_json(StringIO(data), orient="split")
    if not df.empty:
        df["minute"] = pd.to_datetime(df["minute"])
    return df


def _on_grid(d: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    """Reindex one symbol's rows onto a continuous 1-minute grid.

    Missing minutes (ingestion gaps) become NaN, so lines break across them
    (connectgaps=False) and returns/diffs are never computed across a gap —
    which would otherwise treat, e.g., an 11-minute gap as a single 1-min move.
    """
    s = d.sort_values("minute").set_index("minute")
    grid = pd.date_range(s.index.min(), s.index.max(), freq="1min")
    return s.reindex(grid)[cols]


def _base_fig(title: str, ytitle: str) -> go.Figure:
    fig = go.Figure()
    fig.update_layout(
        # Title pinned to the very top; legend moved to the BOTTOM so it never
        # overlaps the title (the old top legend sat right on top of it).
        title=dict(text=title, font=dict(size=15), x=0.01, xanchor="left",
                   y=0.97, yanchor="top"),
        template="plotly_dark", paper_bgcolor=PANEL, plot_bgcolor=PANEL,
        margin=dict(l=50, r=20, t=48, b=58), height=340,
        legend=dict(orientation="h", yanchor="top", y=-0.18, x=0),
        xaxis=dict(gridcolor="#21262d"), yaxis=dict(gridcolor="#21262d", title=ytitle),
        font=dict(color=TEXT),
    )
    return fig


def _empty(title: str) -> go.Figure:
    fig = _base_fig(title, "")
    fig.add_annotation(text="Waiting for data — run the producer + consumer, then dbt.",
                       showarrow=False, font=dict(color=MUTED, size=14))
    return fig


# --- Charts -----------------------------------------------------------
@app.callback(Output("price-vwap-chart", "figure"),
              Input("metrics-store", "data"), Input("symbol-filter", "value"))
def price_vwap(data, symbols):
    df = _read(data)
    if df.empty or not symbols:
        return _empty("Price vs VWAP")
    # Plot deviation of price from session VWAP in basis points. This is
    # scale-free, so BTC (~$64k), ETH (~$1.7k) and SOL (~$70) share one axis
    # centered at 0 — raw prices on a shared linear axis just look flat.
    fig = _base_fig("Price vs Session VWAP (deviation, bps)", "bps from VWAP")
    for i, sym in enumerate(symbols):
        d = df[df["symbol"] == sym]
        if d.empty:
            continue
        g = _on_grid(d, ["close", "session_vwap"])
        dev_bps = (g["close"] / g["session_vwap"] - 1) * 10_000
        fig.add_trace(go.Scatter(
            x=g.index, y=dev_bps, name=sym, mode="lines", connectgaps=False,
            line=dict(color=_color(sym, i), width=2),
            hovertemplate=f"{sym}: %{{y:.1f}} bps<extra></extra>",
        ))
    # Zero line = trading exactly at the session VWAP (rich above, cheap below).
    fig.add_hline(y=0, line_color=MUTED, line_width=1, line_dash="dash")
    return fig


@app.callback(Output("vol-chart", "figure"),
              Input("metrics-store", "data"), Input("symbol-filter", "value"))
def volatility(data, symbols):
    df = _read(data)
    if df.empty or not symbols:
        return _empty("Realized Volatility")
    fig = _base_fig("Realized Volatility (annualized %)", "ann. vol %")
    for i, sym in enumerate(symbols):
        d = df[df["symbol"] == sym]
        if d.empty:
            continue
        g = _on_grid(d, ["annualized_vol_pct"])
        fig.add_trace(go.Scatter(x=g.index, y=g["annualized_vol_pct"], name=sym,
                      mode="lines", connectgaps=False,
                      line=dict(color=_color(sym, i), width=2)))
    return fig


@app.callback(Output("imbalance-chart", "figure"),
              Input("metrics-store", "data"), Input("symbol-filter", "value"))
def imbalance(data, symbols):
    df = _read(data)
    if df.empty or not symbols:
        return _empty("Trade Imbalance")
    fig = _base_fig("Trade Imbalance (smoothed, taker side)", "imbalance")
    for i, sym in enumerate(symbols):
        d = df[df["symbol"] == sym]
        if d.empty:
            continue
        g = _on_grid(d, ["imbalance_smoothed"])
        fig.add_trace(go.Scatter(x=g.index, y=g["imbalance_smoothed"], name=sym,
                      mode="lines", connectgaps=False,
                      line=dict(color=_color(sym, i), width=2)))
    fig.add_hline(y=0, line_color=MUTED, line_width=1)
    fig.update_yaxes(range=[-1, 1])
    return fig


@app.callback(Output("corr-chart", "figure"), Input("metrics-store", "data"))
def btc_eth_correlation(data):
    """Rolling 5-minute correlation of BTC vs ETH 1-minute log returns.

    Always BTC/ETH (a fixed pair), independent of the symbol filter.
    """
    df = _read(data)
    title = "BTC/ETH Rolling 5-min Return Correlation"
    if df.empty:
        return _empty(title)
    wide = df.pivot_table(index="minute", columns="symbol", values="close").sort_index()
    if "BTCUSDT" not in wide or "ETHUSDT" not in wide:
        return _empty(title)
    # Reindex onto a continuous grid so a return is never computed across a gap
    # (an 11-min ingestion gap must not look like one 1-min move).
    wide = wide.reindex(pd.date_range(wide.index.min(), wide.index.max(), freq="1min"))
    rets = np.log(wide[["BTCUSDT", "ETHUSDT"]]).diff()
    # 5 return observations per window; need ≥6 price points before the first value.
    corr = rets["BTCUSDT"].rolling(window=5, min_periods=5).corr(rets["ETHUSDT"])
    fig = _base_fig(title, "correlation")
    fig.add_trace(go.Scatter(
        x=corr.index, y=corr.values, name="BTC↔ETH", mode="lines",
        line=dict(color="#a371f7", width=2), connectgaps=False,
        hovertemplate="%{y:.2f}<extra></extra>",
    ))
    fig.add_hline(y=0, line_color=MUTED, line_width=1, line_dash="dash")
    fig.update_yaxes(range=[-1.05, 1.05])
    if corr.dropna().empty:
        fig.add_annotation(text="Need ≥6 minutes of data to start the window…",
                           showarrow=False, font=dict(color=MUTED, size=13))
    return fig


@app.callback(Output("cumvol-chart", "figure"),
              Input("metrics-store", "data"), Input("symbol-filter", "value"))
def cumulative_volume(data, symbols):
    """Cumulative quote volume (USDT notional) per symbol over the window.

    Quote volume is used (not base volume) so symbols are comparable on one axis.
    """
    df = _read(data)
    if df.empty or not symbols:
        return _empty("Cumulative Volume")
    fig = _base_fig("Cumulative Quote Volume (USDT)", "USDT")
    for i, sym in enumerate(symbols):
        d = df[df["symbol"] == sym]
        if d.empty:
            continue
        # Grid + cumsum(skipna): running total carries across gaps, but the gap
        # minutes are NaN so the line breaks there instead of drawing a flat
        # connector through ingestion downtime.
        g = _on_grid(d, ["quote_volume"])
        fig.add_trace(go.Scatter(
            x=g.index, y=g["quote_volume"].cumsum(), name=sym, mode="lines",
            connectgaps=False, line=dict(color=_color(sym, i), width=2),
            hovertemplate=f"{sym}: $%{{y:,.0f}}<extra></extra>",
        ))
    return fig


# --- KPI cards --------------------------------------------------------
def _kpi_card(sym: str, color: str, price, prem, vol, imb) -> html.Div:
    def fmt(v, suffix="", dp=2):
        return "—" if v is None or pd.isna(v) else f"{v:,.{dp}f}{suffix}"
    prem_color = "#3fb950" if (prem or 0) >= 0 else "#f85149"
    return html.Div(
        style={"backgroundColor": PANEL, "borderLeft": f"4px solid {color}",
               "borderRadius": "8px", "padding": "12px 16px", "minWidth": "190px",
               "flex": "1 1 190px"},
        children=[
            html.Div(sym, style={"fontWeight": "700", "color": color}),
            html.Div(fmt(price), style={"fontSize": "22px", "fontWeight": "600"}),
            html.Div([html.Span("vs VWAP "), html.Span(fmt(prem, "%"),
                     style={"color": prem_color})], style={"fontSize": "12px",
                     "color": MUTED}),
            html.Div(f"vol {fmt(vol, '%', 1)} · imb {fmt(imb, '', 2)}",
                     style={"fontSize": "12px", "color": MUTED}),
        ],
    )


@app.callback(Output("kpi-row", "children"),
              Input("metrics-store", "data"), Input("symbol-filter", "value"))
def kpis(data, symbols):
    df = _read(data)
    if df.empty or not symbols:
        return [html.Div("No data yet.", style={"color": MUTED})]
    latest = latest_per_symbol(df[df["symbol"].isin(symbols)])
    cards = []
    for i, sym in enumerate(symbols):
        row = latest[latest["symbol"] == sym]
        if row.empty:
            continue
        r = row.iloc[0]
        prem = (r["close"] / r["session_vwap"] - 1) * 100 if r.get("session_vwap") else None
        cards.append(_kpi_card(sym, _color(sym, i), r["close"], prem,
                     r.get("annualized_vol_pct"), r.get("imbalance_smoothed")))
    return cards or [html.Div("No data for selection.", style={"color": MUTED})]


def main() -> None:
    # 8050 collides with another local app here; default to 8051 (override with DASH_PORT).
    app.run(host="0.0.0.0", port=int(os.getenv("DASH_PORT", "8051")), debug=True)


if __name__ == "__main__":
    main()
