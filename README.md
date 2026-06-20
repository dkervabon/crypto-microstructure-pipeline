# Real-Time Crypto Microstructure Pipeline

### 📊 Live dashboard → **https://crypto-microstructure-dashboard.onrender.com/**

Stream live trade ticks from Binance for **BTC/USDT, ETH/USDT, SOL/USDT**, move
them through **Kafka**, land them in **BigQuery**, compute microstructure metrics
(**VWAP, realized volatility, trade imbalance**) with **dbt**, and visualize on a
live **Dash** dashboard.

```
Binance WS ──▶ Python producer ──▶ Confluent Cloud Kafka ──▶ consumer ──▶ BigQuery ──▶ dbt ──▶ Dash
   @trade                          ticks.* (SASL_SSL)                       raw         marts   live
```

## Key findings

Computed directly from the `crypto_marts` tables (sample window: ~2h of trading,
2026-06-20 UTC; figures update live on the dashboard as more data accrues).

**Realized volatility scales inversely with market cap.** Annualized realized
vol (rolling 15-min window of 1-minute log returns) cleanly separates the three
assets — the smaller the coin, the more volatile:

| symbol | median ann. vol | typical range (IQR) |
|--------|----------------:|---------------------|
| BTCUSDT | **17.7%** | 15.1 – 23.5% |
| ETHUSDT | **24.8%** | 21.3 – 28.1% |
| SOLUSDT | **34.7%** | 31.5 – 40.8% |

**BTC and ETH are tightly coupled.** The rolling 5-minute correlation of their
1-minute log returns has a **median of 0.79** (IQR 0.70–0.92) and is **positive
in 98% of windows**, but it does break down — the full range spans **−0.13 to
0.99**, i.e. brief decoupling episodes do occur.

**Order flow is near-balanced but noisy.** Per-minute taker-side imbalance
averages near zero (BTC +0.02, ETH −0.05, SOL −0.05) — no persistent directional
pressure over the sample — yet swings hard minute-to-minute (std ≈ 0.5; 10th/90th
percentiles around ±0.7), so individual minutes are frequently dominated by
aggressive buyers or sellers. BTC was buy-dominant in 55% of minutes, ETH 45%.

## Status

- ✅ **Phase 1 — Streaming layer:** Confluent Cloud Kafka (SASL_SSL), per-ticker
  topics, Binance → Kafka producer.
- ✅ **Phase 2 — Kafka → BigQuery consumer:** batched streaming inserts into a
  partitioned/clustered raw table.
- ✅ **Phase 3 — dbt marts:** VWAP, realized volatility, and trade imbalance as
  live views over the raw ticks.
- ✅ **Phase 4 — Dash dashboard:** live-updating charts + KPIs over the marts,
  deployable to Render.

## Prerequisites

- Python 3.11+
- A Confluent Cloud Kafka cluster + an API key/secret

## Quick start

Streaming runs on **Confluent Cloud** (SASL_SSL). First put your cluster
bootstrap server and API key/secret in `.env`:

```bash
cp .env.example .env
# edit .env and set:
#   KAFKA_BOOTSTRAP_SERVERS=pkc-921jm.us-east-2.aws.confluent.cloud:9092
#   KAFKA_API_KEY=...        (Confluent API key  = SASL username)
#   KAFKA_API_SECRET=...     (Confluent API secret = SASL password)
```

```bash
make venv                 # create .venv and install deps
make topics               # create ticks.btcusdt / .ethusdt / .solusdt on the cluster
make producer             # stream Binance trades into Kafka  (Ctrl-C to stop)
```

In another terminal, verify messages are flowing:

```bash
make list-topics                   # list topics on the cluster
make consume-test                  # peek 5 messages from ticks.btcusdt
make consume-test TOPIC=ticks.ethusdt
```

### Land in BigQuery + build marts

```bash
make bq-setup     # create crypto_raw.ticks (partitioned by day, clustered by symbol)
make consumer     # Kafka -> BigQuery streaming inserts  (Ctrl-C to stop)
make dbt-build    # build VWAP / realized-vol / imbalance views + run tests
```

### Run the dashboard

```bash
make dashboard    # http://localhost:8051  (live-updating; refreshes every 15s)
```

## Dashboard

`src/crypto_pipeline/dashboard/` — a Dash app that queries the marts and
live-updates every 15s (one cached BigQuery scan per refresh feeds a `dcc.Store`;
the KPI cards and three charts derive from it):

- **Price vs VWAP** — minute close and VWAP per symbol
- **Realized Volatility** — annualized %, rolling window
- **Trade Imbalance** — smoothed taker-side order-flow imbalance
- **KPI cards** — last price, premium/discount vs session VWAP, vol, imbalance

## Deploy to Render (production)

`render.yaml` defines the whole pipeline as three continuously-running services:

| service | type | does |
|---------|------|------|
| `crypto-producer` | worker | Binance WS → Confluent Cloud |
| `crypto-consumer` | worker | Confluent Cloud → BigQuery (creates topics on pre-deploy) |
| `crypto-microstructure-dashboard` | web | Dash app over the BigQuery marts |

All config comes from environment variables, organized into three Render
**env var groups** so each value is defined once and scoped to only the services
that need it (the producer never receives GCP creds; the dashboard never
receives Kafka creds).

**Steps**
1. Push this repo to GitHub.
2. In Render: **New → Blueprint**, point at the repo (`render.yaml` is detected).
3. Fill in the three secret env vars (marked `sync: false`) in the Render UI:
   - `KAFKA_API_KEY`, `KAFKA_API_SECRET` — Confluent API key/secret
   - `GOOGLE_APPLICATION_CREDENTIALS_JSON` — the service-account key,
     **base64-encoded**: `base64 -i your-key.json` (pipe to `| pbcopy` on macOS).
     Base64 avoids the private-key newline corruption that a hosting UI would
     otherwise cause (the symptom is a `JSONDecodeError`). Locally, dev falls
     back to the file path in `GOOGLE_APPLICATION_CREDENTIALS`.

> **Plans:** all three services run on `starter` — the two workers because
> Render background workers require a paid tier, and the dashboard for always-on
> uptime (no idle spin-down) plus the extra CPU that speeds up chart rendering.
> `wsgi.py` (`gunicorn wsgi:server`) is the web entry point; `Procfile` mirrors it.

## Layout

```
admin.py (in pkg)           Confluent Cloud topic management (create / list / peek)
Makefile                    venv / topics / producer / consumer / dbt / dashboard
requirements.txt            Python deps
src/crypto_pipeline/
  config.py                 env-driven Settings (symbols, topics, Kafka auth, BigQuery)
  producer.py               Binance WS -> Kafka producer  (SASL_SSL)
  consumer.py               Kafka -> BigQuery streaming inserts  (SASL_SSL)
```

## Tick schema

Each Binance `@trade` event is normalized to a flat JSON record, keyed by symbol:

| field            | type   | meaning                                            |
|------------------|--------|----------------------------------------------------|
| `symbol`         | string | e.g. `BTCUSDT`                                      |
| `trade_id`       | int    | Binance trade id                                   |
| `price`          | float  | trade price                                        |
| `quantity`       | float  | base-asset quantity                                |
| `quote_quantity` | float  | `price * quantity` (quote-asset notional)          |
| `is_buyer_maker` | bool   | Binance `m` flag                                   |
| `taker_side`     | string | `buy` / `sell` — aggressor side (drives imbalance) |
| `trade_time`     | int    | exchange trade timestamp (ms)                      |
| `event_time`     | int    | exchange event timestamp (ms)                      |
| `ingest_time`    | int    | producer wall-clock timestamp (ms)                 |

## dbt marts

All marts are **views** (always reflect the latest streamed ticks; no scheduled
rebuild needed) over a shared 1-minute bar layer.

```
source: crypto_raw.ticks
  └─ stg_ticks (view)            cast/clean + per-tick buy/sell/quote volumes
       └─ int_minute_bars (ephemeral)   1-min OHLCV + VWAP + buy/sell split
            ├─ mart_vwap                 minute VWAP + cumulative session VWAP
            ├─ mart_realized_volatility  rolling realized vol from 1-min log returns (annualized)
            └─ mart_trade_imbalance      taker-side order-flow imbalance, raw + smoothed
```

Window sizes are dbt vars (`vol_window_minutes`=15, `imbalance_window_minutes`=5).
Tests: source freshness, `not_null` on keys/metrics, and a grain-uniqueness
assertion (one row per `symbol, minute`) across all marts.

## Notes

- **Endpoint:** uses `wss://data-stream.binance.vision` (Binance's public
  market-data mirror). The main `stream.binance.com` endpoint returns **HTTP 451**
  from US IPs; the mirror serves identical data and symbols.
- **Kafka:** Confluent Cloud over **SASL_SSL** (API key/secret). `acks=all` +
  idempotent producer for exactly-once-ish delivery; messages are keyed by symbol
  so per-symbol order is preserved. Topics use replication factor 3 (Confluent
  requirement) and are created explicitly via `make topics`.
- Secrets (`.env`, service-account JSON, Kafka API key/secret) are git-ignored.
```
