# Stock Trading Assistant

An AI-powered trading assistant for stocks, ETFs, and crypto. Combines real-time stream processing, ML-based signal generation, sentiment analysis, and political trade tracking to produce BUY / SELL / HOLD signals.

---

## Architecture

```
                        ┌─────────────────────────────────────────┐
                        │           DATA SOURCES                  │
                        │                                         │
  Real-time             │  Alpaca WS · Binance WS · Finnhub WS   │
  (sub-second)          └────────────────┬────────────────────────┘
                                         │ WebSocket → Kafka
                        ┌────────────────▼────────────────────────┐
                        │         backend/producers/              │
                        │  price_producer.py · news_producer.py  │
                        └────────────────┬────────────────────────┘
                                         │
                              ┌──────────▼──────────┐
                              │   Apache Kafka       │
                              │  price.ticks         │
                              │  news.raw            │
                              │  congress.raw        │
                              │  signals.generated   │
                              └──────────┬──────────┘
                                         │
                        ┌────────────────▼────────────────────────┐
                        │       flink-kotlin/  (Kotlin/JVM)       │
                        │                                         │
                        │  EventNormaliserFunction                │
                        │    → unifies 3 schemas into MarketEvent │
                        │  FeatureWindowFunction (keyed/symbol)   │
                        │    → RocksDB state: 60-bar price window │
                        │    → RSI, MACD, EMA, BB, ATR, OBV      │
                        │  AsyncInferenceFunction                 │
                        │    → OkHttp async → Python model server │
                        └────────────────┬────────────────────────┘
                                         │ POST /internal/inference
                        ┌────────────────▼────────────────────────┐
                        │        backend/  (Python/FastAPI)       │
                        │                                         │
                        │  ml/signal_generator.py                 │
                        │    quant(50%) + sentiment(30%)          │
                        │    + congress(20%) → BUY/SELL/HOLD      │
                        │                                         │
                        │  ml/sentiment/finbert.py                │
                        │    ProsusAI/finbert → [-1, 1] score     │
                        │                                         │
                        │  ai/multi_model.py                      │
                        │    Claude + Gemini parallel reasoning   │
                        └────────────────┬────────────────────────┘
                                         │
                              ┌──────────▼──────────┐
                              │   PostgreSQL +       │
                              │   TimescaleDB        │
                              │   (historical data)  │
                              └─────────────────────┘
```

**Batch path** (Celery Beat) handles historical ingestion — Yahoo Finance, Reddit, NewsAPI, NewsData.io, congressional disclosures — on schedules ranging from every 5 minutes to daily.

**Stream path** (Flink) handles real-time events — price ticks, breaking news — with signals emitted within seconds.

---

## Data Sources

| Source | Data | Schedule |
|---|---|---|
| Alpaca Markets | Real-time stock bars & trades | WebSocket (continuous) |
| Binance | Crypto OHLCV | Every 5 min |
| Yahoo Finance | Stock/ETF/Crypto historical OHLCV | Every hour |
| Finnhub | News, earnings calendar, pre-scored sentiment | Every 15 min + WebSocket |
| NewsAPI | Financial news headlines | Every 30 min |
| NewsData.io | Global news (90k+ sources) | Every hour |
| Reddit (PRAW) | WSB, r/stocks, r/investing, r/cryptocurrency | Every 15 min |
| QuiverQuant | Congressional trade disclosures | Every 6 hours |
| whitehouse.gov RSS | Presidential/government announcements | Every hour |
| FRED | Macro data (rates, CPI) | Daily |

---

## Signal Generation

Each signal is a weighted composite of three independent scores:

```
composite = 0.5 × quant_score
          + 0.3 × sentiment_score
          + 0.2 × congress_modifier

VIX > 25 → dampen by 20%
VIX > 30 → dampen by 40%

composite > 0.3  → BUY
composite < -0.3 → SELL
else             → HOLD
```

**Quant score** — derived from technical indicators (RSI, MACD, EMA crossovers, Bollinger position, ATR, volume ratio) and eventually XGBoost/LSTM forecasting models trained on historical OHLCV.

**Sentiment score** — FinBERT (`ProsusAI/finbert`) runs on news articles and Reddit posts. Finnhub's pre-scored sentiment is used directly where available.

**Congress modifier** — purchase/sale disclosures from QuiverQuant scaled by amount range. Larger trades ($1M+) get stronger signals.

Each signal is explained in plain English by Claude (`claude-sonnet-4-6`) citing the specific news, technicals, and congressional trades that drove it. Optionally runs through both Claude and Gemini in parallel, with a synthesized consensus view that flags where the models disagree.

---

## Project Structure

```
stock-trading-assistant/
├── backend/                    # Python — FastAPI, ML models, ingestion
│   ├── ai/                     # Claude + Gemini clients, trade reasoning, multi-model
│   ├── api/v1/                 # REST endpoints: assets, signals, congress, portfolio
│   ├── db/                     # SQLAlchemy async session
│   ├── ingestion/              # Data source connectors (one class per source)
│   ├── ml/
│   │   ├── sentiment/          # FinBERT inference + aggregation
│   │   ├── forecasting/        # XGBoost, LSTM, Prophet (Phase 3)
│   │   ├── signal_generator.py # Core signal scoring logic
│   │   └── backtester.py       # Event-driven backtesting (Phase 3)
│   ├── models/                 # SQLAlchemy ORM models
│   ├── producers/              # Kafka producers (Alpaca WS, Finnhub WS)
│   ├── processing/             # Feature engineering, deduplication
│   └── tasks/                  # Celery tasks + beat schedule
│
├── flink-kotlin/               # Kotlin/JVM — Flink stream processor
│   └── src/main/kotlin/com/tradingassistant/
│       ├── SignalProcessorJob.kt
│       ├── model/Events.kt
│       └── operators/
│           ├── EventNormaliserFunction.kt
│           ├── FeatureWindowFunction.kt   # RocksDB stateful windowing
│           └── AsyncInferenceFunction.kt  # Non-blocking HTTP to Python
│
├── alembic/                    # Database migrations
├── scripts/                    # backfill_ohlcv.py, seed_assets.py
├── tests/
├── docker-compose.yml
└── pyproject.toml
```

---

## Stack

| Layer | Technology |
|---|---|
| Stream processing | Apache Flink 1.19 (Kotlin) |
| Event bus | Apache Kafka |
| API | FastAPI + uvicorn |
| ML / Sentiment | PyTorch, HuggingFace Transformers (FinBERT), XGBoost, Prophet |
| AI reasoning | Anthropic Claude (`claude-sonnet-4-6`) + Google Gemini |
| Database | PostgreSQL 16 + TimescaleDB |
| Task queue | Celery + Redis |
| ORM / Migrations | SQLAlchemy (async) + Alembic |
| Monitoring | Flink UI (8081), Kafka UI (8080), Celery Flower (5555) |

---

## Getting Started

### Prerequisites
- Docker + Docker Compose
- Java 11+ and Maven (for the Flink job)
- Python 3.11+

### 1. Configure environment

```bash
cp .env.example .env
# Fill in API keys — see table below
```

| Key | Source | Required |
|---|---|---|
| `ALPACA_API_KEY` / `ALPACA_API_SECRET` | alpaca.markets (free paper account) | Yes |
| `FINNHUB_API_KEY` | finnhub.io (free tier) | Yes |
| `NEWSAPI_KEY` | newsapi.org (free 100/day) | Recommended |
| `NEWSDATA_API_KEY` | newsdata.io (free 200/day) | Recommended |
| `REDDIT_CLIENT_ID` / `REDDIT_CLIENT_SECRET` | reddit.com/prefs/apps | Yes |
| `QUIVERQUANT_API_KEY` | quiverquant.com (free tier) | For congress trades |
| `ANTHROPIC_API_KEY` | console.anthropic.com | Yes |
| `GEMINI_API_KEY` | aistudio.google.com | Optional |

### 2. Start infrastructure

```bash
docker compose up postgres redis kafka zookeeper -d
```

### 3. Install Python dependencies

```bash
pip install -e ".[dev]"
```

### 4. Run database migrations and seed

```bash
alembic upgrade head
python scripts/seed_assets.py
python scripts/backfill_ohlcv.py   # backfills 2 years of OHLCV
```

### 5. Start the Python backend

```bash
# API server
uvicorn backend.main:app --reload

# Celery worker + beat (batch ingestion)
celery -A backend.tasks.celery_app worker --loglevel=info -Q default,ml,ingestion
celery -A backend.tasks.celery_app beat --loglevel=info

# Real-time Kafka producers
python -m backend.producers.price_producer
python -m backend.producers.news_producer
```

Or start everything via Docker:

```bash
docker compose up
```

### 6. Build and submit the Flink job

```bash
cd flink-kotlin
mvn clean package -DskipTests

flink run -c com.tradingassistant.SignalProcessorJobKt \
  target/flink-signal-processor-0.1.0.jar
```

### Service URLs

| Service | URL |
|---|---|
| API (Swagger docs) | http://localhost:8000/docs |
| Flink UI | http://localhost:8081 |
| Kafka UI | http://localhost:8080 |
| Celery Flower | http://localhost:5555 |

---

## Roadmap

- **Phase 1** ✅ Infrastructure, OHLCV ingestion, rule-based signals, REST API
- **Phase 2** — FinBERT sentiment pipeline, Reddit/news correlation to price
- **Phase 3** — XGBoost/LSTM forecasting models, backtesting engine
- **Phase 4** — Presidential announcement tracking, Claude/Gemini chat interface (RAG)
- **Phase 5** — React dashboard, paper trading via Alpaca, portfolio optimisation
