from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # Database
    database_url: str = "postgresql+asyncpg://trader:trader@localhost:5432/trading"
    sync_database_url: str = "postgresql://trader:trader@localhost:5432/trading"

    # Redis
    redis_url: str = "redis://localhost:6379/0"

    # Alpaca
    alpaca_api_key: str = ""
    alpaca_api_secret: str = ""
    alpaca_base_url: str = "https://paper-api.alpaca.markets"

    # Binance
    binance_api_key: str = ""
    binance_api_secret: str = ""

    # Reddit
    reddit_client_id: str = ""
    reddit_client_secret: str = ""
    reddit_user_agent: str = "stock-trading-assistant/0.1"

    # NewsAPI
    newsapi_key: str = ""

    # Finnhub
    finnhub_api_key: str = ""

    # NewsData.io
    newsdata_api_key: str = ""

    # QuiverQuant
    quiverquant_api_key: str = ""

    # Anthropic (Claude)
    anthropic_api_key: str = ""

    # Google (Gemini)
    gemini_api_key: str = ""

    # App
    secret_key: str = "change-me"
    environment: str = "development"
    log_level: str = "INFO"


settings = Settings()
