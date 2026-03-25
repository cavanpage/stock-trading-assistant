import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from backend.api.v1 import assets, signals, congress, portfolio, backfill

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Stock Trading Assistant API starting up")
    yield
    logger.info("Stock Trading Assistant API shutting down")


app = FastAPI(
    title="Stock Trading Assistant API",
    version="0.1.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Restrict in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(assets.router, prefix="/api/v1", tags=["assets"])
app.include_router(signals.router, prefix="/api/v1", tags=["signals"])
app.include_router(congress.router, prefix="/api/v1", tags=["congress"])
app.include_router(portfolio.router, prefix="/api/v1", tags=["portfolio"])
app.include_router(backfill.router, prefix="/api/v1", tags=["backfill"])


@app.get("/health")
async def health():
    return {"status": "ok", "version": "0.1.0"}


@app.get("/")
async def root():
    return {"status": "ok", "version": "0.1.0"}
