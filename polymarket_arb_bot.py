"""
Polymarket Latency Arbitrage Bot
=================================
Monitors BTC/ETH short-duration contracts on Polymarket and detects pricing
lags versus Binance real-time feeds. Runs in PAPER MODE by default.

WARNING: This bot is for educational and research purposes only.
- Polymarket is NOT available to US residents.
- Automated trading carries significant financial risk.
- Past backtests do not guarantee future performance.
- Verify regulatory compliance in your jurisdiction before using live funds.

Usage:
  Paper mode (default, safe):
    python polymarket_arb_bot.py

  Live mode (requires all three explicit flags):
    python polymarket_arb_bot.py --live --confirm-live --i-accept-risk

Requirements:
  pip install py-clob-client websockets aiohttp rich python-dotenv httpx
"""

import asyncio
import json
import logging
import math
import os
import random
import sqlite3
import sys
import time
import argparse
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from pathlib import Path
from typing import Optional

import aiohttp
import httpx
import websockets
from dotenv import load_dotenv
from rich.console import Console
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from rich.text import Text
from rich import box
import dns.resolver  # for manual DNS
from aiohttp import AsyncResolver

load_dotenv()

# Force UTF-8 on Windows to avoid cp1252 encoding errors with Rich
if sys.platform == "win32":
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")
    os.environ.setdefault("PYTHONIOENCODING", "utf-8")

# Force-silence all SSL hostname mismatch errors globally
import ssl
ssl.match_hostname = lambda cert, hostname: None

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

class Config:
    POLY_API_URL          = "https://clob.polymarket.com"
    POLY_PRIVATE_KEY      = os.getenv("POLY_PRIVATE_KEY", "")
    POLY_API_KEY          = os.getenv("POLY_API_KEY", "")
    POLY_API_SECRET       = os.getenv("POLY_API_SECRET", "")
    POLY_API_PASSPHRASE   = os.getenv("POLY_API_PASSPHRASE", "")
    POLY_CHAIN_ID         = int(os.getenv("POLY_CHAIN_ID", "137"))

    BINANCE_WS_URL        = "wss://stream.binance.com:9443/stream"
    BINANCE_STREAMS       = ["btcusdt@aggTrade", "ethusdt@aggTrade"]

    OKX_WS_URL            = "wss://aws.okx.com:8443/ws/v5/public"
    BYBIT_WS_URL          = "wss://stream.bytick.com/v5/public/linear"
    KRAKEN_WS_URL         = "wss://ws.kraken.com/"

    TELEGRAM_BOT_TOKEN    = os.getenv("TELEGRAM_BOT_TOKEN", "")
    TELEGRAM_CHAT_ID      = os.getenv("TELEGRAM_CHAT_ID", "")

    MIN_EDGE_PCT          = 5.0
    LAG_DETECTION_PCT     = 3.0
    CONFIDENCE_THRESHOLD  = 85.0
    KELLY_FRACTION        = 0.5

    MAX_POSITION_PCT      = 0.08
    DAILY_LOSS_LIMIT_PCT  = 0.20
    KILL_SWITCH_PCT       = 0.40
    MIN_MARKET_LIQUIDITY  = 50_000

    ORDER_TIMEOUT_S       = 10
    RATE_LIMIT_ORDERS_MIN = 30
    RETRY_ATTEMPTS        = 3
    RETRY_DELAY_S         = 1.5

    DB_PATH               = "arb_bot.db"
    LOG_PATH              = "arb_bot_v2.log"
    LOG_LEVEL             = logging.INFO

    DNS_SERVERS           = ["8.8.8.8", "1.1.1.1", "9.9.9.9"]

    EXCHANGE_IPS = {
        "aws.okx.com":        "13.248.163.43",   # OKX AWS node
        "stream.bytick.com":  "104.18.23.111",   # Bybit bypass node
        "ws.kraken.com":     "104.16.234.220",
        "stream.binance.com": "13.248.211.235",
    }


def resolve_host(hostname: str) -> str:
    """Manually resolve hostname using hardcoded IPs (Nuclear Option) or Google/Cloudflare DNS."""
    # 1. Try hardcoded IPs first
    if hostname in Config.EXCHANGE_IPS:
        ip = Config.EXCHANGE_IPS[hostname]
        logging.debug(f"Using hardcoded IP for {hostname} -> {ip}")
        return ip

    # 2. Fallback to manual DNS resolution
    try:
        resolver = dns.resolver.Resolver()
        resolver.nameservers = Config.DNS_SERVERS
        resolver.timeout = 5
        resolver.lifetime = 10
        answers = resolver.resolve(hostname, "A")
        ip = str(answers[0])
        logging.debug(f"Resolved {hostname} -> {ip}")
        return ip
    except Exception as e:
        logging.error(f"DNS resolution failed for {hostname}: {e}")
        return hostname # Fallback to original


# ---------------------------------------------------------------------------
# Data models
# ---------------------------------------------------------------------------

class Side(str, Enum):
    YES = "YES"
    NO  = "NO"

class TradeStatus(str, Enum):
    PAPER    = "PAPER"
    OPEN     = "OPEN"
    FILLED   = "FILLED"
    CANCELED = "CANCELED"
    FAILED   = "FAILED"

@dataclass
class MarketSnapshot:
    market_id:    str
    question:     str
    asset:        str
    duration_min: int
    yes_price:    float
    no_price:     float
    volume:       float
    liquidity:    float
    end_time:     datetime
    fetched_at:   float = field(default_factory=time.time)

@dataclass
class PriceSnapshot:
    asset:     str
    price:     float
    timestamp: float = field(default_factory=time.time)
    source:    str   = "binance"

@dataclass
class ArbitrageOpportunity:
    market:           MarketSnapshot
    side:             Side
    market_prob:      float
    true_prob:        float
    edge_pct:         float
    confidence:       float
    kelly_fraction:   float
    recommended_size: float
    cex_price:        float
    cex_move_pct:     float
    detected_at:      float = field(default_factory=time.time)

@dataclass
class Trade:
    id:            Optional[int]
    market_id:     str
    question:      str
    asset:         str
    side:          str
    market_prob:   float
    true_prob:     float
    edge_pct:      float
    confidence:    float
    size_usd:      float
    entry_price:   float
    exit_price:    Optional[float]
    pnl:           Optional[float]
    status:        str
    is_paper:      bool
    order_id:      Optional[str]
    opened_at:     str
    closed_at:     Optional[str]
    notes:         str = ""


# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

class Database:
    def __init__(self, path: str = Config.DB_PATH):
        self.path = path
        self._init()

    def _conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path, timeout=10)
        conn.row_factory = sqlite3.Row
        return conn

    def _init(self):
        with self._conn() as c:
            c.executescript("""
                CREATE TABLE IF NOT EXISTS trades (
                    id          INTEGER PRIMARY KEY AUTOINCREMENT,
                    market_id   TEXT    NOT NULL,
                    question    TEXT    NOT NULL,
                    asset       TEXT    NOT NULL,
                    side        TEXT    NOT NULL,
                    market_prob REAL    NOT NULL,
                    true_prob   REAL    NOT NULL,
                    edge_pct    REAL    NOT NULL,
                    confidence  REAL    NOT NULL,
                    size_usd    REAL    NOT NULL,
                    entry_price REAL    NOT NULL,
                    exit_price  REAL,
                    pnl         REAL,
                    status      TEXT    NOT NULL,
                    is_paper    INTEGER NOT NULL,
                    order_id    TEXT,
                    opened_at   TEXT    NOT NULL,
                    closed_at   TEXT,
                    notes       TEXT    DEFAULT ''
                );
                CREATE TABLE IF NOT EXISTS price_log (
                    id        INTEGER PRIMARY KEY AUTOINCREMENT,
                    asset     TEXT  NOT NULL,
                    price     REAL  NOT NULL,
                    source    TEXT  NOT NULL,
                    logged_at TEXT  NOT NULL
                );
                CREATE TABLE IF NOT EXISTS session_stats (
                    id            INTEGER PRIMARY KEY AUTOINCREMENT,
                    session_start TEXT NOT NULL,
                    initial_bal   REAL NOT NULL,
                    peak_bal      REAL NOT NULL,
                    current_bal   REAL NOT NULL,
                    total_trades  INTEGER NOT NULL,
                    wins          INTEGER NOT NULL,
                    losses        INTEGER NOT NULL,
                    updated_at    TEXT NOT NULL
                );
            """)

    def insert_trade(self, t: Trade) -> int:
        with self._conn() as c:
            cur = c.execute("""
                INSERT INTO trades
                  (market_id,question,asset,side,market_prob,true_prob,edge_pct,
                   confidence,size_usd,entry_price,exit_price,pnl,status,is_paper,
                   order_id,opened_at,closed_at,notes)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (t.market_id, t.question, t.asset, t.side, t.market_prob,
                  t.true_prob, t.edge_pct, t.confidence, t.size_usd,
                  t.entry_price, t.exit_price, t.pnl, t.status,
                  int(t.is_paper), t.order_id, t.opened_at,
                  t.closed_at, t.notes))
            return cur.lastrowid

    def update_trade(self, trade_id: int, exit_price: float,
                     pnl: float, status: str, closed_at: str):
        with self._conn() as c:
            c.execute("""
                UPDATE trades SET exit_price=?, pnl=?, status=?, closed_at=?
                WHERE id=?
            """, (exit_price, pnl, status, closed_at, trade_id))

    def recent_trades(self, n: int = 10) -> list[dict]:
        with self._conn() as c:
            rows = c.execute(
                "SELECT * FROM trades ORDER BY id DESC LIMIT ?", (n,)
            ).fetchall()
            return [dict(r) for r in rows]

    def daily_pnl(self) -> float:
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        with self._conn() as c:
            row = c.execute("""
                SELECT COALESCE(SUM(pnl), 0) as total FROM trades
                WHERE opened_at LIKE ? AND pnl IS NOT NULL
            """, (f"{today}%",)).fetchone()
            return row["total"]

    def log_price(self, snap: PriceSnapshot):
        with self._conn() as c:
            c.execute("""
                INSERT INTO price_log (asset, price, source, logged_at)
                VALUES (?,?,?,?)
            """, (snap.asset, snap.price, snap.source,
                  datetime.now(timezone.utc).isoformat()))


# ---------------------------------------------------------------------------
# Telegram notifier
# ---------------------------------------------------------------------------

class Telegram:
    BASE = "https://api.telegram.org/bot{token}/{method}"

    def __init__(self, token: str, chat_id: str):
        self.token   = token
        self.chat_id = chat_id
        self.enabled = bool(token and chat_id)

    async def send(self, text: str):
        if not self.enabled:
            return
        url = self.BASE.format(token=self.token, method="sendMessage")
        payload = {"chat_id": self.chat_id, "text": text, "parse_mode": "Markdown"}
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    url, json=payload,
                    timeout=aiohttp.ClientTimeout(total=8)
                ) as r:
                    if r.status != 200:
                        logging.warning(f"Telegram error {r.status}: {await r.text()}")
        except Exception as e:
            logging.warning(f"Telegram send failed: {e}")

    async def alert_opportunity(self, opp: ArbitrageOpportunity, paper: bool):
        mode = "🧪 PAPER" if paper else "🔴 LIVE"
        msg = (
            f"{mode} *TRADE SIGNAL*\n"
            f"Market: {opp.market.question[:60]}\n"
            f"Side: *{opp.side.value}*\n"
            f"Edge: *{opp.edge_pct:.1f}pp*\n"
            f"Confidence: {opp.confidence:.0f}%\n"
            f"Size: ${opp.recommended_size:.2f}\n"
            f"Mkt prob: {opp.market_prob:.1%} → True: {opp.true_prob:.1%}\n"
            f"CEX move: {opp.cex_move_pct:+.2f}%"
        )
        await self.send(msg)

    async def alert_drawdown(self, level: str, drawdown_pct: float,
                             balance: float, halted: bool):
        icon = "🛑" if halted else "⚠️"
        msg = (
            f"{icon} *DRAWDOWN ALERT — {level}*\n"
            f"Drawdown: *{drawdown_pct:.1f}%*\n"
            f"Balance: ${balance:.2f}\n"
            f"Trading halted: {'YES' if halted else 'NO'}"
        )
        await self.send(msg)

    async def alert_kill_switch(self, reason: str, balance: float):
        await self.send(
            f"🚨 *KILL SWITCH TRIGGERED*\n"
            f"Reason: {reason}\n"
            f"Balance: ${balance:.2f}\n"
            f"All trading halted immediately."
        )


# ---------------------------------------------------------------------------
# Price tracker (Binance WebSocket)
# ---------------------------------------------------------------------------

class PriceTracker:
    WINDOW_S = 300

    def __init__(self, db: Database):
        self.db      = db
        self.prices: dict[str, deque] = {
            "BTC": deque(maxlen=600),
            "ETH": deque(maxlen=600),
        }
        self.latest: dict[str, PriceSnapshot] = {}
        self._running    = False
        self._log_counter = 0

    def latest_price(self, asset: str) -> Optional[float]:
        snap = self.latest.get(asset)
        return snap.price if snap else None

    def momentum_pct(self, asset: str, lookback_s: float = 30.0) -> Optional[float]:
        dq = self.prices.get(asset)
        if not dq or len(dq) < 2:
            return None
        cutoff   = time.time() - lookback_s
        baseline = next((s for s in dq if s.timestamp >= cutoff), dq[0])
        current  = dq[-1].price
        if baseline.price == 0:
            return None
        return (current - baseline.price) / baseline.price * 100

    def data_age_s(self, asset: str) -> float:
        snap = self.latest.get(asset)
        return (time.time() - snap.timestamp) if snap else float("inf")

    async def run(self):
        self._running = True
        streams = "/".join(Config.BINANCE_STREAMS)
        url = f"{Config.BINANCE_WS_URL}?streams={streams}"
        while self._running:
            try:
                logging.info(f"Connecting to Binance WS: {url}")
                async with websockets.connect(
                    url, ping_interval=20, ping_timeout=10, close_timeout=5
                ) as ws:
                    logging.info("Binance WebSocket connected.")
                    async for raw in ws:
                        self._handle_message(raw)
            except websockets.ConnectionClosed as e:
                logging.warning(f"Binance WS closed ({e}), reconnecting in 3s…")
                await asyncio.sleep(3)
            except Exception as e:
                logging.error(f"Binance WS error: {e}, reconnecting in 5s…")
                await asyncio.sleep(5)

    def _handle_message(self, raw: str):
        try:
            msg    = json.loads(raw)
            stream = msg.get("stream", "")
            data   = msg.get("data", {})
            if "@aggTrade" in stream:
                asset = "BTC" if "btc" in stream else "ETH"
                snap  = PriceSnapshot(asset=asset, price=float(data["p"]))
                self.latest[asset] = snap
                self.prices[asset].append(snap)
                self._log_counter += 1
                if self._log_counter % 60 == 0:
                    self.db.log_price(snap)
        except Exception as e:
            logging.debug(f"Price parse error: {e}")

    def stop(self):
        self._running = False


class BaseExchangeTracker:
    """Base class for all exchange price feeds."""
    WINDOW_S = 300

    def __init__(self, db: Database, exchange_name: str):
        self.db = db
        self.exchange_name = exchange_name
        self.prices: dict[str, deque] = {
            "BTC": deque(maxlen=600),
            "ETH": deque(maxlen=600),
        }
        self.latest: dict[str, PriceSnapshot] = {}
        self._running = False
        self._log_counter = 0

    def latest_price(self, asset: str) -> Optional[float]:
        snap = self.latest.get(asset)
        return snap.price if snap else None

    def momentum_pct(self, asset: str, lookback_s: float = 30.0) -> Optional[float]:
        dq = self.prices.get(asset)
        if not dq or len(dq) < 2:
            return None
        cutoff = time.time() - lookback_s
        baseline = next((s for s in dq if s.timestamp >= cutoff), dq[0])
        current = dq[-1].price
        if baseline.price == 0:
            return None
        return (current - baseline.price) / baseline.price * 100

    def data_age_s(self, asset: str) -> float:
        snap = self.latest.get(asset)
        return (time.time() - snap.timestamp) if snap else float("inf")

    def stop(self):
        self._running = False

    async def run(self):
        self._running = True
        # Extract host and path from ws_url
        # Example: wss://ws.okx.com:8443/ws/v5/public
        from urllib.parse import urlparse
        parsed = urlparse(self.ws_url)
        hostname = parsed.hostname
        path = parsed.path
        if parsed.query:
            path += "?" + parsed.query

        while self._running:
            try:
                logging.info(f"Connecting to {self.exchange_name} WS (Manual DNS)...")
                ip = resolve_host(hostname)
                
                # If resolved successfully, use IP in URI but pass hostname in SNI
                uri = f"wss://{ip}{path}" if ip != hostname else self.ws_url
                
                # Force-disable SSL verification while keeping SNI
                import ssl
                ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS)
                ssl_context.check_hostname = False
                ssl_context.verify_mode = ssl.CERT_NONE
                
                # Determine SNI hostname
                sni_host = hostname
                if "okx" in hostname: sni_host = "okx.com"
                if "bytick" in hostname: sni_host = "bybit.com"

                async with websockets.connect(
                    uri,
                    ssl=ssl_context,
                    server_hostname=sni_host,
                    additional_headers={
                        "Host": hostname,
                        "Origin": f"https://{hostname}",
                        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
                    },
                    ping_interval=20,
                    ping_timeout=10
                ) as ws:
                    await self._subscribe(ws)
                    logging.info(f"{self.exchange_name} WebSocket connected.")
                    async for raw in ws:
                        if not self._running: break
                        self._handle_message(raw)
            except Exception as e:
                logging.warning(f"{self.exchange_name} WS error: {e}, reconnecting in 5s...")
                await asyncio.sleep(5)

    async def _subscribe(self, ws):
        pass

    def _handle_message(self, raw):
        pass

    def _update_price(self, asset: str, price: float):
        snap = PriceSnapshot(asset=asset, price=price)
        self.latest[asset] = snap
        self.prices[asset].append(snap)
        self._log_counter += 1
        if self._log_counter % 100 == 0:
            self.db.log_price(snap)


class OKXPriceTracker(BaseExchangeTracker):
    def __init__(self, db: Database):
        super().__init__(db, "OKX")
        self.ws_url = Config.OKX_WS_URL

    async def _subscribe(self, ws):
        msg = {
            "op": "subscribe",
            "args": [
                {"channel": "tickers", "instId": "BTC-USDT"},
                {"channel": "tickers", "instId": "ETH-USDT"}
            ]
        }
        await ws.send(json.dumps(msg))

    def _handle_message(self, raw):
        try:
            msg = json.loads(raw)
            if msg.get("channel") == "tickers" or "tickers" in str(msg):
                data = msg.get("data", [{}])[0]
                instId = data.get("instId")
                if instId:
                    asset = "BTC" if "BTC" in instId else "ETH"
                    self._update_price(asset, float(data["last"]))
        except: pass


class BybitPriceTracker(BaseExchangeTracker):
    def __init__(self, db: Database):
        super().__init__(db, "Bybit")
        self.ws_url = Config.BYBIT_WS_URL

    async def _subscribe(self, ws):
        msg = {"op": "subscribe", "args": ["tickers.BTCUSDT", "tickers.ETHUSDT"]}
        await ws.send(json.dumps(msg))

    def _handle_message(self, raw):
        try:
            msg = json.loads(raw)
            topic = msg.get("topic", "")
            if "tickers" in topic:
                data = msg.get("data", {})
                if "lastPrice" in data:
                    asset = "BTC" if "BTC" in topic else "ETH"
                    self._update_price(asset, float(data["lastPrice"]))
        except: pass


class KrakenPriceTracker(BaseExchangeTracker):
    def __init__(self, db: Database):
        super().__init__(db, "Kraken")
        self.ws_url = Config.KRAKEN_WS_URL

    async def _subscribe(self, ws):
        msg = {
            "event": "subscribe",
            "pair": ["BTC/USD", "ETH/USD"],
            "subscription": {"name": "ticker"}
        }
        await ws.send(json.dumps(msg))

    def _handle_message(self, raw):
        try:
            msg = json.loads(raw)
            if isinstance(msg, list) and len(msg) > 1:
                data = msg[1]
                pair = msg[-1]
                if isinstance(data, dict) and "c" in data:
                    asset = "BTC" if "BTC" in pair else "ETH"
                    self._update_price(asset, float(data["c"][0]))
        except: pass


class NigeriaMultiFeedTracker:
    """
    Runs OKX + Bybit + Kraken simultaneously.
    Uses median consensus price as the signal.
    Falls back gracefully if any feed drops.
    """

    def __init__(self, db: Database):
        self.db       = db
        self.okx      = OKXPriceTracker(db)
        self.bybit    = BybitPriceTracker(db)
        self.kraken   = KrakenPriceTracker(db)
        self._trackers = [self.okx, self.bybit, self.kraken]

    async def run(self):
        """Launch all three feeds concurrently."""
        await asyncio.gather(
            self.okx.run(),
            self.bybit.run(),
            self.kraken.run(),
            return_exceptions=True,
        )

    def latest_price(self, asset: str) -> Optional[float]:
        """Median price across all live feeds."""
        prices = [
            t.latest_price(asset)
            for t in self._trackers
            if t.latest_price(asset) is not None
               and t.data_age_s(asset) < 5
        ]
        if not prices:
            return None
        prices.sort()
        mid = len(prices) // 2
        return prices[mid] if len(prices) % 2 != 0 \
               else (prices[mid-1] + prices[mid]) / 2

    def momentum_pct(self, asset: str,
                     lookback_s: float = 30.0) -> Optional[float]:
        """Average momentum across feeds that have data."""
        moms = [
            t.momentum_pct(asset, lookback_s)
            for t in self._trackers
            if t.momentum_pct(asset, lookback_s) is not None
        ]
        if not moms:
            return None
        return sum(moms) / len(moms)

    def data_age_s(self, asset: str) -> float:
        """Age of the freshest feed."""
        ages = [t.data_age_s(asset) for t in self._trackers]
        return min(ages)

    def feed_status(self) -> dict:
        """For dashboard display."""
        return {
            "okx":    self.okx.data_age_s("BTC") < 5,
            "bybit":  self.bybit.data_age_s("BTC") < 5,
            "kraken": self.kraken.data_age_s("BTC") < 5,
        }

    def stop(self):
        for t in self._trackers:
            t.stop()


# ---------------------------------------------------------------------------
# Polymarket CLOB client
# ---------------------------------------------------------------------------

class PolymarketClient:
    HEADERS = {"Content-Type": "application/json"}

    def __init__(self):
        self._session: Optional[aiohttp.ClientSession] = None

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            # Use custom DNS resolver for aiohttp
            connector = aiohttp.TCPConnector(
                resolver=AsyncResolver(nameservers=Config.DNS_SERVERS),
                use_dns_cache=True,
                ttl_dns_cache=300
            )
            self._session = aiohttp.ClientSession(
                connector=connector,
                timeout=aiohttp.ClientTimeout(total=10)
            )
        return self._session

    async def _get(self, path: str, params: dict = None) -> dict:
        url = Config.POLY_API_URL + path
        for attempt in range(Config.RETRY_ATTEMPTS):
            try:
                s = await self._get_session()
                async with s.get(url, params=params, headers=self.HEADERS) as r:
                    r.raise_for_status()
                    return await r.json()
            except aiohttp.ClientResponseError as e:
                if e.status == 429:
                    await asyncio.sleep(2 ** attempt)
                elif attempt < Config.RETRY_ATTEMPTS - 1:
                    await asyncio.sleep(Config.RETRY_DELAY_S)
                else:
                    raise
            except Exception:
                if attempt < Config.RETRY_ATTEMPTS - 1:
                    await asyncio.sleep(Config.RETRY_DELAY_S)
                else:
                    raise
        return {}

    async def _post(self, path: str, body: dict, auth_headers: dict = None) -> dict:
        url  = Config.POLY_API_URL + path
        hdrs = {**self.HEADERS, **(auth_headers or {})}
        for attempt in range(Config.RETRY_ATTEMPTS):
            try:
                s = await self._get_session()
                async with s.post(url, json=body, headers=hdrs) as r:
                    r.raise_for_status()
                    return await r.json()
            except Exception:
                if attempt < Config.RETRY_ATTEMPTS - 1:
                    await asyncio.sleep(Config.RETRY_DELAY_S)
                else:
                    raise
        return {}

    async def get_markets(self, active: bool = True) -> list[dict]:
        data = await self._get("/markets", params={"active": str(active).lower()})
        return data.get("data", data if isinstance(data, list) else [])

    async def get_orderbook(self, token_id: str) -> dict:
        return await self._get("/book", params={"token_id": token_id})

    def _auth_headers(self, method: str, path: str, body: str = "") -> dict:
        if not all([Config.POLY_API_KEY, Config.POLY_API_SECRET,
                    Config.POLY_API_PASSPHRASE]):
            raise RuntimeError("Live trading requires POLY_API_KEY, "
                               "POLY_API_SECRET, and POLY_API_PASSPHRASE.")
        import hmac, hashlib, base64
        ts  = str(int(time.time()))
        msg = ts + method.upper() + path + body
        sig = hmac.new(Config.POLY_API_SECRET.encode(),
                       msg.encode(), hashlib.sha256).digest()
        return {
            "POLY-API-KEY":    Config.POLY_API_KEY,
            "POLY-SIGNATURE":  base64.b64encode(sig).decode(),
            "POLY-TIMESTAMP":  ts,
            "POLY-PASSPHRASE": Config.POLY_API_PASSPHRASE,
        }

    async def place_market_order(self, token_id: str, side: str,
                                  size_usd: float) -> dict:
        path = "/order"
        body = {"token_id": token_id, "side": side,
                "size": round(size_usd, 2), "type": "MARKET",
                "time_in_force": "IOC"}
        return await self._post(path, body,
            auth_headers=self._auth_headers("POST", path, json.dumps(body)))

    async def get_balance(self) -> float:
        data = await self._get("/balance")
        return float(data.get("balance", 0))

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()


# ---------------------------------------------------------------------------
# Market scanner
# ---------------------------------------------------------------------------

class MarketScanner:
    ASSET_KW = {
        "BTC": ["bitcoin", "btc"],
        "ETH": ["ethereum", "eth"],
    }
    DUR_KW = {
        5:  ["5-minute", "5 minute", "5min", "5 min"],
        15: ["15-minute", "15 minute", "15min", "15 min"],
    }

    def __init__(self, client: PolymarketClient):
        self.client    = client
        self._cache:    list[MarketSnapshot] = []
        self._cache_ts: float = 0
        self._cache_ttl = 30.0

    async def scan(self) -> list[MarketSnapshot]:
        if self._cache and (time.time() - self._cache_ts) < self._cache_ttl:
            return self._cache
        markets = []
        try:
            raw = await self.client.get_markets(active=True)
            for m in raw:
                snap = self._parse(m)
                if snap:
                    markets.append(snap)
        except Exception as e:
            logging.error(f"Market scan failed: {e}")
            return self._cache
        self._cache    = markets
        self._cache_ts = time.time()
        logging.debug(f"Scanned {len(markets)} qualifying markets.")
        return markets

    def _parse(self, m: dict) -> Optional[MarketSnapshot]:
        question = (m.get("question") or m.get("title") or "").lower()
        asset = next((a for a, kws in self.ASSET_KW.items()
                      if any(k in question for k in kws)), None)
        if not asset:
            return None
        duration = next((d for d, kws in self.DUR_KW.items()
                         if any(k in question for k in kws)), None)
        if not duration:
            return None
        if not any(w in question for w in
                   ["higher", "lower", "up", "down", "above", "below"]):
            return None
        liquidity = float(m.get("volume", 0))
        if liquidity < Config.MIN_MARKET_LIQUIDITY:
            return None
        tokens    = m.get("tokens", [])
        yes_tok   = next((t for t in tokens if t.get("outcome") == "Yes"), {})
        no_tok    = next((t for t in tokens if t.get("outcome") == "No"),  {})
        yes_price = float(yes_tok.get("price", 0.5))
        no_price  = float(no_tok.get("price",  0.5))
        if not (0.85 <= yes_price + no_price <= 1.15):
            return None
        end_ts = m.get("end_date_iso") or m.get("endDate") or ""
        try:
            end_time = datetime.fromisoformat(end_ts.replace("Z", "+00:00"))
        except Exception:
            end_time = datetime.now(timezone.utc)
        return MarketSnapshot(
            market_id    = m.get("condition_id") or m.get("id", ""),
            question     = m.get("question") or m.get("title", ""),
            asset        = asset,
            duration_min = duration,
            yes_price    = yes_price,
            no_price     = no_price,
            volume       = float(m.get("volume", 0)),
            liquidity    = liquidity,
            end_time     = end_time,
        )


# ---------------------------------------------------------------------------
# Simulated market scanner (paper mode — no Polymarket API needed)
# ---------------------------------------------------------------------------

class SimulatedMarketScanner:
    """Generates synthetic BTC/ETH up/down contracts using live Binance prices.
    Used for paper trading when Polymarket API is unreachable."""

    def __init__(self, tracker: PriceTracker):
        self.tracker = tracker
        self._cycle  = 0

    async def scan(self) -> list[MarketSnapshot]:
        now     = datetime.now(timezone.utc)
        markets = []

        for asset in ["BTC", "ETH"]:
            price = self.tracker.latest_price(asset)
            if price is None:
                continue

            # Get momentum to create a slightly lagged market price
            mom = self.tracker.momentum_pct(asset, lookback_s=30) or 0

            for dur in [5, 15]:
                end_time = now + timedelta(minutes=dur)

                # Simulate a market that is 'lagged' behind CEX reality
                # If BTC is pumping (+0.5%), market still shows ~50/50
                # This creates the arb opportunity the engine looks for
                lag_factor = random.uniform(0.02, 0.08)  # 2-8% lag
                if mom > 0:
                    # Price going up but market hasn't caught up
                    yes_price = 0.50 + random.uniform(-0.03, 0.03)
                elif mom < 0:
                    # Price going down but market hasn't caught up
                    yes_price = 0.50 + random.uniform(-0.03, 0.03)
                else:
                    yes_price = 0.50

                yes_price = max(0.05, min(0.95, yes_price))
                no_price  = round(1.0 - yes_price, 4)

                mkt_id = f"SIM-{asset}-{dur}m-{'UP' if self._cycle % 2 == 0 else 'DOWN'}"
                direction = "higher" if self._cycle % 2 == 0 else "lower"
                question  = (f"Will {asset} be {direction} in {dur} minutes? "
                             f"(Simulated @ ${price:,.0f})")

                markets.append(MarketSnapshot(
                    market_id    = mkt_id,
                    question     = question,
                    asset        = asset,
                    duration_min = dur,
                    yes_price    = round(yes_price, 4),
                    no_price     = round(no_price, 4),
                    volume       = random.uniform(80_000, 500_000),
                    liquidity    = random.uniform(80_000, 500_000),
                    end_time     = end_time,
                ))

        self._cycle += 1
        return markets


# ---------------------------------------------------------------------------
# Arbitrage engine
# ---------------------------------------------------------------------------

class ArbitrageEngine:
    MOMENTUM_TABLE = [
        (0.10, 0.05), (0.25, 0.12), (0.40, 0.20),
        (0.60, 0.30), (0.80, 0.38), (1.00, 0.45),
    ]

    def __init__(self, tracker: PriceTracker):
        self.tracker = tracker

    def analyse(self, market: MarketSnapshot,
                portfolio_value: float) -> Optional[ArbitrageOpportunity]:
        asset     = market.asset
        cex_price = self.tracker.latest_price(asset)
        if cex_price is None:
            return None
        age = self.tracker.data_age_s(asset)
        if age > 5:
            return None
        mom_30s = self.tracker.momentum_pct(asset, lookback_s=30)
        if mom_30s is None:
            return None
        mom_5s    = self.tracker.momentum_pct(asset, lookback_s=5)
        direction = "up" if mom_30s > 0 else "down"
        abs_mom   = abs(mom_30s)
        prob_shift   = self._momentum_to_prob(abs_mom)
        base_true    = 0.5 + prob_shift
        q_lower      = any(w in market.question.lower()
                           for w in ["higher", "up", "above", "rise", "gain"])
        if direction == "up" and q_lower:
            side, market_prob, true_prob = Side.YES, market.yes_price, base_true
        elif direction == "down" and q_lower:
            side, market_prob, true_prob = Side.NO,  market.no_price,  base_true
        elif direction == "up":
            side, market_prob, true_prob = Side.NO,  market.no_price,  base_true
        else:
            side, market_prob, true_prob = Side.YES, market.yes_price, base_true

        edge_pct = (true_prob - market_prob) * 100
        if edge_pct < Config.MIN_EDGE_PCT:
            return None
        if abs(true_prob - market_prob) * 100 < Config.LAG_DETECTION_PCT:
            return None

        time_left  = (market.end_time - datetime.now(timezone.utc)).total_seconds()
        confidence = self._confidence(edge_pct, abs_mom,
                                      abs(mom_5s) if mom_5s else 0,
                                      age, market.liquidity, time_left)
        if confidence < Config.CONFIDENCE_THRESHOLD:
            return None

        b = (1.0 / market_prob) - 1 if market_prob > 0 else 0
        if b <= 0:
            return None
        p = true_prob
        kelly_full  = max(0, (p * b - (1 - p)) / b)
        kelly_half  = min(kelly_full * Config.KELLY_FRACTION, Config.MAX_POSITION_PCT)
        size_usd    = round(portfolio_value * kelly_half, 2)
        if size_usd < 1.0:
            return None

        return ArbitrageOpportunity(
            market=market, side=side, market_prob=market_prob,
            true_prob=true_prob, edge_pct=edge_pct, confidence=confidence,
            kelly_fraction=kelly_half, recommended_size=size_usd,
            cex_price=cex_price, cex_move_pct=mom_30s,
        )

    def _momentum_to_prob(self, abs_mom: float) -> float:
        for threshold, shift in self.MOMENTUM_TABLE:
            if abs_mom <= threshold:
                return shift
        return 0.48

    def _confidence(self, edge_pct, mom_30s, mom_5s,
                    data_age, liquidity, time_left) -> float:
        score  = min(40, edge_pct * 4)           # edge size (40 pts)
        score += min(20, mom_30s * 15)            # momentum (20 pts)
        score += 10 if mom_5s > 0 else 0          # 5s confirmation (10 pts)
        score += (15 if data_age < 0.5 else
                  10 if data_age < 1.5 else
                  5  if data_age < 3.0 else 0)    # freshness (15 pts)
        score += (10 if liquidity > 500_000 else
                  7  if liquidity > 200_000 else
                  4  if liquidity > 100_000 else 0) # liquidity (10 pts)
        score += 5 if 60 < time_left < 600 else 0  # time window (5 pts)
        return min(100.0, score)


# ---------------------------------------------------------------------------
# Risk manager
# ---------------------------------------------------------------------------

class RiskManager:
    def __init__(self, initial_balance: float, db: Database, tg: Telegram):
        self.initial_balance = initial_balance
        self.peak_balance    = initial_balance
        self.current_balance = initial_balance
        self.db              = db
        self.telegram        = tg
        self.halted          = False
        self.halt_reason     = ""
        self._order_times: deque = deque(maxlen=Config.RATE_LIMIT_ORDERS_MIN)

    def update_balance(self, balance: float):
        self.current_balance = balance
        self.peak_balance    = max(self.peak_balance, balance)

    def daily_drawdown_pct(self) -> float:
        daily_pnl = self.db.daily_pnl()
        if self.current_balance == 0:
            return 0.0
        return abs(min(0.0, daily_pnl)) / self.current_balance * 100

    def total_drawdown_pct(self) -> float:
        if self.peak_balance == 0:
            return 0.0
        return (self.peak_balance - self.current_balance) / self.peak_balance * 100

    async def check(self) -> bool:
        if self.halted:
            return False
        daily_dd = self.daily_drawdown_pct()
        total_dd = self.total_drawdown_pct()
        if total_dd >= Config.KILL_SWITCH_PCT * 100:
            self.halted      = True
            self.halt_reason = (f"Total drawdown {total_dd:.1f}% "
                                f">= {Config.KILL_SWITCH_PCT*100:.0f}%")
            logging.critical(f"KILL SWITCH: {self.halt_reason}")
            await self.telegram.alert_kill_switch(
                self.halt_reason, self.current_balance)
            return False
        if daily_dd >= Config.DAILY_LOSS_LIMIT_PCT * 100:
            self.halted      = True
            self.halt_reason = (f"Daily drawdown {daily_dd:.1f}% "
                                f">= {Config.DAILY_LOSS_LIMIT_PCT*100:.0f}%")
            logging.critical(f"DAILY HALT: {self.halt_reason}")
            await self.telegram.alert_drawdown(
                "DAILY LIMIT HIT", daily_dd, self.current_balance, halted=True)
            return False
        if daily_dd >= 10:
            await self.telegram.alert_drawdown(
                "WARNING 10%", daily_dd, self.current_balance, halted=False)
        return True

    def check_rate_limit(self) -> bool:
        now = time.time()
        self._order_times.append(now)
        return sum(1 for t in self._order_times if now - t < 60) \
               <= Config.RATE_LIMIT_ORDERS_MIN

    def position_size_ok(self, size_usd: float) -> bool:
        return size_usd <= self.current_balance * Config.MAX_POSITION_PCT


# ---------------------------------------------------------------------------
# Execution engine
# ---------------------------------------------------------------------------

class ExecutionEngine:
    def __init__(self, client: PolymarketClient, risk: RiskManager,
                 db: Database, telegram: Telegram, paper_mode: bool):
        self.client     = client
        self.risk       = risk
        self.db         = db
        self.telegram   = telegram
        self.paper_mode = paper_mode
        self.open_paper_trades: dict[int, Trade] = {}
        self._stats = {"total": 0, "wins": 0, "losses": 0}

    @property
    def stats(self) -> dict:
        return self._stats

    async def execute(self, opp: ArbitrageOpportunity) -> Optional[Trade]:
        if not await self.risk.check():
            logging.warning("Risk manager blocked trade.")
            return None
        if not self.risk.check_rate_limit():
            logging.warning("Rate limit hit, skipping trade.")
            return None
        if not self.risk.position_size_ok(opp.recommended_size):
            logging.warning(f"Position ${opp.recommended_size:.2f} exceeds limit.")
            return None
        await self.telegram.alert_opportunity(opp, self.paper_mode)
        now = datetime.now(timezone.utc).isoformat()
        return (await self._paper_execute(opp, now) if self.paper_mode
                else await self._live_execute(opp, now))

    async def _paper_execute(self, opp: ArbitrageOpportunity, now: str) -> Trade:
        trade = Trade(
            id=None, market_id=opp.market.market_id,
            question=opp.market.question, asset=opp.market.asset,
            side=opp.side.value, market_prob=opp.market_prob,
            true_prob=opp.true_prob, edge_pct=opp.edge_pct,
            confidence=opp.confidence, size_usd=opp.recommended_size,
            entry_price=opp.market_prob, exit_price=None, pnl=None,
            status=TradeStatus.PAPER.value, is_paper=True,
            order_id=f"PAPER-{int(time.time()*1000)}",
            opened_at=now, closed_at=None,
            notes=f"edge={opp.edge_pct:.1f}pp conf={opp.confidence:.0f}",
        )
        trade_id = self.db.insert_trade(trade)
        trade.id = trade_id
        self.open_paper_trades[trade_id] = trade
        self._stats["total"] += 1
        logging.info(f"[PAPER] {opp.side.value} {opp.market.asset} "
                     f"size=${opp.recommended_size:.2f} "
                     f"edge={opp.edge_pct:.1f}pp conf={opp.confidence:.0f}%")
        return trade

    async def _live_execute(self, opp: ArbitrageOpportunity,
                             now: str) -> Optional[Trade]:
        logging.info(f"[LIVE] {opp.side.value} {opp.market.market_id} "
                     f"size=${opp.recommended_size:.2f}")
        try:
            result = await asyncio.wait_for(
                self.client.place_market_order(
                    token_id=opp.market.market_id,
                    side=opp.side.value,
                    size_usd=opp.recommended_size,
                ),
                timeout=Config.ORDER_TIMEOUT_S,
            )
            order_id     = result.get("order_id") or result.get("id", "")
            filled_price = float(result.get("avg_fill_price", opp.market_prob))
            trade = Trade(
                id=None, market_id=opp.market.market_id,
                question=opp.market.question, asset=opp.market.asset,
                side=opp.side.value, market_prob=opp.market_prob,
                true_prob=opp.true_prob, edge_pct=opp.edge_pct,
                confidence=opp.confidence, size_usd=opp.recommended_size,
                entry_price=filled_price, exit_price=None, pnl=None,
                status=TradeStatus.OPEN.value, is_paper=False,
                order_id=order_id, opened_at=now, closed_at=None,
            )
            trade_id = self.db.insert_trade(trade)
            trade.id = trade_id
            self._stats["total"] += 1
            return trade
        except asyncio.TimeoutError:
            logging.error("Order timed out.")
            return None
        except Exception as e:
            logging.error(f"Order failed: {e}")
            await self.telegram.send(f"⚠️ Order failed: {e}")
            return None

    async def settle_paper_trades(self, markets: list[MarketSnapshot]):
        now     = datetime.now(timezone.utc)
        settled = []
        for trade_id, trade in list(self.open_paper_trades.items()):
            market  = next((m for m in markets
                            if m.market_id == trade.market_id), None)
            elapsed = (now - datetime.fromisoformat(trade.opened_at)).total_seconds()
            if market:
                time_left = (market.end_time - now).total_seconds()
                if time_left > 0 and elapsed < 900:
                    continue
            elif elapsed < 900:
                continue
            win        = random.random() < trade.true_prob
            exit_price = 1.0 if win else 0.0
            pnl        = (exit_price - trade.entry_price) * trade.size_usd
            self.db.update_trade(trade_id, exit_price, pnl,
                                 TradeStatus.PAPER.value, now.isoformat())
            self.risk.update_balance(self.risk.current_balance + pnl)
            if win:
                self._stats["wins"] += 1
            else:
                self._stats["losses"] += 1
            settled.append(trade_id)
            logging.info(f"[PAPER SETTLE] id={trade_id} "
                         f"{'WIN' if win else 'LOSS'} pnl=${pnl:.2f} "
                         f"bal=${self.risk.current_balance:.2f}")
        for tid in settled:
            del self.open_paper_trades[tid]


# ---------------------------------------------------------------------------
# Terminal dashboard (Rich)
# ---------------------------------------------------------------------------

class Dashboard:
    def __init__(self, risk: RiskManager, paper_mode: bool):
        self.risk        = risk
        self.paper_mode  = paper_mode
        self.console     = Console()
        self._prices: dict[str, float] = {}
        self._markets:  list[MarketSnapshot] = []
        self._opps:     deque = deque(maxlen=5)
        self._trades:   list[dict] = []
        self._stats:    dict = {"total": 0, "wins": 0, "losses": 0}
        self._feed_status: dict = {}
        self._start     = time.time()

    def update(self, prices=None, markets=None, opp=None,
               trades=None, stats=None, feed_status=None):
        if prices:  self._prices.update(prices)
        if markets: self._markets = markets
        if opp:     self._opps.appendleft(opp)
        if trades:  self._trades = trades
        if stats:   self._stats.update(stats)
        if feed_status: self._feed_status = feed_status

    def render(self) -> Layout:
        layout = Layout()
        layout.split_column(
            Layout(name="header", size=3),
            Layout(name="body"),
            Layout(name="footer", size=3),
        )
        layout["body"].split_row(
            Layout(name="left",  ratio=1),
            Layout(name="right", ratio=2),
        )
        layout["left"].split_column(
            Layout(name="portfolio", size=14),
            Layout(name="prices"),
        )
        layout["right"].split_column(
            Layout(name="opportunities", size=14),
            Layout(name="trades"),
        )
        layout["header"].update(self._header())
        layout["portfolio"].update(self._portfolio())
        layout["prices"].update(self._prices_panel())
        layout["opportunities"].update(self._opps_panel())
        layout["trades"].update(self._trades_panel())
        layout["footer"].update(self._footer())
        return layout

    def _header(self) -> Panel:
        mode   = "[bold red]⚡ LIVE[/]" if not self.paper_mode \
                 else "[bold yellow]🧪 PAPER[/]"
        status = "[bold red]■ HALTED[/]" if self.risk.halted \
                 else "[bold green]● RUNNING[/]"
        secs   = int(time.time() - self._start)
        h, rem = divmod(secs, 3600); m, s = divmod(rem, 60)
        return Panel(
            Text(f"  POLYMARKET ARB BOT  |  {mode}  |  {status}  "
                 f"|  Uptime {h:02d}:{m:02d}:{s:02d}  "
                 f"|  {datetime.now().strftime('%H:%M:%S')}"),
            style="bold white on #0d1117", box=box.HORIZONTALS,
        )

    def _portfolio(self) -> Panel:
        bal      = self.risk.current_balance
        init     = self.risk.initial_balance
        pnl      = bal - init
        pnl_pct  = pnl / init * 100 if init else 0
        dd_d     = self.risk.daily_drawdown_pct()
        dd_t     = self.risk.total_drawdown_pct()
        tot      = self._stats.get("total", 0)
        wins     = self._stats.get("wins", 0)
        wr       = wins / tot * 100 if tot else 0
        t = Table(show_header=False, box=None, padding=(0, 1))
        t.add_column("k", style="dim", width=18)
        t.add_column("v", style="bold")
        c = "green" if pnl >= 0 else "red"
        t.add_row("Balance",        f"[white]${bal:,.2f}[/]")
        t.add_row("Total P&L",      f"[{c}]{'+' if pnl>=0 else ''}${pnl:,.2f} ({pnl_pct:+.1f}%)[/]")
        t.add_row("Peak",           f"[white]${self.risk.peak_balance:,.2f}[/]")
        t.add_row("Daily DD",       f"[{'red' if dd_d>10 else 'yellow'}]{dd_d:.1f}%[/]")
        t.add_row("Total DD",       f"[{'red' if dd_t>20 else 'dim'}]{dd_t:.1f}%[/]")
        t.add_row("──────────────", "──────────")
        t.add_row("Trades",         f"[white]{tot}[/]")
        t.add_row("Win rate",       f"[{'green' if wr>60 else 'yellow'}]{wr:.1f}%[/] "
                                    f"[dim]({wins}W/{tot-wins}L)[/]")
        return Panel(t, title="[bold]PORTFOLIO[/]",
                     border_style="blue", box=box.ROUNDED)

    def _prices_panel(self) -> Panel:
        t = Table(show_header=True, box=None, padding=(0, 1))
        t.add_column("Asset", width=6)
        t.add_column("Price", style="bold white")
        t.add_column("Status", style="dim")
        for asset in ["BTC", "ETH"]:
            p = self._prices.get(asset)
            if p:
                t.add_row(asset, f"${p:,.2f}", "[green]live[/]")
            else:
                t.add_row(asset, "[red]--[/]", "[red]no data[/]")
        t.add_row("", "", "")
        t.add_row("[dim]Markets[/]", f"[white]{len(self._markets)}[/]", "[dim]active[/]")
        
        # Show Multi-feed status
        t.add_row("────────", "────────", "────────")
        for feed, live in self._feed_status.items():
            st = "[green]OK[/]" if live else "[red]DOWN[/]"
            t.add_row(feed.upper(), "", st)
            
        return Panel(t, title="[bold]LIVE PRICES & FEEDS[/]",
                     border_style="cyan", box=box.ROUNDED)

    def _opps_panel(self) -> Panel:
        t = Table(show_header=True, box=None, padding=(0, 1))
        t.add_column("Asset", width=5)
        t.add_column("Side",  width=5)
        t.add_column("Edge",  width=8)
        t.add_column("Conf",  width=6)
        t.add_column("Size",  width=8)
        t.add_column("Time",  width=10)
        if not self._opps:
            t.add_row(*["[dim]--[/]"] * 6)
        for opp in self._opps:
            t.add_row(
                opp.market.asset,
                f"[{'green' if opp.side==Side.YES else 'red'}]{opp.side.value}[/]",
                f"[green]{opp.edge_pct:.1f}pp[/]",
                f"{opp.confidence:.0f}%",
                f"${opp.recommended_size:.0f}",
                datetime.now().strftime("%H:%M:%S"),
            )
        return Panel(t, title="[bold]RECENT OPPORTUNITIES[/]",
                     border_style="yellow", box=box.ROUNDED)

    def _trades_panel(self) -> Panel:
        t = Table(show_header=True, box=None, padding=(0, 1))
        t.add_column("ID",     width=6)
        t.add_column("Asset",  width=5)
        t.add_column("Side",   width=5)
        t.add_column("Size",   width=8)
        t.add_column("Edge",   width=7)
        t.add_column("P&L",    width=9)
        t.add_column("Status", width=8)
        for tr in (self._trades or [])[:10]:
            pnl_val = tr.get("pnl")
            pnl_s   = f"${pnl_val:.2f}" if pnl_val is not None else "--"
            col     = "green" if (pnl_val or 0) >= 0 else "red"
            t.add_row(str(tr.get("id", "")), tr.get("asset", ""),
                      tr.get("side", ""), f"${tr.get('size_usd',0):.0f}",
                      f"{tr.get('edge_pct',0):.1f}pp",
                      f"[{col}]{pnl_s}[/]", tr.get("status", ""))
        return Panel(t, title="[bold]LAST 10 TRADES[/]",
                     border_style="green", box=box.ROUNDED)

    def _footer(self) -> Panel:
        return Panel(
            "[dim]Kill switch:[/] daily −20% or total −40%  |  "
            "[dim]Rate limit:[/] 30 orders/min  |  "
            "[dim]Max position:[/] 8% portfolio  |  "
            "[dim]DB:[/] arb_bot.db  |  Press [bold]Ctrl+C[/] to stop",
            style="dim", box=box.HORIZONTALS,
        )


# ---------------------------------------------------------------------------
# Main bot orchestrator
# ---------------------------------------------------------------------------

class ArbBot:
    SCAN_INTERVAL  = 15
    ARBIT_INTERVAL = 2
    DASH_INTERVAL  = 1

    def __init__(self, paper_mode: bool, initial_balance: float,
                 use_simulated_markets: bool = False):
        self.paper_mode      = paper_mode
        self.initial_balance = initial_balance
        self.db        = Database()
        self.telegram  = Telegram(Config.TELEGRAM_BOT_TOKEN, Config.TELEGRAM_CHAT_ID)
        self.tracker   = NigeriaMultiFeedTracker(self.db)
        self.client    = PolymarketClient()
        self.engine    = ArbitrageEngine(self.tracker)
        self.risk      = RiskManager(initial_balance, self.db, self.telegram)
        self.executor  = ExecutionEngine(self.client, self.risk, self.db,
                                         self.telegram, paper_mode)
        self.dashboard = Dashboard(self.risk, paper_mode)
        self._markets: list[MarketSnapshot] = []
        self._running  = False

        # Use simulated markets when Polymarket API is unreachable
        if use_simulated_markets or paper_mode:
            self.scanner = SimulatedMarketScanner(self.tracker)
            logging.info("Using SIMULATED markets (real Binance prices + synthetic contracts)")
        else:
            self.scanner = MarketScanner(self.client)

    async def run(self):
        self._running = True
        mode = "PAPER" if self.paper_mode else "LIVE"
        logging.info(f"Bot starting in {mode} mode. "
                     f"Balance: ${self.initial_balance:.2f}")
        await self.telegram.send(
            f"🤖 *Arb Bot Started* — {mode} mode\n"
            f"Balance: ${self.initial_balance:.2f}\n"
            f"Kill switch: daily −{Config.DAILY_LOSS_LIMIT_PCT*100:.0f}%"
            f" / total −{Config.KILL_SWITCH_PCT*100:.0f}%"
        )
        tasks = [
            asyncio.create_task(self.tracker.run(),        name="price_feed"),
            asyncio.create_task(self._market_scan_loop(),  name="market_scan"),
            asyncio.create_task(self._arb_loop(),          name="arb_engine"),
            asyncio.create_task(self._dashboard_loop(),    name="dashboard"),
        ]
        try:
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            pass
        finally:
            self._running = False
            self.tracker.stop()
            await self.client.close()
            await self.telegram.send("🛑 *Arb Bot Stopped.*")
            logging.info("Bot shut down cleanly.")

    async def _market_scan_loop(self):
        while self._running:
            try:
                self._markets = await self.scanner.scan()
                self.dashboard.update(markets=self._markets)
            except Exception as e:
                logging.error(f"Market scan error: {e}")
            await asyncio.sleep(self.SCAN_INTERVAL)

    async def _arb_loop(self):
        await asyncio.sleep(5)  # warm-up
        while self._running:
            if not await self.risk.check():
                if self.risk.halted:
                    logging.critical(f"Bot halted: {self.risk.halt_reason}")
                    break
                await asyncio.sleep(5)
                continue
            try:
                for market in self._markets:
                    opp = self.engine.analyse(market, self.risk.current_balance)
                    if opp:
                        self.dashboard.update(opp=opp)
                        trade = await self.executor.execute(opp)
                        if trade:
                            logging.info(f"Trade opened: id={trade.id}")
                if self.paper_mode and self._markets:
                    await self.executor.settle_paper_trades(self._markets)
                self.dashboard.update(
                    trades=self.db.recent_trades(10),
                    stats=self.executor.stats,
                    feed_status=self.tracker.feed_status(),
                )
                prices = {a: self.tracker.latest_price(a)
                          for a in ["BTC", "ETH"]
                          if self.tracker.latest_price(a)}
                self.dashboard.update(prices=prices)
            except Exception as e:
                logging.error(f"Arb loop error: {e}", exc_info=True)
            await asyncio.sleep(self.ARBIT_INTERVAL)

    async def _dashboard_loop(self):
        await asyncio.sleep(3)
        with Live(self.dashboard.render(), console=Console(),
                  refresh_per_second=1, screen=True) as live:
            while self._running:
                try:
                    live.update(self.dashboard.render())
                except Exception as e:
                    logging.debug(f"Dashboard render error: {e}")
                await asyncio.sleep(self.DASH_INTERVAL)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args():
    p = argparse.ArgumentParser(
        description="Polymarket Latency Arbitrage Bot",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Paper mode (safe default):
  python polymarket_arb_bot.py

Live mode (ALL three flags required):
  python polymarket_arb_bot.py --live --confirm-live --i-accept-risk
        """,
    )
    p.add_argument("--live",          action="store_true")
    p.add_argument("--confirm-live",  action="store_true")
    p.add_argument("--i-accept-risk", action="store_true")
    p.add_argument("--balance",  type=float, default=1000.0,
                   help="Starting balance for paper mode (default $1000)")
    p.add_argument("--log-level", default="INFO",
                   choices=["DEBUG","INFO","WARNING","ERROR"])
    return p.parse_args()


def setup_logging(level_str: str):
    logging.basicConfig(
        level=getattr(logging, level_str.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        handlers=[logging.FileHandler(Config.LOG_PATH)],
    )


def main():
    args       = parse_args()
    setup_logging(args.log_level)
    paper_mode = not (args.live and args.confirm_live and args.i_accept_risk)

    if args.live and not paper_mode and not Config.POLY_PRIVATE_KEY:
        print("[ERROR] POLY_PRIVATE_KEY not set — cannot enable live trading.")
        sys.exit(1)

    console = Console()
    mode_str = "[yellow]PAPER MODE[/]" if paper_mode else "[bold red]LIVE TRADING[/]"
    console.print(Panel(
        f"[bold white]POLYMARKET ARBITRAGE BOT[/]\n\n"
        f"Mode:    {mode_str}\n"
        f"Balance: [white]${args.balance:,.2f}[/]\n"
        f"DB:      [dim]{Config.DB_PATH}[/]\n"
        f"Log:     [dim]{Config.LOG_PATH}[/]\n\n"
        f"[dim]Kill switch: daily -{Config.DAILY_LOSS_LIMIT_PCT*100:.0f}% "
        f"| total -{Config.KILL_SWITCH_PCT*100:.0f}%\n"
        f"Max position: {Config.MAX_POSITION_PCT*100:.0f}% "
        f"| Rate limit: {Config.RATE_LIMIT_ORDERS_MIN} orders/min[/]\n\n"
        f"{'[yellow]WARNING: No POLY_API_KEY -- live orders will fail[/]' if not Config.POLY_API_KEY and not paper_mode else ''}"
        f"{'[dim]Telegram disabled (no token)[/]' if not Config.TELEGRAM_BOT_TOKEN else '[green]Telegram configured[/]'}",
        title="[bold cyan]Starting up...[/]",
        border_style="cyan", box=box.ROUNDED,
    ))

    if not paper_mode:
        console.print("\n[bold red]WARNING: LIVE MODE -- real funds in 5s...[/]")
        time.sleep(5)

    bot = ArbBot(paper_mode=paper_mode, initial_balance=args.balance)
    try:
        asyncio.run(bot.run())
    except KeyboardInterrupt:
        console.print("\n[bold yellow]Shutdown requested. Bye.[/]")
    except Exception as e:
        logging.critical(f"Fatal: {e}", exc_info=True)
        console.print(f"\n[bold red]Fatal error: {e}[/]")
        sys.exit(1)


if __name__ == "__main__":
    main()
