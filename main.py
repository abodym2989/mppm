#!/usr/bin/env python3
"""
moon_scalp_bot.py — BTC 1000x mean-reversion scalp bot for moon.com

- While a trade is open  -> does nothing (platform TP/SL closes it)
- When flat             -> samples price every POLL_SECONDS
- Price spikes >= ENTRY_Z above rolling mean -> SHORT (expect reversion)
- Price dips  >= ENTRY_Z below rolling mean  -> LONG  (expect reversion)
- TP is sized so a ~TP_USD_MOVE BTC move in your favor closes at +tp% margin
- SL at -SL_MARGIN_PCT% of margin (~0.1% adverse move at 1000x)

Safety: cooldown, max trades/hour, daily loss limit, auth-failure stop.
"""

import json
import logging
import time
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import requests

# ------------------------------- CONFIG ------------------------------------
ACCESS_TOKEN = "6649ced5fcbe3c4056aefc49745a3e05db8256000b494fae57b66e6949ce113867bd92a28882260e1eb8454888d8fd38"   # from X-Access-Token header
COOKIE       = "_ga=GA1.1.1276363762.1788470782;"   # full Cookie header from capture

API_URL     = "https://moon.com/_api/graphqlv2"
CURRENCY    = "mooney"
SYMBOL_CODE = "BTC"
SYMBOL_UUID = "eedad81f-c0a0-4653-9a10-4b2495bb877b"  # from activeTradeList.symbol.id

AMOUNT        = 2000.0   # margin per trade (mooney)
LEVERAGE      = 1000
TP_USD_MOVE   = 100.0    # take profit when BTC moves this much in your favor
SL_MARGIN_PCT = 100.0    # stop loss: -100% of margin

POLL_SECONDS        = 2.0
PRICE_WINDOW_SEC    = 90       # rolling window for the mean
ENTRY_Z             = 0.0009   # 0.09% deviation needed to enter
MIN_SIGNALS         = 15       # price samples required before first trade
COOLDOWN_SECONDS    = 30
MAX_TRADES_PER_HOUR = 10
DAILY_LOSS_LIMIT    = 400.0    # mooney — bot stops after approx this loss
DRY_RUN             = False    # True = log signals, don't actually trade

LOG_FILE = Path("moon_scalp_trades.jsonl")
# ----------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.StreamHandler(), logging.FileHandler("moon_scalp_bot.log")],
)
log = logging.getLogger("moonbot")

FRAGMENTS = """
  fragment TradeSymbolRiskSettingsFields on AssetRiskSettings {
    marketImpactBaseRate
    marketImpactLiquidityUnit
    marketImpactMinMove
    marketImpactMinPayoutFactor
    marketImpactMoveExponent
    marketImpactReferenceMove
    marketImpactSizeExponent
  }

  fragment TradeSymbolFields on Symbol {
    code
    id
    name
    price
    updatedAt
  }

  fragment TradeDataFields on TradeData {
    active
    amount
    amountIncludingFee
    autoLossPercent
    autoProfitPercent
    closePrice
    closureReason
    createdAt
    currency
    updatedAt
    openFee
    openReason
    rollingFee
    id
    kind
    leverage
    openPrice
    payoutMultiplierIncludingFee
    pnl
    status
    symbol { ...TradeSymbolFields }
    bet { iid }
  }

  fragment TradeDataWithRiskFields on TradeData {
    ...TradeDataFields
    symbol {
      ...TradeSymbolFields
      riskSettings { ...TradeSymbolRiskSettingsFields }
    }
  }
"""

QUERY_ACTIVE = FRAGMENTS + """
  query GetUserActiveTradeList($currency: MoonCurrencyEnum!) {
    user {
      activeTradeList(limit: 100, currency: $currency) {
        ...TradeDataWithRiskFields
      }
    }
  }
"""

MUTATION_OPEN = FRAGMENTS + """
  mutation OpenTrade(
    $amount: Float!
    $kind: TradeTypeEnum!
    $leverage: Float!
    $symbol: String!
    $autoLossPercent: Float
    $autoProfitPercent: Float
    $currency: MoonCurrencyEnum
  ) {
    openTrade(
      amount: $amount
      kind: $kind
      leverage: $leverage
      symbol: $symbol
      autoLossPercent: $autoLossPercent
      autoProfitPercent: $autoProfitPercent
      currency: $currency
    ) {
      ...TradeDataWithRiskFields
    }
  }
"""

# moon.com schema: Query.symbol(symbol: String!)  (confirmed from server error)
PRICE_QUERY = """
  query SymbolPrice($symbol: String!) {
    symbol(symbol: $symbol) { code price updatedAt }
  }
"""


class AuthError(Exception):
    pass


class ApiError(Exception):
    pass


@dataclass
class PricePoint:
    ts: float
    price: float


class MoonBot:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "Accept": "application/graphql-response+json, application/json",
            "Content-Type": "application/json",
            "X-Access-Token": ACCESS_TOKEN,
            "X-Language": "en",
            "Origin": "https://moon.com",
            "Referer": "https://moon.com/bet",
            "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                           "AppleWebKit/537.36 (KHTML, like Gecko) "
                           "Chrome/154.0.0.0 Safari/537.36"),
        })
        if COOKIE:
            self.session.headers["Cookie"] = COOKIE

        self.history = deque()
        self.open_trades = {}          # id -> {kind, open_price, last_pnl}
        self.trade_times = deque()     # timestamps of opened trades (rate limit)
        self.last_open_ts = 0.0
        self.consec_losses = 0
        self.total_trades = 0
        self.day = datetime.now(timezone.utc).date()
        self.day_pnl = 0.0
        self.last_price = None
        self._warned_price_fallback = False

    # ------------------------- GraphQL plumbing -------------------------
    def gql(self, query, variables, operation):
        payload = {"query": query, "variables": variables,
                   "operationName": operation}
        for attempt in range(3):
            try:
                r = self.session.post(API_URL, json=payload, timeout=15)
            except requests.RequestException as e:
                log.warning("network error (%s), retry %d/3", e, attempt + 1)
                time.sleep(3)
                continue

            if r.status_code in (401, 403) and "text/html" not in r.headers.get("Content-Type", ""):
                raise AuthError(f"HTTP {r.status_code} — token/cookie expired. Re-capture and restart.")
            if r.status_code == 403:  # Cloudflare challenge — usually transient
                log.warning("Cloudflare 403, retry %d/3", attempt + 1)
                time.sleep(5)
                continue
            if r.status_code >= 500:
                time.sleep(3)
                continue
            if r.status_code != 200:
                raise ApiError(f"HTTP {r.status_code}: {r.text[:200]}")

            data = r.json()
            if data.get("errors"):
                raise ApiError(str(data["errors"])[:300])
            return data.get("data") or {}
        raise ApiError("request failed after 3 retries")

    # ------------------------------ data --------------------------------
    def get_active_trades(self):
        data = self.gql(QUERY_ACTIVE, {"currency": CURRENCY}, "GetUserActiveTradeList")
        return data["user"]["activeTradeList"] or []

    def get_price(self):
        """Prefer moon.com's own price (UUID first, then code); fall back to Binance."""
        for ident in (SYMBOL_UUID, SYMBOL_CODE):
            try:
                data = self.gql(PRICE_QUERY, {"symbol": ident}, "SymbolPrice")
                sym = data.get("symbol")
                if sym and sym.get("price"):
                    return float(sym["price"]), "moon"
            except ApiError as e:
                log.info("moon price query failed for %s (%s)", ident[:8], e)
        r = requests.get("https://api.binance.com/api/v3/ticker/price",
                         params={"symbol": "BTCUSDT"}, timeout=10)
        return float(r.json()["price"]), "binance"

    def open_trade(self, kind, price):
        # TP% of margin such that a TP_USD_MOVE BTC move cashes out:
        #   move% = TP_USD/price ; pnl% = move% * leverage  (in % units)
        tp_pct = round(TP_USD_MOVE / price * 100 * LEVERAGE, 2)
        variables = {
            "symbol": SYMBOL_CODE,
            "amount": AMOUNT,
            "kind": kind,
            "leverage": LEVERAGE,
            "autoProfitPercent": tp_pct,
            "autoLossPercent": SL_MARGIN_PCT,
            "currency": CURRENCY,
        }
        log.info(">>> OPEN %s %s @ ~%.2f | tp=%.1f%% margin (=$%d move) sl=-%.0f%% | fee=%.0f mooney",
                 kind.upper(), SYMBOL_CODE, price, tp_pct, TP_USD_MOVE,
                 SL_MARGIN_PCT, AMOUNT * 0.01)
        if DRY_RUN:
            return {"id": "dry-run", "openPrice": price, "pnl": None}
        data = self.gql(MUTATION_OPEN, variables, "OpenTrade")
        return data["openTrade"]

    # ---------------------------- strategy -------------------------------
    def signal(self, price):
        """Mean reversion: spike above window mean -> SHORT, dip -> LONG."""
        now = time.time()
        while self.history and now - self.history[0].ts > PRICE_WINDOW_SEC:
            self.history.popleft()
        if len(self.history) < MIN_SIGNALS:
            return None, 0.0
        mean = sum(p.price for p in self.history) / len(self.history)
        dev = (price - mean) / mean
        if dev >= ENTRY_Z:
            return "short", dev
        if dev <= -ENTRY_Z:
            return "long", dev
        return None, dev

    def estimate_pnl(self, trade, price):
        if trade.get("pnl") is not None:
            return float(trade["pnl"])
        side = 1 if trade["kind"] == "long" else -1
        return (price - trade["openPrice"]) / trade["openPrice"] * LEVERAGE * AMOUNT * side

    # ----------------------------- state ---------------------------------
    def reconcile(self, active, price):
        """Detect closed trades, update daily PnL from last-known pnl."""
        ids = {t["id"] for t in active}
        for tid, info in list(self.open_trades.items()):
            if tid not in ids:
                pnl = info.get("last_pnl", 0.0)
                self.day_pnl += pnl
                self.total_trades += 1
                self.consec_losses = self.consec_losses + 1 if pnl < 0 else 0
                log.info("CLOSED %s %s | last pnl≈%+.2f | day=%+.2f consec_losses=%d",
                         info["kind"].upper(), tid[:8], pnl, self.day_pnl, self.consec_losses)
                with LOG_FILE.open("a") as f:
                    f.write(json.dumps({"ts": time.time(), "event": "close",
                                        "id": tid, "kind": info["kind"],
                                        "est_pnl": pnl}) + "\n")
                del self.open_trades[tid]

        for t in active:
            if t["id"] not in self.open_trades:
                self.open_trades[t["id"]] = {"kind": t["kind"],
                                             "open_price": t["openPrice"],
                                             "last_pnl": 0.0}
                log.info("Trade already open: %s %s @ %.2f", t["kind"].upper(), t["id"][:8], t["openPrice"])
            self.open_trades[t["id"]]["last_pnl"] = self.estimate_pnl(t, price) if price else 0.0

    def can_trade(self):
        now = time.time()
        while self.trade_times and now - self.trade_times[0] > 3600:
            self.trade_times.popleft()
        if now - self.last_open_ts < COOLDOWN_SECONDS * (1 + self.consec_losses):
            return False, f"cooldown (consec losses: {self.consec_losses})"
        if len(self.trade_times) >= MAX_TRADES_PER_HOUR:
            return False, f"rate limit ({MAX_TRADES_PER_HOUR}/h)"
        return True, ""

    # ------------------------------ main ---------------------------------
    def run(self):
        log.info("Bot started | %s %dx | amount=%.0f | TP=$%d move | DRY_RUN=%s",
                 SYMBOL_CODE, LEVERAGE, AMOUNT, TP_USD_MOVE, DRY_RUN)
        while True:
            try:
                # new day -> reset counters
                today = datetime.now(timezone.utc).date()
                if today != self.day:
                    self.day, self.day_pnl = today, 0.0

                active = self.get_active_trades()

                # refresh price (needed for pnl tracking even while a trade is open)
                try:
                    price, src = self.get_price()
                    self.last_price = price
                except Exception as e:
                    price, src = self.last_price, "stale"
                    log.warning("price fetch failed: %s", e)

                self.reconcile(active, price)

                if self.day_pnl <= -DAILY_LOSS_LIMIT:
                    log.error("Daily loss limit hit (%.2f). Stopping.", self.day_pnl)
                    break

                if active:
                    t = active[0]
                    log.info("OPEN position %s %s | entry=%.2f | now=%.2f | pnl≈%+.2f | day=%+.2f",
                             t["kind"].upper(), t["id"][:8], t["openPrice"],
                             price or 0, self.open_trades.get(t["id"], {}).get("last_pnl", 0.0),
                             self.day_pnl)
                    time.sleep(POLL_SECONDS)
                    continue

                # ---- flat: build history and look for entry ----
                if price:
                    self.history.append(PricePoint(time.time(), price))
                    side, dev = self.signal(price)
                    log.info("flat | %.2f (%s) | window dev %+.4f%% | samples %d/%d",
                             price, src, dev * 100, len(self.history), MIN_SIGNALS)
                    if side:
                        ok, reason = self.can_trade()
                        if not ok:
                            log.info("signal %s ignored: %s", side.upper(), reason)
                        else:
                            trade = self.open_trade(side, price)
                            self.open_trades[trade["id"]] = {
                                "kind": side, "open_price": trade["openPrice"], "last_pnl": 0.0}
                            self.last_open_ts = time.time()
                            self.trade_times.append(self.last_open_ts)
                            with LOG_FILE.open("a") as f:
                                f.write(json.dumps({"ts": self.last_open_ts, "event": "open",
                                                    "id": trade["id"], "kind": side,
                                                    "price": price, "dev": dev}) + "\n")
                time.sleep(POLL_SECONDS)

            except AuthError as e:
                log.error("FATAL: %s", e)
                break
            except ApiError as e:
                log.error("API error: %s — pausing 10s", e)
                time.sleep(10)
            except Exception:
                log.exception("unexpected error — pausing 10s")
                time.sleep(10)


if __name__ == "__main__":
    MoonBot().run()
