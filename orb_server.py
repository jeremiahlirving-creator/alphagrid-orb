import asyncio, os, json, logging, uuid
from contextlib import asynccontextmanager
from datetime import datetime, date, time
from typing import Optional
from zoneinfo import ZoneInfo
from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from dotenv import load_dotenv
import aiohttp

load_dotenv()

# Upstash Redis for persistent state across Railway restarts
try:
    from upstash import redis_set_json, redis_get_json
    UPSTASH_ENABLED = True
except ImportError:
    UPSTASH_ENABLED = False
    import logging
    logging.getLogger("orb_bot").warning("⚠️ upstash.py not found — state will not persist across restarts")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
logger = logging.getLogger("orb_bot")

EST = ZoneInfo("America/New_York")

# ── ORB STRATEGY CONFIG ───────────────────────────────────────────────────────
# Opening Range Breakout — ES/MES only, NY session
# Rules:
#   1. Build opening range from 8:00–8:15 AM ET (first 15 mins)
#   2. First tick at 8:00 = candle open; last tick before 8:15 = candle close
#   3. OB midpoint = (candle open + candle close) / 2
#   4. Check directional bias: price above/below prior day close
#   5. Only trade breakouts IN BIAS DIRECTION
#   6. TWO WINDOWS per day:
#      W1 (early breakout): 8:15–9:30 ET  — breakout of OR high/low, max 1 trade
#      W2 (break & retest): 9:30–10:30 ET — retest of OR high, OR low, or OB midpoint, max 1 trade
#   7. SL: opposite side of opening range
#   8. TP: 1.5× the opening range size
#   9. News filter: no trades within 30 mins of major release
#  11. OR size filter: skip if OR < 3pts or > 30pts

ORB_BUILD_START  = time(8,  0)
ORB_BUILD_END    = time(8, 15)
ORB_W1_START     = time(8, 15)
ORB_W1_END       = time(9, 30)
ORB_W2_START     = time(9, 30)
ORB_W2_END       = time(10, 30)

OR_TP_MULTIPLIER = 1.5
MAX_OR_SIZE_PTS  = 20.0
MIN_OR_SIZE_PTS  = 3.0
BREAKOUT_CONFIRM = 0.5        # pts beyond OR edge to confirm W1 breakout
RETEST_TOLERANCE = 0.75       # pts — how close price must get to retest level to trigger W2

IS_EVAL_MODE     = True
PROFIT_TARGET    = 1_500.0
MAX_EOD_LOSS     = -1_000.0
INTRADAY_TRAIL   = -1_000.0
MLL_LOCK_AT      =    100.0
PAYOUT_BUFFER    =  1_100.0
PAYOUT_THRESHOLD =    500.0
CONSISTENCY_CAP  =    0.50
MAX_DAY_LOSS     = -99_999.0

TIER1_NEWS_TIMES: list[tuple[int,int]] = [
    (8, 30),
    (10, 0),
    (14, 0),
    (14, 30),
]
NEWS_BLOCK_MINS = 30

CONTRACTS   = 5
POINT_VALUE = 5.0

# ── ENV ───────────────────────────────────────────────────────────────────────
PMT_URL        = os.getenv("PMT_WEBHOOK_URL",     "https://api.pickmytrade.trade/v2/add-trade-data-latest?t=18504")
PMT_TOKEN      = os.getenv("PMT_TOKEN",           "")
PMT_ACCOUNT    = os.getenv("PMT_ACCOUNT_ID",      "54155940")
TG_TOKEN       = os.getenv("TELEGRAM_BOT_TOKEN",  "")
TG_CHAT        = os.getenv("TELEGRAM_CHAT_ID",    "")
TRADOVATE_ACCT = os.getenv("TRADOVATE_ACCOUNT_ID","MFFUEVRPD505461066")

# ── RUNTIME STATE ─────────────────────────────────────────────────────────────
price_es   = 0.0
ws_clients = []
trades     = []

# ── OPENING RANGE + ORDER BLOCK STATE ────────────────────────────────────────
class ORBState:
    def __init__(self):
        self.reset()

    def reset(self):
        # Opening range
        self.or_high        = None
        self.or_low         = None
        self.or_size        = None
        self.or_locked      = False
        # Order block candle (8:00–8:15 body)
        self.ob_open        = None    # first tick at 8:00 ET
        self.ob_close       = None    # last tick before 8:15 ET
        self.ob_mid         = None    # (ob_open + ob_close) / 2
        # Retest levels for W2 (sorted, set at OR lock)
        self.retest_levels  = []      # list of {"level": float, "label": str}
        # Bias
        self.prior_close    = None
        self.bias           = None
        # Filters
        self.or_skip        = False
        # Breakout triggers (W1)
        self.breakout_high  = None
        self.breakout_low   = None
        # W2 retest tracking
        self.w2_level_hit   = None    # which level triggered W2
        # Per-window trade flags
        self.w1_traded      = False
        self.w1_trade_dir   = None
        self.w2_traded      = False
        self.w2_trade_dir   = None
        self.day_date       = date.today()
        logger.info("🔄 ORB state reset for new day")

    def check_reset(self):
        if date.today() != self.day_date:
            self.reset()

    @property
    def traded_today(self):
        return self.w1_traded and self.w2_traded

    @property
    def trade_dir(self):
        if self.w2_traded:  return self.w2_trade_dir
        if self.w1_traded:  return self.w1_trade_dir
        return None

    def update_or(self, price: float, now: datetime):
        h, m = now.hour, now.minute
        in_build = (h == 8 and m < 15)
        at_lock  = (h == 8 and m == 15)

        if in_build:
            # Capture candle open on very first tick
            if self.ob_open is None:
                self.ob_open = price
                logger.info(f"📌 OB open captured: {price:.2f}")
            # Track OR high/low
            if self.or_high is None:
                self.or_high = price
                self.or_low  = price
            else:
                self.or_high = max(self.or_high, price)
                self.or_low  = min(self.or_low,  price)
            # Always update close (last tick before lock)
            self.ob_close = price

        elif at_lock and not self.or_locked and self.or_high is not None:
            self.or_size   = self.or_high - self.or_low
            self.or_locked = True

            # Calculate OB midpoint
            if self.ob_open is not None and self.ob_close is not None:
                self.ob_mid = round((self.ob_open + self.ob_close) / 2, 2)
                logger.info(f"📐 OB: open={self.ob_open:.2f} close={self.ob_close:.2f} mid={self.ob_mid:.2f}")

            active_min = orb_tuner.min_or_size
            active_max = orb_tuner.max_or_size
            if self.or_size > active_max:
                self.or_skip = True
                logger.warning(f"⚠️ OR skip — too wide: {self.or_size:.1f}pts (max={active_max}pts)")
            elif self.or_size < active_min:
                self.or_skip = True
                logger.warning(f"⚠️ OR skip — too tight: {self.or_size:.1f}pts (min={active_min}pts)")
            else:
                self.breakout_high = self.or_high + BREAKOUT_CONFIRM
                self.breakout_low  = self.or_low  - BREAKOUT_CONFIRM

                # Bias from prior close
                if self.prior_close:
                    mid = (self.or_high + self.or_low) / 2
                    self.bias = "BUY" if mid > self.prior_close else "SELL"
                else:
                    self.bias = None

                # Build W2 retest levels
                levels = [
                    {"level": self.or_high, "label": "OR High"},
                    {"level": self.or_low,  "label": "OR Low"},
                ]
                if self.ob_mid is not None:
                    levels.append({"level": self.ob_mid, "label": "OB Mid"})
                self.retest_levels = levels

                logger.info(
                    f"🎯 OR locked: H={self.or_high:.2f} L={self.or_low:.2f} "
                    f"Size={self.or_size:.1f}pts | Bias={self.bias} | OB Mid={self.ob_mid}"
                )

    def current_window(self, now: datetime) -> Optional[int]:
        h, m   = now.hour, now.minute
        mins   = h * 60 + m
        if 8*60+15 <= mins < 9*60+30:  return 1
        if 9*60+30 <= mins <= 10*60+30: return 2
        return None

    # ── W1: early breakout ────────────────────────────────────────────────────
    def check_breakout_w1(self, price: float, tuner_bias: str = None) -> Optional[str]:
        if not self.or_locked or self.or_skip or self.w1_traded:
            return None
        active_bias = tuner_bias if tuner_bias and tuner_bias != "BOTH" else self.bias
        if price >= self.breakout_high and (active_bias in ("BUY", None)):
            return "BUY"
        if price <= self.breakout_low  and (active_bias in ("SELL", None)):
            return "SELL"
        return None

    # ── W2: break & retest ────────────────────────────────────────────────────
    def check_retest_w2(self, price: float, tuner_bias: str = None) -> tuple[Optional[str], Optional[str]]:
        """
        Returns (direction, level_label) or (None, None).
        W2 break & retest logic:
          - OR High retest (BUY): price previously broke ABOVE OR High, now retests from above
          - OR Low retest (SELL): price previously broke BELOW OR Low, now retests from below
          - OB Mid: retest in bias direction
        Only fires if price has already extended BEYOND the level first.
        """
        if not self.or_locked or self.or_skip or self.w2_traded:
            return None, None
        active_bias = tuner_bias if tuner_bias and tuner_bias != "BOTH" else self.bias

        for lvl in self.retest_levels:
            level = lvl["level"]
            label = lvl["label"]
            dist  = abs(price - level)
            if dist > RETEST_TOLERANCE:
                continue

            # OR High retest → BUY (price broke above OR High, now pulling back to it)
            # Price must be approaching from ABOVE (just broken level retest)
            if label == "OR High":
                if price <= level and price >= level - RETEST_TOLERANCE and (active_bias in ("BUY", None)):
                    return "BUY", label

            # OR Low retest → SELL (price broke below OR Low, now bouncing back to it)
            # Price must be approaching from BELOW (just broken level retest)
            elif label == "OR Low":
                if price >= level and price <= level + RETEST_TOLERANCE and (active_bias in ("SELL", None)):
                    return "SELL", label

            # OB Mid → trade in bias direction
            elif label == "OB Mid":
                if active_bias == "BUY" and price <= level and price >= level - RETEST_TOLERANCE:
                    return "BUY", label
                if active_bias == "SELL" and price >= level and price <= level + RETEST_TOLERANCE:
                    return "SELL", label

        return None, None

    def mark_traded(self, window: int, direction: str):
        if window == 1:
            self.w1_traded    = True
            self.w1_trade_dir = direction
        elif window == 2:
            self.w2_traded    = True
            self.w2_trade_dir = direction

    def tp_price(self, direction: str, entry: float, tp_mult: float = None) -> float:
        mult   = tp_mult or OR_TP_MULTIPLIER
        tp_pts = self.or_size * mult
        return entry + tp_pts if direction == "BUY" else entry - tp_pts

    def sl_price(self, direction: str) -> float:
        return self.or_low if direction == "BUY" else self.or_high

    def dollar_tp(self, direction: str, entry: float, tp_mult: float = None) -> float:
        mult   = tp_mult or OR_TP_MULTIPLIER
        tp_pts = self.or_size * mult
        return tp_pts * CONTRACTS * POINT_VALUE

    def dollar_sl(self) -> float:
        return self.or_size * CONTRACTS * POINT_VALUE

    def status(self) -> dict:
        return {
            "or_high":       self.or_high,
            "or_low":        self.or_low,
            "or_size":       round(self.or_size, 2) if self.or_size else None,
            "or_locked":     self.or_locked,
            "ob_open":       self.ob_open,
            "ob_close":      self.ob_close,
            "ob_mid":        self.ob_mid,
            "retest_levels": self.retest_levels,
            "or_skip":       self.or_skip,
            "bias":          self.bias,
            "prior_close":   self.prior_close,
            "breakout_high": self.breakout_high,
            "breakout_low":  self.breakout_low,
            "w1_traded":     self.w1_traded,
            "w1_trade_dir":  self.w1_trade_dir,
            "w2_traded":     self.w2_traded,
            "w2_trade_dir":  self.w2_trade_dir,
            "w2_level_hit":  self.w2_level_hit,
            "traded_today":  self.traded_today,
            "trade_dir":     self.trade_dir,
        }

orb = ORBState()

# ── ORB SELF-TUNING ENGINE ────────────────────────────────────────────────────
ORB_TUNE_EVERY = 10

class ORBTradeRecord:
    def __init__(self, direction, or_size, entry_time_mins, pnl, won, bias, window, level_label=None):
        self.direction       = direction
        self.or_size         = or_size
        self.entry_time_mins = entry_time_mins
        self.pnl             = pnl
        self.won             = won
        self.bias            = bias
        self.window          = window
        self.level_label     = level_label
        self.ts              = datetime.now(EST).strftime("%Y-%m-%d %H:%M ET")

class ORBTuner:
    def __init__(self):
        self.records: list[ORBTradeRecord] = []
        self.trades_since_tune = 0
        self.tune_count        = 0
        self.tp_multiplier     = OR_TP_MULTIPLIER
        self.min_or_size       = MIN_OR_SIZE_PTS
        self.max_or_size       = MAX_OR_SIZE_PTS
        self.breakout_confirm  = BREAKOUT_CONFIRM
        self.retest_tolerance  = RETEST_TOLERANCE
        self.direction_bias    = None

    def record(self, rec: ORBTradeRecord):
        self.records.append(rec)
        self.trades_since_tune += 1

    def _wr(self, subset):
        if not subset: return None
        return sum(1 for r in subset if r.won) / len(subset)

    def tune(self) -> list[str]:
        self.tune_count       += 1
        self.trades_since_tune = 0
        changes = []
        recent  = self.records[-30:]
        if len(recent) < 5:
            return changes

        wr = self._wr(recent)

        # TP multiplier
        old_tp = self.tp_multiplier
        if wr >= 0.65:
            new_tp = min(old_tp + 0.25, 2.5); reason = f"WR {wr:.0%} ≥ 65%"
        elif wr >= 0.50:
            new_tp = min(old_tp + 0.10, 2.5); reason = f"WR {wr:.0%} ≥ 50%"
        elif wr < 0.35:
            new_tp = max(old_tp - 0.20, 1.2); reason = f"WR {wr:.0%} < 35%"
        else:
            new_tp = old_tp; reason = None
        if reason and abs(new_tp - old_tp) > 0.05:
            self.tp_multiplier = round(new_tp, 2)
            changes.append(f"📐 TP mult {old_tp:.1f}×→{new_tp:.1f}× ({reason})")

        # Direction bias
        buys  = [r for r in recent if r.direction == "BUY"]
        sells = [r for r in recent if r.direction == "SELL"]
        buy_wr, sell_wr = self._wr(buys), self._wr(sells)
        old_bias = self.direction_bias
        if buy_wr and sell_wr and len(buys) >= 4 and len(sells) >= 4:
            if buy_wr >= 0.60 and sell_wr < 0.35:   self.direction_bias = "BUY"
            elif sell_wr >= 0.60 and buy_wr < 0.35: self.direction_bias = "SELL"
            elif buy_wr >= 0.45 and sell_wr >= 0.45: self.direction_bias = None
        if self.direction_bias != old_bias:
            changes.append(f"🧭 Bias {old_bias or 'BOTH'}→{self.direction_bias or 'BOTH'}")

        # W2 retest level performance
        w2 = [r for r in recent if r.window == 2]
        if len(w2) >= 4:
            for label in ["OR High", "OR Low", "OB Mid"]:
                subset = [r for r in w2 if r.level_label == label]
                if len(subset) >= 3:
                    lwr = self._wr(subset)
                    changes.append(f"🎯 W2 {label} WR: {lwr:.0%} ({len(subset)} trades)")

        # Retest tolerance
        if wr < 0.35:
            old = self.retest_tolerance
            self.retest_tolerance = min(old + 0.25, 2.0)
            if abs(self.retest_tolerance - old) > 0.1:
                changes.append(f"📍 Retest tolerance {old:.2f}→{self.retest_tolerance:.2f}pts")
        elif wr >= 0.65:
            old = self.retest_tolerance
            self.retest_tolerance = max(old - 0.10, 0.25)
            if abs(self.retest_tolerance - old) > 0.05:
                changes.append(f"📍 Retest tolerance {old:.2f}→{self.retest_tolerance:.2f}pts")

        # OR size filter tuning — find the OR size sweet spot
        # Analyze which OR sizes produced wins vs losses
        sized = [(r.or_size, r.won) for r in recent if r.or_size]
        if len(sized) >= 8:
            win_sizes  = [s for s, w in sized if w]
            loss_sizes = [s for s, w in sized if not w]
            if win_sizes and loss_sizes:
                avg_win_or  = sum(win_sizes)  / len(win_sizes)
                avg_loss_or = sum(loss_sizes) / len(loss_sizes)
                # Tighten min OR if small ranges are losing
                if avg_loss_or < avg_win_or and avg_loss_or < self.min_or_size + 2:
                    old_min = self.min_or_size
                    self.min_or_size = round(min(avg_win_or * 0.7, self.min_or_size + 1.0), 1)
                    if abs(self.min_or_size - old_min) >= 0.5:
                        changes.append(f"📏 Min OR size {old_min:.1f}→{self.min_or_size:.1f}pts (losing on small ORs)")
                # Tighten max OR if large ranges are losing
                if avg_loss_or > avg_win_or and avg_loss_or > self.max_or_size - 5:
                    old_max = self.max_or_size
                    self.max_or_size = round(max(avg_win_or * 1.3, self.max_or_size - 2.0), 1)
                    if abs(self.max_or_size - old_max) >= 0.5:
                        changes.append(f"📏 Max OR size {old_max:.1f}→{self.max_or_size:.1f}pts (losing on large ORs)")

        return changes

    def status(self) -> dict:
        recent = self.records[-30:]
        w1_rec = [r for r in recent if r.window == 1]
        w2_rec = [r for r in recent if r.window == 2]
        wins   = sum(1 for r in recent if r.won)
        return {
            "tune_count":        self.tune_count,
            "trades_since_tune": self.trades_since_tune,
            "next_tune_in":      max(0, ORB_TUNE_EVERY - self.trades_since_tune),
            "total_records":     len(self.records),
            "recent_wr":         round(wins / len(recent) * 100, 1) if recent else None,
            "w1_wr":             round(self._wr(w1_rec) * 100, 1) if w1_rec else None,
            "w2_wr":             round(self._wr(w2_rec) * 100, 1) if w2_rec else None,
            "tp_multiplier":     self.tp_multiplier,
            "min_or_size":       self.min_or_size,
            "max_or_size":       self.max_or_size,
            "breakout_confirm":  self.breakout_confirm,
            "retest_tolerance":  self.retest_tolerance,
            "direction_bias":    self.direction_bias or "BOTH",
            "windows":           "W1: 8:15–9:30 ET | W2: 9:30–10:30 ET",
        }

orb_tuner = ORBTuner()

# ── DAILY STATS ───────────────────────────────────────────────────────────────
class DayStats:
    def __init__(self):
        self.total_pnl     = 0.0
        self.day_pnl       = 0.0
        self.day_date      = date.today()
        self.eod_peak_pnl  = 0.0
        self.wins          = 0
        self.losses        = 0
        self.yesterday_pnl = 0.0
        self.payout_count  = 0
        self.intraday_peak = 0.0

    def _reset_day(self):
        today = date.today()
        if today != self.day_date:
            self.yesterday_pnl = self.day_pnl
            if self.total_pnl > self.eod_peak_pnl:
                self.eod_peak_pnl = self.total_pnl
            self.day_pnl       = 0.0
            self.intraday_peak = 0.0
            self.day_date      = today

    @property
    def trailing_floor(self): return self.eod_peak_pnl - abs(MAX_EOD_LOSS)

    @property
    def win_rate(self):
        t = self.wins + self.losses
        return self.wins / t if t else 1.0

    @property
    def total_trades(self): return self.wins + self.losses

    @property
    def trailing_floor_intraday(self): return self.intraday_peak + INTRADAY_TRAIL

    def record(self, pnl: float) -> bool:
        self._reset_day()
        self.total_pnl += pnl
        self.day_pnl   += pnl
        if pnl > 0: self.wins   += 1
        else:       self.losses += 1
        if self.total_pnl > self.intraday_peak:
            self.intraday_peak = self.total_pnl
        return self.total_pnl <= self.trailing_floor

    def can_trade(self, now: datetime = None) -> tuple[bool, str]:
        self._reset_day()
        if IS_EVAL_MODE and self.total_pnl <= self.trailing_floor:
            return False, f"EOD trailing drawdown hit (floor: ${self.trailing_floor:.0f})"
        if not IS_EVAL_MODE and self.total_pnl <= self.trailing_floor_intraday:
            return False, f"Intraday trailing hit (floor: ${self.trailing_floor_intraday:.0f})"
        if IS_EVAL_MODE and self.total_pnl >= PROFIT_TARGET:
            return False, f"Profit target reached! (${self.total_pnl:.0f})"
        if IS_EVAL_MODE and self.total_pnl > 0 and self.day_pnl > 0:
            if self.day_pnl >= self.total_pnl * CONSISTENCY_CAP:
                return False, f"Consistency cap hit (day ${self.day_pnl:.0f} > 50% of ${self.total_pnl:.0f})"
        if now:
            h, m = now.hour, now.minute
            curr = h * 60 + m
            for nh, nm in TIER1_NEWS_TIMES:
                if abs(curr - (nh * 60 + nm)) <= NEWS_BLOCK_MINS:
                    return False, f"Tier 1 news window ({nh:02d}:{nm:02d} ET ±{NEWS_BLOCK_MINS}min)"
        return True, "ok"

    def status(self) -> dict:
        return {
            "total_pnl":          round(self.total_pnl, 2),
            "day_pnl":            round(self.day_pnl, 2),
            "trailing_floor":     round(self.trailing_floor, 2),
            "drawdown_remaining": round(self.total_pnl - self.trailing_floor, 2),
            "day_loss_remaining": round(MAX_DAY_LOSS - self.day_pnl, 2),
            "wins":               self.wins,
            "losses":             self.losses,
            "win_rate":           round(self.win_rate * 100, 1),
            "total_trades":       self.total_trades,
            "payout_count":       self.payout_count,
            "to_payout":          round(max(0, PAYOUT_BUFFER + PAYOUT_THRESHOLD - self.total_pnl), 2),
            "yesterday_pnl":      round(self.yesterday_pnl, 2),
            "to_target":          round(max(0, PROFIT_TARGET - self.total_pnl), 2),
            "is_eval":            IS_EVAL_MODE,
        }

stats = DayStats()

# ── HELPERS ───────────────────────────────────────────────────────────────────
async def broadcast(msg: dict):
    dead = []
    for ws in ws_clients:
        try:    await ws.send_text(json.dumps(msg))
        except: dead.append(ws)
    for ws in dead:
        try:    ws_clients.remove(ws)
        except: pass

async def send_telegram(text: str):
    if not TG_TOKEN or not TG_CHAT:
        return
    url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
    try:
        async with aiohttp.ClientSession() as s:
            await s.post(url, json={"chat_id": TG_CHAT, "text": text,
                                    "parse_mode": "Markdown"},
                         timeout=aiohttp.ClientTimeout(total=5))
    except Exception as e:
        logger.warning(f"Telegram error: {e}")

# ── PMT WEBHOOK ───────────────────────────────────────────────────────────────
async def fire_pmt(direction: str, dollar_tp: float, dollar_sl: float) -> tuple[bool, str]:
    sl_pts = dollar_sl / CONTRACTS / POINT_VALUE
    tp_pts = dollar_tp / CONTRACTS / POINT_VALUE
    if direction.lower() == "buy":
        sl_price_calc = round(price_es - sl_pts, 2)
        tp_price_calc = round(price_es + tp_pts, 2)
    else:
        sl_price_calc = round(price_es + sl_pts, 2)
        tp_price_calc = round(price_es - tp_pts, 2)
    payload = {
        "symbol":                "MES1!",
        "strategy_name":         f"AlphaGrid_ORB_{direction}",
        "date":                  datetime.now(EST).strftime("%Y-%m-%dT%H:%M:%S"),
        "data":                  direction.lower(),
        "quantity":              str(CONTRACTS),
        "risk_percentage":       0,
        "price":                 str(price_es),
        "tp":                    tp_price_calc, "percentage_tp": 0,
        "dollar_tp":             0,
        "sl":                    sl_price_calc, "dollar_sl": 0,
        "percentage_sl":         0,
        "trail":                 0, "trail_stop": 0,
        "trail_trigger":         0, "trail_freq": 0,
        "update_tp":             False, "update_sl": False,
        "breakeven":             0, "breakeven_offset": 0,
        "token":                 PMT_TOKEN,
        "pyramid":               True,
        "same_direction_ignore": False,
        "reverse_order_close":   False,
        "multiple_accounts": [{
            "token":               PMT_TOKEN,
            "account_id":          TRADOVATE_ACCT,
            "risk_percentage":     0,
            "quantity_multiplier": 1,
        }]
    }
    headers = {
        "Content-Type": "application/json",
        "User-Agent":   "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Origin":       "https://www.pickmytrade.trade",
        "Referer":      "https://www.pickmytrade.trade/",
    }
    try:
        async with aiohttp.ClientSession() as s:
            async with s.post(PMT_URL, json=payload, headers=headers,
                              timeout=aiohttp.ClientTimeout(total=10)) as r:
                body = await r.text()
                return r.status == 200 and "success" in body.lower(), body
    except Exception as e:
        return False, str(e)

# ── SIGNAL ENGINE ─────────────────────────────────────────────────────────────
async def process_price(price: float, now: datetime):
    global price_es
    price_es = price
    h, m = now.hour, now.minute

    orb.check_reset()
    orb.update_or(price, now)

    # Log OR building progress
    if h == 8 and m in [0, 5, 10] and not orb.or_locked and orb.or_high:
        logger.info(f"📏 Building OR: H={orb.or_high:.2f} L={orb.or_low:.2f} @ {now.strftime('%H:%M ET')}")

    # OR locked notification at 8:15
    if h == 8 and m == 15 and orb.or_locked and not orb.or_skip:
        lvl_lines = "\n".join(f"  `{l['label']}`: `{l['level']:.2f}`" for l in orb.retest_levels)
        await send_telegram(
            f"🎯 *ORB Locked* — {now.strftime('%b %d')}\n"
            f"High: `{orb.or_high:.2f}` | Low: `{orb.or_low:.2f}`\n"
            f"Size: `{orb.or_size:.1f}pts` | Bias: `{orb.bias or 'NONE'}`\n"
            f"OB Open: `{orb.ob_open:.2f}` | Close: `{orb.ob_close:.2f}` | Mid: `{orb.ob_mid:.2f}`\n\n"
            f"🪟 *W1 triggers* (breakout)\n"
            f"  BUY: `>{orb.breakout_high:.2f}` | SELL: `<{orb.breakout_low:.2f}`\n\n"
            f"🪟 *W2 retest levels* (9:30–10:30 ET)\n"
            f"{lvl_lines}"
        )
    elif h == 8 and m == 15 and orb.or_locked and orb.or_skip:
        reason = f"OR size {orb.or_size:.1f}pts out of {MIN_OR_SIZE_PTS}–{MAX_OR_SIZE_PTS}pt range"
        await send_telegram(
            f"⏭️ *ORB Skipped* — {now.strftime('%b %d')}\n"
            f"Reason: {reason}\nNo trades today."
        )

    # W2 open alert at 9:30
    if h == 9 and m == 30 and orb.or_locked and not orb.or_skip:
        w1_result = f"W1: {'✅ ' + orb.w1_trade_dir if orb.w1_traded else '⬜ No trade'}"
        lvl_lines = "\n".join(f"  `{l['label']}`: `{l['level']:.2f}`" for l in orb.retest_levels)
        await send_telegram(
            f"🪟 *W2 Open — Break & Retest*\n"
            f"9:30–10:30 ET | {w1_result}\n\n"
            f"Watching for retest of:\n{lvl_lines}\n"
            f"Tolerance: ±`{orb_tuner.retest_tolerance}pts`"
        )

    # Determine active window
    window = orb.current_window(now)
    if window is None:
        return

    allowed, reason = stats.can_trade(now)
    if not allowed:
        return

    direction     = None
    level_label   = None

    if window == 1:
        direction = orb.check_breakout_w1(price, tuner_bias=orb_tuner.direction_bias)
    elif window == 2:
        direction, level_label = orb.check_retest_w2(price, tuner_bias=orb_tuner.direction_bias)

    if not direction:
        return

    dollar_tp = orb.dollar_tp(direction, price)
    dollar_sl = orb.dollar_sl()
    tp_price  = orb.tp_price(direction, price)
    sl_price  = orb.sl_price(direction)
    tp_pts    = orb.or_size * OR_TP_MULTIPLIER
    sl_pts    = orb.or_size

    window_label = "W1 Breakout" if window == 1 else f"W2 Retest ({level_label})"
    logger.info(
        f"🚀 ORB [{window_label}] {direction} @ {price:.2f} | "
        f"OR {orb.or_low:.2f}–{orb.or_high:.2f} ({orb.or_size:.1f}pts) | "
        f"TP +{tp_pts:.1f}pts (${dollar_tp:.0f}) | SL {sl_pts:.1f}pts (${dollar_sl:.0f})"
    )

    ok, body = await fire_pmt(direction, dollar_tp, dollar_sl)

    if ok:
        orb.mark_traded(window, direction)
        if window == 2:
            orb.w2_level_hit = level_label
        sig = {
            "id":          str(uuid.uuid4())[:8],
            "window":      window,
            "window_label": window_label,
            "direction":   direction,
            "entry":       price,
            "tp":          round(tp_price, 2),
            "sl":          round(sl_price, 2),
            "tp_pts":      round(tp_pts, 2),
            "sl_pts":      round(sl_pts, 2),
            "dollar_tp":   round(dollar_tp, 2),
            "dollar_sl":   round(dollar_sl, 2),
            "or_high":     orb.or_high,
            "or_low":      orb.or_low,
            "ob_mid":      orb.ob_mid,
            "or_size":     orb.or_size,
            "bias":        orb.bias,
            "level_label": level_label,
            "contracts":   CONTRACTS,
            "ts":          now.strftime("%H:%M ET"),
        }
        trades.insert(0, sig)
        await broadcast({"type": "trade", "sig": sig, "stats": stats.status()})
        wr_str = f"{stats.win_rate:.0%} ({stats.wins}W/{stats.losses}L)" if stats.total_trades else "—"
        level_line = f"📍 Level: `{level_label}`\n" if level_label else ""
        await send_telegram(
            f"🏛️ *ORB Trade Fired* ✅ [{window_label}]\n"
            f"MES {direction} @ `{price:.2f}`\n\n"
            f"📏 OR: `{orb.or_low:.2f}` – `{orb.or_high:.2f}` ({orb.or_size:.1f}pts)\n"
            f"{level_line}"
            f"🎯 TP: `{tp_price:.2f}` (`+{tp_pts:.1f}pts` / `+${dollar_tp:.0f}`)\n"
            f"🛑 SL: `{sl_price:.2f}` (`-{sl_pts:.1f}pts` / `-${dollar_sl:.0f}`)\n"
            f"📊 Bias: `{orb.bias or 'None'}` | Contracts: `{CONTRACTS}`\n"
            f"Win Rate: {wr_str} | Day P&L: `${stats.day_pnl:+.0f}`"
        )
    else:
        logger.error(f"ORB webhook failed: {body}")
        await send_telegram(f"⚠️ *ORB webhook failed* — {direction} [{window_label}]\n`{body[:150]}`")

# ── SCHEDULED REPORTS ─────────────────────────────────────────────────────────
async def report_premarket():
    s = stats.status()
    allowed, reason = stats.can_trade(datetime.now(EST))
    await send_telegram(
        f"⚡ *ORB Pre-Market Brief* — opens in 5 mins\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"📏 *Opening Range* builds 8:00–8:15 AM ET\n"
        f"🪟 W1 (early breakout):  8:15–9:30 ET\n"
        f"🪟 W2 (break & retest):  9:30–10:30 ET\n"
        f"   Retest levels: OR High | OR Low | OB Midpoint\n\n"
        f"💼 *Account*\n"
        f"  Total P&L: `${s['total_pnl']:+.2f}`\n"
        f"  Drawdown room: `${s['drawdown_remaining']:.0f}`\n\n"
        f"{'🟢 ARMED' if allowed else f'🔴 PAUSED — {reason}'}"
    )

async def report_eod():
    s     = stats.status()
    orb_s = orb.status()
    emoji = "🟢" if stats.day_pnl > 0 else "🔴" if stats.day_pnl < 0 else "⚪"
    skip  = ""
    if orb_s['or_skip']: skip = f"OR size skip ({orb_s['or_size']}pts)"
    w1_str = f"✅ {orb_s['w1_trade_dir']}" if orb_s['w1_traded'] else "⬜ None"
    w2_str = f"✅ {orb_s['w2_trade_dir']} @ {orb_s['w2_level_hit']}" if orb_s['w2_traded'] else "⬜ None"
    await send_telegram(
        f"{emoji} *ORB EOD Report* — {datetime.now(EST).strftime('%b %d, %Y')}\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"📊 *Today*\n"
        f"  W1 (breakout): `{w1_str}`\n"
        f"  W2 (retest):   `{w2_str}`\n"
        f"  Day P&L: `${stats.day_pnl:+.2f}`\n\n"
        f"📏 *OR* H:`{orb_s['or_high']}` L:`{orb_s['or_low']}` Size:`{orb_s['or_size']}pts`\n"
        f"📐 *OB Mid*: `{orb_s['ob_mid']}`\n"
        f"  Bias: `{orb_s['bias'] or 'N/A'}`\n\n"
        f"💼 *Account*\n"
        f"  Total P&L: `${s['total_pnl']:+.2f}`\n"
        f"  Win Rate: `{s['win_rate']}%` ({s['wins']}W/{s['losses']}L)\n"
        f"  Drawdown room: `${s['drawdown_remaining']:.0f}`\n"
        f"  To target: `${s['to_target']:.0f}`"
    )

# ── SCHEDULER ─────────────────────────────────────────────────────────────────
async def scheduler():
    sent_755 = False
    sent_eod = False
    last_date = date.today()
    while True:
        await asyncio.sleep(30)
        now   = datetime.now(EST)
        today = now.date()
        if today != last_date:
            sent_755 = False
            sent_eod = False
            last_date = today
        h, m = now.hour, now.minute
        if now.weekday() >= 5:   # skip weekends
            continue
        if h == 7  and m == 55 and not sent_755: sent_755 = True; await report_premarket()
        if h == 16 and m == 30 and not sent_eod: sent_eod = True; await report_eod()

# ── LIFESPAN ──────────────────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Restore prior close from Upstash Redis on startup
    try:
        data = await redis_get_json("alphagrid:orb:prior_close") if UPSTASH_ENABLED else None
        if data:
            saved_date = date.fromisoformat(data.get("date", "2000-01-01"))
            if (date.today() - saved_date).days <= 1:
                orb.prior_close = float(data["close"])
                logger.info(f"📂 Prior close restored from Redis: {orb.prior_close:.2f} (saved {saved_date})")
            else:
                logger.info(f"📂 Prior close too old ({saved_date}) — skipping")
        else:
            logger.info("📂 No prior close in Redis — waiting for TradingView alert")
    except Exception as e:
        logger.warning(f"📂 Could not restore prior close: {e}")

    task = asyncio.create_task(scheduler())
    logger.info("AlphaGrid ORB Bot — autonomous 🏛️")
    logger.info("OR: 8:00–8:15 ET | W1: 8:15–9:30 ET | W2: 9:30–10:30 ET (break & retest)")
    logger.info(f"W2 levels: OR High | OR Low | OB Midpoint | tolerance ±{RETEST_TOLERANCE}pts")
    yield
    task.cancel()

app = FastAPI(lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

# ── ENDPOINTS ─────────────────────────────────────────────────────────────────
@app.get("/health")
async def health():
    allowed, reason = stats.can_trade(datetime.now(EST))
    now  = datetime.now(EST)
    mins = now.hour * 60 + now.minute
    if mins < 8*60:                      phase = "PRE_MARKET"
    elif mins < 8*60+15:                 phase = "BUILDING_OR"
    elif mins < 9*60+30:                 phase = "WINDOW_1"
    elif mins <= 10*60+30:               phase = "WINDOW_2"
    else:                                phase = "CLOSED"
    return {
        "status":   "ok",
        "phase":    phase,
        "trading":  allowed,
        "reason":   reason,
        "price_es": price_es,
        "orb":      orb.status(),
        "tuner":    orb_tuner.status(),
        **stats.status(),
    }

@app.post("/price-update")
async def price_update(req: Request):
    body  = await req.json()
    inst  = body.get("ticker", "").upper().replace("1!", "").replace("!", "")
    price = float(body.get("price", 0))
    if inst not in ["MES", "ES"] or price <= 0:
        return {"ok": False, "reason": "not MES/ES"}
    now = datetime.now(EST)
    await process_price(price, now)
    await broadcast({"type": "price", "price": price, "orb": orb.status(), "stats": stats.status()})
    return {"ok": True, "price": price}

@app.post("/set-prior-close")
async def set_prior_close(req: Request):
    body  = await req.json()
    close = float(body.get("close", 0))
    if close <= 0:
        return {"ok": False}
    orb.prior_close = close
    logger.info(f"📌 Prior close set: {close:.2f}")
    # Persist to Upstash Redis — survives Railway restarts
    try:
        if UPSTASH_ENABLED:
            await redis_set_json("alphagrid:orb:prior_close", {
            "close": close,
            "date":  date.today().isoformat(),
            })
    except Exception as e:
        logger.warning(f"Could not save prior close to Redis: {e}")
    return {"ok": True, "prior_close": close}

class ResultPayload(BaseModel):
    pnl:             float
    won:             bool
    direction:       Optional[str]   = None
    or_size:         Optional[float] = None
    entry_time_mins: Optional[int]   = None
    window:          Optional[int]   = None
    level_label:     Optional[str]   = None
    note:            Optional[str]   = None

@app.post("/result")
async def record_result(p: ResultPayload):
    locked = stats.record(p.pnl)
    s      = stats.status()
    now_et = datetime.now(EST)
    rec = ORBTradeRecord(
        direction       = p.direction or "BUY",
        or_size         = p.or_size or (orb.or_size or 10.0),
        entry_time_mins = p.entry_time_mins or (now_et.hour * 60 + now_et.minute),
        pnl             = p.pnl,
        won             = p.won,
        bias            = orb.bias,
        window          = p.window or 1,
        level_label     = p.level_label,
    )
    orb_tuner.record(rec)
    if orb_tuner.trades_since_tune >= ORB_TUNE_EVERY:
        changes = orb_tuner.tune()
        ts = orb_tuner.status()
        if changes:
            await send_telegram(
                f"🧠 *ORB Auto-Tune* — Cycle #{orb_tuner.tune_count}\n"
                f"After {orb_tuner.total_records} trades ({ts['recent_wr']}% WR):\n\n"
                + "\n".join(f"  {c}" for c in changes)
            )
        else:
            await send_telegram(f"🧠 *ORB Auto-Tune #{orb_tuner.tune_count}* — No changes\nWR: {ts['recent_wr']}% ✅")
    await broadcast({"type": "result", "pnl": p.pnl, "stats": s})
    if locked:
        await send_telegram(
            f"⛔ *ORB BOT LOCKED — Drawdown Hit*\n"
            f"Total P&L: `${stats.total_pnl:.0f}` | Floor: `${stats.trailing_floor:.0f}`"
        )
    return {**s, "tuner": orb_tuner.status()}

@app.get("/stats")
async def get_stats():
    allowed, reason = stats.can_trade(datetime.now(EST))
    return {**stats.status(), "trading_allowed": allowed, "reason": reason, "orb": orb.status()}

@app.get("/tuner")
async def get_tuner():
    return orb_tuner.status()

@app.post("/reset-day")
@app.get("/reset-day")
async def reset_day():
    stats.day_pnl  = 0.0
    stats.day_date = date.today()
    orb.reset()
    return {"ok": True}

@app.post("/report/now")
async def report_now():
    await report_premarket()
    return {"ok": True}

@app.post("/report/eod")
async def report_eod_now():
    await report_eod()
    return {"ok": True}

@app.post("/test-trade")
async def test_trade():
    global price_es
    test_price = price_es if price_es > 0 else 7616.0
    price_es   = test_price
    sl_pts, tp_pts = 6.75, 13.25
    test_sl = sl_pts * CONTRACTS * POINT_VALUE
    test_tp = tp_pts * CONTRACTS * POINT_VALUE
    ok, body = await fire_pmt("buy", test_tp, test_sl)
    await send_telegram(
        f"🧪 *ORB Test Trade*\nMES BUY @ `{test_price:.2f}`\n"
        f"{'✅' if ok else '❌'} PMT: `{body[:100]}`"
    )
    return {"ok": ok, "body": body[:200], "price": test_price}

@app.websocket("/ws")
async def ws_ep(ws: WebSocket):
    await ws.accept()
    ws_clients.append(ws)
    await ws.send_text(json.dumps({
        "type":   "init",
        "price":  price_es,
        "orb":    orb.status(),
        "stats":  stats.status(),
        "trades": trades[:10],
        "config": {
            "contracts":         CONTRACTS,
            "or_build":          "8:00–8:15 AM ET",
            "window_1":          "8:15–9:30 ET — early breakout",
            "window_2":          "9:30–10:30 ET — break & retest",
            "w2_levels":         ["OR High", "OR Low", "OB Midpoint"],
            "retest_tolerance":  RETEST_TOLERANCE,
            "tp_multiplier":     OR_TP_MULTIPLIER,
            "or_size_range":     f"{MIN_OR_SIZE_PTS}–{MAX_OR_SIZE_PTS}pts",
        }
    }))
    try:
        while True: await ws.receive_text()
    except WebSocketDisconnect:
        try: ws_clients.remove(ws)
        except: pass
