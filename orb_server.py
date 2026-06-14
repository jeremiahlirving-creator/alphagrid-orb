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

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
logger = logging.getLogger("orb_bot")

EST = ZoneInfo("America/New_York")

# ── ORB STRATEGY CONFIG ───────────────────────────────────────────────────────
# Opening Range Breakout — ES/MES only, NY session
# Rules:
#   1. Build opening range from 8:00–8:15 AM ET (first 15 mins)
#   2. Check directional bias: price above/below prior day close
#   3. Only trade breakouts IN BIAS DIRECTION
#   4. Entry: price breaks beyond OR high (BUY) or OR low (SELL)
#   5. SL: opposite side of opening range
#   6. TP: 1.5× the opening range size
#   7. News filter: no trades within 30 mins of major release
#   8. Gap filter: skip if ES gaps more than 20pts at open
#   9. Volume filter: breakout bar volume > 20-bar avg (if available)
#  10. One trade per day maximum

ORB_BUILD_START  = time(8,  0)    # ET — start building opening range
ORB_BUILD_END    = time(8, 15)    # ET — opening range locked
ORB_TRADE_END    = time(9, 30)    # ET — no new entries after this
OR_GAP_MAX_PTS   = 20.0           # skip day if gap > 20pts
OR_TP_MULTIPLIER = 1.5            # TP = 1.5 × OR size
MAX_OR_SIZE_PTS  = 30.0           # skip if OR too wide (choppy open)
MIN_OR_SIZE_PTS  = 3.0            # skip if OR too tight (low conviction)
BREAKOUT_CONFIRM = 0.5            # price must break by 0.5pts beyond OR edge

# MFFU $25K Account — MFFUEVRPD505461066
# Eval:       Profit target $1,500 | Max EOD loss $1,000 | 50% consistency | 2 min days
# Sim-Funded: Intraday trailing $1,000 | MLL locks at +$100 | Max 3 mini / 30 micro
# Payout:     Daily | 90/10 split | $500 min | $1,100 buffer | No Tier 1 news

IS_EVAL_MODE     = True        # True = eval, False = sim-funded
PROFIT_TARGET    = 1_500.0     # eval profit target
MAX_EOD_LOSS     = -1_000.0    # EOD trailing drawdown limit
INTRADAY_TRAIL   = -1_000.0    # sim-funded intraday trailing
MLL_LOCK_AT      =    100.0    # trailing stops once +$100 profit reached (sim-funded)
PAYOUT_BUFFER    =  1_100.0    # sim-funded payout buffer
PAYOUT_THRESHOLD =    500.0    # min payout amount
CONSISTENCY_CAP  =    0.50     # eval only — no single day > 50% of total profits

# No daily loss limit on this account — EOD trailing is the protection
MAX_DAY_LOSS     = -99_999.0   # effectively disabled

# Tier 1 news times (ET) — no trades 30 mins before/after
# Add dates as needed when news calendar is known
TIER1_NEWS_TIMES: list[tuple[int,int]] = [
    # (hour, minute) ET — common recurring Tier 1 times
    (8, 30),   # CPI, NFP, Retail Sales, etc.
    (10, 0),   # ISM, Consumer Confidence
    (14, 0),   # FOMC decision (some days)
    (14, 30),  # FOMC press conference start
]
NEWS_BLOCK_MINS = 30  # block trades within this many mins of Tier 1

# Trade sizing — ES micro contracts
# ES point value = $50/pt, MES = $5/pt
# Using MES so sizing matches All Night Bot
CONTRACTS    = 2    # 2 MES contracts per signal (conservative start)
POINT_VALUE  = 5.0  # MES

# ── ENV ───────────────────────────────────────────────────────────────────────
PMT_URL        = os.getenv("PMT_WEBHOOK_URL",     "https://api.pickmytrade.trade/v2/add-trade-data-latest?t=18504")
PMT_TOKEN      = os.getenv("PMT_TOKEN",           "")
PMT_ACCOUNT    = os.getenv("PMT_ACCOUNT_ID",      "54155940")
TG_TOKEN       = os.getenv("TELEGRAM_BOT_TOKEN",  "")
TG_CHAT        = os.getenv("TELEGRAM_CHAT_ID",    "")
TRADOVATE_ACCT = os.getenv("TRADOVATE_ACCOUNT_ID","MFFUEVRPD505461066")

# ── RUNTIME STATE ─────────────────────────────────────────────────────────────
price_es    = 0.0       # latest ES/MES tick
ws_clients  = []
trades      = []

# ── OPENING RANGE STATE ───────────────────────────────────────────────────────
class ORBState:
    """
    Tracks daily opening range and trade state.
    Resets at midnight ET each day.
    """
    def __init__(self):
        self.reset()

    def reset(self):
        self.or_high       = None    # opening range high (8:00–8:15 AM)
        self.or_low        = None    # opening range low
        self.or_size       = None    # OR high - OR low
        self.or_locked     = False   # True after 8:15 AM
        self.prior_close   = None    # yesterday's close (for bias)
        self.bias          = None    # "BUY" or "SELL" or None
        self.gap_pts       = None    # gap from prior close to 8:00 AM open
        self.gap_skip      = False   # True if gap too large
        self.or_skip       = False   # True if OR size out of range
        self.traded_today  = False   # one trade per day
        self.trade_dir     = None    # direction of today's trade
        self.breakout_high = None    # OR high + confirm (BUY trigger)
        self.breakout_low  = None    # OR low - confirm (SELL trigger)
        self.day_date      = date.today()
        logger.info("🔄 ORB state reset for new day")

    def check_reset(self):
        today = date.today()
        if today != self.day_date:
            self.reset()

    def update_or(self, price: float, now: datetime):
        """Update opening range during build window (8:00–8:15 AM ET)."""
        h, m = now.hour, now.minute
        in_build = (h == 8 and m < 15)
        at_lock  = (h == 8 and m == 15)

        if in_build:
            if self.or_high is None:
                self.or_high = price
                self.or_low  = price
                # Record gap from prior close
                if self.prior_close:
                    self.gap_pts = abs(price - self.prior_close)
                    if self.gap_pts > OR_GAP_MAX_PTS:
                        self.gap_skip = True
                        logger.warning(f"⚠️ Gap skip — {self.gap_pts:.1f}pts gap at open")
            else:
                self.or_high = max(self.or_high, price)
                self.or_low  = min(self.or_low,  price)

        elif at_lock and not self.or_locked and self.or_high is not None:
            self.or_size = self.or_high - self.or_low
            self.or_locked = True

            # OR size filter
            if self.or_size > MAX_OR_SIZE_PTS:
                self.or_skip = True
                logger.warning(f"⚠️ OR skip — OR too wide: {self.or_size:.1f}pts")
            elif self.or_size < MIN_OR_SIZE_PTS:
                self.or_skip = True
                logger.warning(f"⚠️ OR skip — OR too tight: {self.or_size:.1f}pts")
            else:
                # Set breakout triggers
                self.breakout_high = self.or_high + BREAKOUT_CONFIRM
                self.breakout_low  = self.or_low  - BREAKOUT_CONFIRM

                # Set bias from prior close
                if self.prior_close:
                    mid = (self.or_high + self.or_low) / 2
                    self.bias = "BUY" if mid > self.prior_close else "SELL"
                else:
                    self.bias = None  # no bias filter if no prior close

                logger.info(
                    f"🎯 OR locked: H={self.or_high:.2f} L={self.or_low:.2f} "
                    f"Size={self.or_size:.1f}pts | Bias={self.bias} | "
                    f"Triggers: >{self.breakout_high:.2f} / <{self.breakout_low:.2f}"
                )

    def check_breakout(self, price: float) -> Optional[str]:
        """Returns 'BUY', 'SELL', or None."""
        if not self.or_locked or self.or_skip or self.gap_skip or self.traded_today:
            return None
        # Only trade in bias direction
        if price >= self.breakout_high and (self.bias == "BUY" or self.bias is None):
            return "BUY"
        if price <= self.breakout_low  and (self.bias == "SELL" or self.bias is None):
            return "SELL"
        return None

    def tp_price(self, direction: str, entry: float) -> float:
        tp_pts = self.or_size * OR_TP_MULTIPLIER
        return entry + tp_pts if direction == "BUY" else entry - tp_pts

    def sl_price(self, direction: str) -> float:
        return self.or_low if direction == "BUY" else self.or_high

    def dollar_tp(self, direction: str, entry: float) -> float:
        tp_pts = self.or_size * OR_TP_MULTIPLIER
        return tp_pts * CONTRACTS * POINT_VALUE

    def dollar_sl(self) -> float:
        sl_pts = self.or_size   # SL is width of OR
        return sl_pts * CONTRACTS * POINT_VALUE

    def status(self) -> dict:
        return {
            "or_high":       self.or_high,
            "or_low":        self.or_low,
            "or_size":       round(self.or_size, 2) if self.or_size else None,
            "or_locked":     self.or_locked,
            "or_skip":       self.or_skip,
            "gap_skip":      self.gap_skip,
            "gap_pts":       round(self.gap_pts, 2) if self.gap_pts else None,
            "bias":          self.bias,
            "prior_close":   self.prior_close,
            "traded_today":  self.traded_today,
            "trade_dir":     self.trade_dir,
            "breakout_high": self.breakout_high,
            "breakout_low":  self.breakout_low,
        }

orb = ORBState()

# ── DAILY STATS ───────────────────────────────────────────────────────────────
class DayStats:
    def __init__(self):
        self.total_pnl       = 0.0
        self.day_pnl         = 0.0
        self.day_date        = date.today()
        self.eod_peak_pnl    = 0.0
        self.wins            = 0
        self.losses          = 0
        self.yesterday_pnl   = 0.0
        self.payout_count    = 0
        self.intraday_peak   = 0.0

    def _reset_day(self):
        today = date.today()
        if today != self.day_date:
            self.yesterday_pnl = self.day_pnl
            if self.total_pnl > self.eod_peak_pnl:
                self.eod_peak_pnl = self.total_pnl
            self.day_pnl        = 0.0
            self.intraday_peak  = 0.0
            self.day_date       = today

    @property
    def trailing_floor(self): return self.eod_peak_pnl - EOD_TRAIL_MAX

    @property
    def win_rate(self):
        t = self.wins + self.losses
        return self.wins / t if t else 1.0

    @property
    def total_trades(self): return self.wins + self.losses

    def record(self, pnl: float) -> bool:
        self._reset_day()
        self.total_pnl += pnl
        self.day_pnl   += pnl
        if pnl > 0: self.wins   += 1
        else:       self.losses += 1
        if self.total_pnl > self.intraday_peak:
            self.intraday_peak = self.total_pnl
        return self.total_pnl <= self.trailing_floor

    @property
    def trailing_floor_intraday(self) -> float:
        """Sim-funded uses intraday trailing from peak intraday P&L."""
        return self.intraday_peak + INTRADAY_TRAIL

    def can_trade(self, now: datetime = None) -> tuple[bool, str]:
        self._reset_day()
        # EOD trailing drawdown (eval mode)
        if IS_EVAL_MODE and self.total_pnl <= self.trailing_floor:
            return False, f"EOD trailing drawdown hit (floor: ${self.trailing_floor:.0f})"
        # Intraday trailing (sim-funded)
        if not IS_EVAL_MODE and self.total_pnl <= self.trailing_floor_intraday:
            return False, f"Intraday trailing hit (floor: ${self.trailing_floor_intraday:.0f})"
        # Eval profit target
        if IS_EVAL_MODE and self.total_pnl >= PROFIT_TARGET:
            return False, f"Profit target reached! (${self.total_pnl:.0f})"
        # Consistency rule (eval only) — no single day > 50% of total profits
        if IS_EVAL_MODE and self.total_pnl > 0:
            max_day = self.total_pnl * CONSISTENCY_CAP
            if self.day_pnl >= max_day:
                return False, f"Consistency cap hit (day ${self.day_pnl:.0f} > 50% of ${self.total_pnl:.0f})"
        # News filter
        if now:
            h, m = now.hour, now.minute
            for nh, nm in TIER1_NEWS_TIMES:
                news_mins = nh * 60 + nm
                curr_mins = h * 60 + m
                if abs(curr_mins - news_mins) <= NEWS_BLOCK_MINS:
                    return False, f"Tier 1 news window ({nh:02d}:{nm:02d} ET ±{NEWS_BLOCK_MINS}min)"
        return True, "ok"

    def status(self) -> dict:
        t = self.wins + self.losses
        return {
            "total_pnl":          round(self.total_pnl, 2),
            "day_pnl":            round(self.day_pnl, 2),
            "trailing_floor":     round(self.trailing_floor, 2),
            "drawdown_remaining": round(self.total_pnl - self.trailing_floor, 2),
            "day_loss_remaining": round(MAX_DAY_LOSS - self.day_pnl, 2),
            "wins":               self.wins,
            "losses":             self.losses,
            "win_rate":           round(self.win_rate * 100, 1),
            "total_trades":       t,
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
    payload = {
        "symbol":                "MES1!",
        "strategy_name":         f"AlphaGrid_ORB_{direction}",
        "date":                  datetime.now(EST).strftime("%Y-%m-%dT%H:%M:%S"),
        "data":                  direction.lower(),
        "quantity":              str(CONTRACTS),
        "risk_percentage":       0,
        "price":                 str(price_es),
        "tp":                    0, "percentage_tp": 0,
        "dollar_tp":             dollar_tp,
        "sl":                    0, "dollar_sl": dollar_sl,
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

    # Log OR building progress every 5 mins
    if h == 8 and m in [0, 5, 10] and not orb.or_locked and orb.or_high:
        logger.info(f"📏 Building OR: H={orb.or_high:.2f} L={orb.or_low:.2f} @ {now.strftime('%H:%M ET')}")

    # Send OR summary when locked at 8:15
    if h == 8 and m == 15 and orb.or_locked and not orb.gap_skip and not orb.or_skip:
        await send_telegram(
            f"🎯 *ORB Locked* — {now.strftime('%b %d')}\n"
            f"High: `{orb.or_high:.2f}` | Low: `{orb.or_low:.2f}`\n"
            f"Size: `{orb.or_size:.1f}pts` | Bias: `{orb.bias or 'NONE'}`\n"
            f"BUY trigger: `>{orb.breakout_high:.2f}`\n"
            f"SELL trigger: `<{orb.breakout_low:.2f}`\n"
            f"TP mult: `{OR_TP_MULTIPLIER}×` = `{orb.or_size * OR_TP_MULTIPLIER:.1f}pts`"
        )
    elif h == 8 and m == 15 and orb.or_locked and (orb.gap_skip or orb.or_skip):
        reason = f"gap {orb.gap_pts:.1f}pts > {OR_GAP_MAX_PTS}pts" if orb.gap_skip else f"OR size {orb.or_size:.1f}pts out of {MIN_OR_SIZE_PTS}–{MAX_OR_SIZE_PTS}pt range"
        await send_telegram(
            f"⏭️ *ORB Skipped* — {now.strftime('%b %d')}\n"
            f"Reason: {reason}\nNo trades today."
        )

    # Only check for breakout during trade window
    if not (h == 8 and m >= 15) and not (h == 9 and m <= 30):
        return
    if h == 9 and m > 30:
        return

    # Check kill conditions
    allowed, reason = stats.can_trade(datetime.now(EST))
    if not allowed:
        return

    direction = orb.check_breakout(price)
    if not direction:
        return

    # Calculate TP and SL in dollars
    dollar_tp = orb.dollar_tp(direction, price)
    dollar_sl = orb.dollar_sl()
    tp_price  = orb.tp_price(direction, price)
    sl_price  = orb.sl_price(direction)
    tp_pts    = orb.or_size * OR_TP_MULTIPLIER
    sl_pts    = orb.or_size

    logger.info(
        f"🚀 ORB {direction} @ {price:.2f} | "
        f"OR {orb.or_low:.2f}–{orb.or_high:.2f} ({orb.or_size:.1f}pts) | "
        f"TP +{tp_pts:.1f}pts (${dollar_tp:.0f}) | SL {sl_pts:.1f}pts (${dollar_sl:.0f})"
    )

    ok, body = await fire_pmt(direction, dollar_tp, dollar_sl)

    if ok:
        orb.traded_today = True
        orb.trade_dir    = direction
        sig = {
            "id":        str(uuid.uuid4())[:8],
            "direction": direction,
            "entry":     price,
            "tp":        round(tp_price, 2),
            "sl":        round(sl_price, 2),
            "tp_pts":    round(tp_pts, 2),
            "sl_pts":    round(sl_pts, 2),
            "dollar_tp": round(dollar_tp, 2),
            "dollar_sl": round(dollar_sl, 2),
            "or_high":   orb.or_high,
            "or_low":    orb.or_low,
            "or_size":   orb.or_size,
            "bias":      orb.bias,
            "contracts": CONTRACTS,
            "ts":        now.strftime("%H:%M ET"),
        }
        trades.insert(0, sig)
        await broadcast({"type": "trade", "sig": sig, "stats": stats.status()})
        wr_str = f"{stats.win_rate:.0%} ({stats.wins}W/{stats.losses}L)" if stats.total_trades else "—"
        await send_telegram(
            f"🏛️ *ORB Trade Fired* ✅\n"
            f"MES {direction} @ `{price:.2f}`\n\n"
            f"📏 OR: `{orb.or_low:.2f}` – `{orb.or_high:.2f}` ({orb.or_size:.1f}pts)\n"
            f"🎯 TP: `{tp_price:.2f}` (`+{tp_pts:.1f}pts` / `+${dollar_tp:.0f}`)\n"
            f"🛑 SL: `{sl_price:.2f}` (`-{sl_pts:.1f}pts` / `-${dollar_sl:.0f}`)\n"
            f"📊 Bias: `{orb.bias or 'None'}` | Contracts: `{CONTRACTS}`\n"
            f"Win Rate: {wr_str} | Day P&L: `${stats.day_pnl:+.0f}`"
        )
    else:
        logger.error(f"ORB webhook failed: {body}")
        await send_telegram(f"⚠️ *ORB webhook failed* — {direction}\n`{body[:150]}`")

# ── SCHEDULED REPORTS ─────────────────────────────────────────────────────────
async def report_premarket():
    """7:55 AM ET — ORB setup brief before market opens."""
    s = stats.status()
    allowed, reason = stats.can_trade(datetime.now(EST))
    await send_telegram(
        f"⚡ *ORB Pre-Market Brief* — opens in 5 mins\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"📏 *Opening Range* builds 8:00–8:15 AM ET\n"
        f"🎯 Trade window: 8:15–9:30 AM ET\n"
        f"⚙️ Filters: gap <{OR_GAP_MAX_PTS}pts | OR {MIN_OR_SIZE_PTS}–{MAX_OR_SIZE_PTS}pts | bias-aligned only\n\n"
        f"💼 *Account*\n"
        f"  Total P&L: `${s['total_pnl']:+.2f}`\n"
        f"  Day loss room: `${s['day_loss_remaining']:.0f}`\n"
        f"  Drawdown room: `${s['drawdown_remaining']:.0f}`\n\n"
        f"{'🟢 ARMED — watching for ORB setup' if allowed else f'🔴 PAUSED — {reason}'}"
    )

async def report_eod():
    """4:30 PM ET — ORB daily summary."""
    s   = stats.status()
    orb_s = orb.status()
    day_emoji = "🟢" if stats.day_pnl > 0 else "🔴" if stats.day_pnl < 0 else "⚪"
    skip_reason = ""
    if orb_s['gap_skip']:    skip_reason = f"Gap skip ({orb_s['gap_pts']}pts)"
    elif orb_s['or_skip']:   skip_reason = f"OR size skip ({orb_s['or_size']}pts)"
    await send_telegram(
        f"{day_emoji} *ORB EOD Report* — {datetime.now(EST).strftime('%b %d, %Y')}\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"📊 *Today*\n"
        f"  Traded: `{'Yes — ' + orb_s['trade_dir'] if orb_s['traded_today'] else 'No' + (' (' + skip_reason + ')' if skip_reason else '')}`\n"
        f"  Day P&L: `${stats.day_pnl:+.2f}`\n\n"
        f"💼 *Account*\n"
        f"  Total P&L: `${s['total_pnl']:+.2f}`\n"
        f"  Win Rate: `{s['win_rate']}%` ({s['wins']}W/{s['losses']}L)\n"
        f"  Drawdown room: `${s['drawdown_remaining']:.0f}`\n"
        f"  To payout: `${s['to_payout']:.0f}`\n\n"
        f"📏 *Today's OR*\n"
        f"  H: `{orb_s['or_high']}` L: `{orb_s['or_low']}` Size: `{orb_s['or_size']}pts`\n"
        f"  Bias: `{orb_s['bias'] or 'N/A'}` | Gap: `{orb_s['gap_pts']}pts`"
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
        if h == 7  and m == 55 and not sent_755: sent_755 = True; await report_premarket()
        if h == 16 and m == 30 and not sent_eod: sent_eod = True; await report_eod()

# ── LIFESPAN ──────────────────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    task = asyncio.create_task(scheduler())
    logger.info("AlphaGrid ORB Bot — autonomous 🏛️")
    logger.info(f"OR window: 8:00–8:15 ET | Trade window: 8:15–9:30 ET")
    logger.info(f"Filters: gap<{OR_GAP_MAX_PTS}pts | OR {MIN_OR_SIZE_PTS}–{MAX_OR_SIZE_PTS}pts | bias-aligned | 1 trade/day")
    yield
    task.cancel()

app = FastAPI(lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"],
                   allow_methods=["*"], allow_headers=["*"])

# ── ENDPOINTS ─────────────────────────────────────────────────────────────────
@app.get("/health")
async def health():
    allowed, reason = stats.can_trade(datetime.now(EST))
    now = datetime.now(EST)
    h, m = now.hour, now.minute
    if 8*60 <= h*60+m <= 8*60+15:   phase = "BUILDING_OR"
    elif 8*60+15 < h*60+m <= 9*60+30: phase = "TRADING"
    elif h*60+m > 9*60+30:            phase = "CLOSED"
    else:                              phase = "PRE_MARKET"
    return {
        "status":   "ok",
        "phase":    phase,
        "trading":  allowed,
        "reason":   reason,
        "price_es": price_es,
        "orb":      orb.status(),
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
    await broadcast({"type": "price", "price": price, "orb": orb.status(),
                     "stats": stats.status()})
    return {"ok": True, "price": price}

@app.post("/set-prior-close")
async def set_prior_close(req: Request):
    """Set yesterday's close price for bias calculation.
    Called by TradingView Pine alert at market open."""
    body = await req.json()
    close = float(body.get("close", 0))
    if close <= 0:
        return {"ok": False}
    orb.prior_close = close
    logger.info(f"📌 Prior close set: {close:.2f}")
    return {"ok": True, "prior_close": close}

class ResultPayload(BaseModel):
    pnl:  float
    won:  bool
    note: Optional[str] = None

@app.post("/result")
async def record_result(p: ResultPayload):
    locked = stats.record(p.pnl)
    s = stats.status()
    await broadcast({"type": "result", "pnl": p.pnl, "stats": s})
    if locked:
        await send_telegram(
            f"⛔ *ORB BOT LOCKED — Drawdown Hit*\n"
            f"Total P&L: `${stats.total_pnl:.0f}` | Floor: `${stats.trailing_floor:.0f}`"
        )
    return s

@app.get("/stats")
async def get_stats():
    allowed, reason = stats.can_trade(datetime.now(EST))
    return {**stats.status(), "trading_allowed": allowed, "reason": reason,
            "orb": orb.status()}

@app.post("/reset-day")
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
            "contracts":        CONTRACTS,
            "or_build_window":  "8:00–8:15 AM ET",
            "trade_window":     "8:15–9:30 AM ET",
            "tp_multiplier":    OR_TP_MULTIPLIER,
            "gap_max_pts":      OR_GAP_MAX_PTS,
            "or_size_range":    f"{MIN_OR_SIZE_PTS}–{MAX_OR_SIZE_PTS}pts",
        }
    }))
    try:
        while True: await ws.receive_text()
    except WebSocketDisconnect:
        try: ws_clients.remove(ws)
        except: pass
