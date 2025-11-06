"""
Daily yFinance Stock Data downloader/updater.

- Reads current nasdaq.csv
- Applies filters (price, mcap, volume)
- Existing tickers in this month's data folder: incrementally append rows from the last saved date to today.
- New tickers: full window (DAYS_BACK) using resilient batches.
- Normalizes all frames to single-level OHLCV columns and repairs any bad CSVs on write.
"""

from __future__ import annotations

import csv
import logging
import random
import re
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Iterable, Iterator, List, Tuple, Optional, Dict

import pandas as pd
import yfinance as yf


# ---------- Config ----------
SRC_CSV = Path("/Users/kaite/Trading/_nasdaq_tickers/nasdaq.csv")
BASE_DIR = Path("/Users/kaite/Trading/_nasdaq_tickers")

# Filters
MIN_PRICE = 4.5
MIN_MCAP = 1e9
MIN_VOLUME = 1_000

# Full-history window for NEW tickers
DAYS_BACK = 200  # 6 months

# Batching for new tickers
DEFAULT_BATCH_SIZE = 40
DEFAULT_PAUSE_SECS = 3.0

# Rate-limit detection
RATE_LIMIT_TOKENS = ("Rate limited", "Too Many Requests", "429", "YFRateLimitError")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

def create_monthly_dir() -> Path:
    """Create .../{Mon}/data dir and return it."""
    month_str = datetime.now().strftime("%b")
    out_dir = BASE_DIR / month_str / "data"
    out_dir.mkdir(parents=True, exist_ok=True)
    return out_dir

def get_date_range(days_back: int = DAYS_BACK) -> Tuple[str, str]:
    """Return (start_date, end_date) as YYYY-MM-DD strings."""
    end_date = datetime.now().strftime("%Y-%m-%d")
    start_date = (datetime.now() - timedelta(days=days_back)).strftime("%Y-%m-%d")
    return start_date, end_date


def parse_row(row: List[str]) -> Optional[Tuple[str, float, float, int]]:
    """
    CSV layout:
      0=ticker
      2=price like '$12.34'
      5=market cap (float string)
      8=volume
    """
    try:
        ticker = row[0]
        price  = float(row[2][1:]) if row[2] else 0.0  # strip leading '$'
        m_cap  = float(row[5]) if row[5] else 0.0
        vol    = int(float(row[8])) if row[8] else 0
        return ticker, price, m_cap, vol
    except (IndexError, ValueError):
        return None


def filter_tickers(src_csv: Path) -> List[str]:
    """Return tickers passing (price, mcap, volume) filters."""
    tickers: List[str] = []
    with src_csv.open(newline="", encoding="utf-8-sig") as f:
        reader = csv.reader(f)
        next(reader, None)  # header
        for row in reader:
            parsed = parse_row(row)
            if not parsed:
                continue
            ticker, price, m_cap, vol = parsed
            if (price >= MIN_PRICE) and (m_cap >= MIN_MCAP) and (vol >= MIN_VOLUME):
                tickers.append(ticker)
    return tickers


def chunked(seq: List[str], size: int) -> Iterator[List[str]]:
    """Yield successive chunks from seq of length size."""
    for i in range(0, len(seq), size):
        yield seq[i : i + size]


_TUPLE_COL_PAT = re.compile(r"\('([^']+)',\s*'([^']+)'\)")

def collapse_tuplelike_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    If a CSV contains tuple-like column names like "('A','Open')",
    coalesce them into simple names ('Open', ...) and drop the tuple-like ones.
    """
    out = df.copy()
    tuple_cols = [c for c in out.columns if isinstance(c, str) and c.startswith("('") and "'," in c]
    if not tuple_cols:
        return out

    for tcol in tuple_cols:
        m = _TUPLE_COL_PAT.match(tcol)
        if not m:
            continue
        base = m.group(2)  # e.g. "Open"
        if base in out.columns:
            out[base] = out[base].combine_first(out[tcol])
        else:
            out[base] = out[tcol]
        out.drop(columns=[tcol], inplace=True)
    return out


def normalize_ohlcv(df: pd.DataFrame, ticker: str | None = None) -> pd.DataFrame:
    """
    Ensure a single-level, canonical OHLCV frame indexed by Date.
    If df has MultiIndex columns with a single ticker level, drop that level.
    """
    out = df.copy()

    # MultiIndex columns from yfinance
    if isinstance(out.columns, pd.MultiIndex):
        first_level = set(out.columns.get_level_values(0))
        if ticker and first_level == {ticker}:
            out = out.xs(ticker, level=0, axis=1)
        elif len(first_level) == 1:
            out = out.droplevel(0, axis=1)
        else:
            # Caller must slice per ticker before normalizing
            raise ValueError("normalize_ohlcv received multi-ticker columns; slice per ticker first")

    # Tuple-like column names from previous bad appends
    out = collapse_tuplelike_columns(out)

    # Ensure Date index
    if "Date" in out.columns:
        out = out.set_index("Date")

    out.index = pd.to_datetime(out.index, errors="coerce")
    out = out[~out.index.isna()]

    # Stable column order if present
    cols = [c for c in ["Open", "High", "Low", "Close", "Adj Close", "Volume"] if c in out.columns]
    out = out[cols] if cols else out
    return out.sort_index()


def last_saved_date(file_path: Path) -> Optional[pd.Timestamp]:
    """Return the last date in an existing ticker CSV (or None)."""
    try:
        df = pd.read_csv(file_path, index_col=0, parse_dates=True)
        if df.empty:
            return None
        df = normalize_ohlcv(df)  # normalize in case of past issues
        if df.empty:
            return None
        return df.index.max()
    except Exception:
        return None


def append_and_dedupe(file_path: Path, new_data: pd.DataFrame, *, ticker: str | None = None) -> None:
    """
    Append normalized new_data to file_path, de-duplicate by index (Date), and sort.
    """
    new_norm = normalize_ohlcv(new_data, ticker=ticker)

    if file_path.exists():
        try:
            existing = pd.read_csv(file_path, index_col=0, parse_dates=True)
        except Exception:
            existing = pd.DataFrame()
        existing = normalize_ohlcv(existing, ticker=ticker)

        combined = pd.concat([existing, new_norm])
        combined = combined[~combined.index.duplicated(keep="last")].sort_index()
        combined.to_csv(file_path)
    else:
        new_norm.to_csv(file_path)


def existing_tickers_in_dir(data_folder: Path) -> List[str]:
    """Return tickers already saved as CSV in data_folder."""
    return [p.stem for p in data_folder.glob("*.csv") if p.is_file()]


def partition(universe: List[str], data_folder: Path) -> Tuple[List[str], List[str]]:
    """
    Split eligible universe into (saved, not_saved) based on what's already in the folder.
    """
    saved = set(existing_tickers_in_dir(data_folder))
    saved_list = sorted([t for t in universe if t in saved])
    not_saved_list = sorted([t for t in universe if t not in saved])
    return saved_list, not_saved_list


# ---------- Rate-limit helpers ----------
def is_rate_limited(exc: Exception) -> bool:
    msg = str(exc)
    return any(tok in msg for tok in RATE_LIMIT_TOKENS)


def sleep_backoff(attempt: int, base: float = 2.0, cap: float = 60.0) -> None:
    """
    Exponential backoff with jitter:
    attempt 0 -> ~2s, 1 -> ~4s, 2 -> ~8s, ... (capped), plus 0–1s jitter
    """
    delay = min(cap, base * (2 ** attempt)) + random.random()
    logging.warning(f"Backing off for {delay:.1f}s…")
    time.sleep(delay)
# ---------------------------------------


# ---------- yFinance download ----------
def batch_download_resilient(batch: List[str], start_date: str, end_date: str,
                             max_retries: int = 5, min_batch: int = 8) -> Dict[str, pd.DataFrame]:
    """
    Try to download a batch of tickers.
    - On rate limit, exponential backoff and split the batch.
    - Falls back to single-ticker retries for stubborn chunks.
    - Returns a dict {ticker: df} for the tickers that succeeded.
    """
    results: Dict[str, pd.DataFrame] = {}

    def _download_chunk(tickers: List[str], attempt: int = 0):
        nonlocal results
        if not tickers:
            return

        try:
            df = yf.download(
                tickers=tickers,
                start=start_date,
                end=end_date,
                auto_adjust=True,
                group_by="ticker",
                threads=True,
                progress=False,
            )

            # Multi-ticker result (normal path)
            if isinstance(df.columns, pd.MultiIndex):
                present = set(df.columns.get_level_values(0))
                for tkr in tickers:
                    if tkr in present:
                        sub = df[tkr].dropna(how="all")
                        if not sub.empty:
                            results[tkr] = sub
                        else:
                            logging.info(f"empty data for {tkr} (skip)")
                    else:
                        logging.info(f"no data for {tkr} (skip)")
                return

            # Single-ticker fallback (can happen if len(tickers)==1)
            if len(tickers) == 1:
                sub = df.dropna(how="all")
                if not sub.empty:
                    results[tickers[0]] = sub
                else:
                    logging.info(f"empty data for {tickers[0]} (skip)")
                return

            # Unexpected shape: try splitting
            raise RuntimeError("Unexpected dataframe shape from yfinance.")

        except Exception as e:
            if is_rate_limited(e):
                if attempt >= max_retries and len(tickers) <= min_batch:
                    logging.error(f"Rate-limited after {attempt} attempts for a small chunk; "
                                  f"falling back to single-ticker downloads ({len(tickers)}).")
                    # last resort: one-by-one with backoff per symbol
                    for t in tickers:
                        for a in range(max_retries + 1):
                            try:
                                df1 = yf.download(
                                    tickers=t,
                                    start=start_date,
                                    end=end_date,
                                    auto_adjust=True,
                                    group_by="ticker",
                                    threads=False,
                                    progress=False,
                                )
                                sub = df1.dropna(how="all")
                                if not sub.empty:
                                    results[t] = sub
                                break
                            except Exception as e1:
                                if is_rate_limited(e1):
                                    sleep_backoff(a)
                                    continue
                                logging.exception(f"{t}: hard failure: {e1}")
                                break
                    return
                # backoff and either retry or split
                sleep_backoff(attempt)
                if len(tickers) > min_batch:
                    mid = len(tickers) // 2
                    _download_chunk(tickers[:mid], attempt + 1)
                    _download_chunk(tickers[mid:], attempt + 1)
                else:
                    _download_chunk(tickers, attempt + 1)
            else:
                logging.exception(f"Non-rate-limit error for chunk ({len(tickers)}): {e}")
                # Best effort: split if big, else skip
                if len(tickers) > 1:
                    mid = len(tickers) // 2
                    _download_chunk(tickers[:mid], attempt)
                    _download_chunk(tickers[mid:], attempt)

    _download_chunk(batch, attempt=0)
    return results


# ---------- Save Helpers ----------
def save_multiticker_results(results: Dict[str, pd.DataFrame], out_dir: Path) -> None:
    """
    Save each ticker dataframe (already sliced per ticker).
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    for tkr, sub in results.items():
        out_path = out_dir / f"{tkr}.csv"
        append_and_dedupe(out_path, sub, ticker=tkr)
        print(f"  → saved {tkr}.csv")


def save_single_ticker_frame(ticker: str, df: pd.DataFrame, out_dir: Path) -> None:
    """Save/append one ticker's DataFrame."""
    if df.empty:
        print(f"  ! no new rows for {ticker}")
        return
    out_path = out_dir / f"{ticker}.csv"
    append_and_dedupe(out_path, df, ticker=ticker)
    print(f"  → updated {ticker}.csv")
# --------------------------------------------


# ---------- Entry Points ----------
def download_new_tickers(not_saved: List[str], out_dir: Path, batch_size: int, pause_secs: float) -> None:
    """Full download for NEW tickers in resilient batches."""
    if not not_saved:
        return

    start_date, end_date = get_date_range(DAYS_BACK)

    for batch_idx, batch in enumerate(chunked(not_saved, batch_size), start=1):
        logging.info(f"[NEW] Batch {batch_idx}: {len(batch)} tickers")
        results = batch_download_resilient(batch, start_date, end_date)
        save_multiticker_results(results, out_dir)
        time.sleep(pause_secs)  # polite pause between top-level batches


def single_ticker_download(ticker: str, start_date: str, end_date: str) -> pd.DataFrame:
    """Per-ticker download (used for UPDATES)."""
    return yf.download(
        tickers=ticker,
        start=start_date,
        end=end_date,
        auto_adjust=True,
        group_by="ticker",
        threads=False,
        progress=False,
    )


def update_existing_tickers(saved: List[str], out_dir: Path) -> None:
    """
    Incrementally update each saved ticker from last_saved_date + 1d to today.
    """
    end_date = datetime.now().strftime("%Y-%m-%d")
    for tkr in saved:
        fp = out_dir / f"{tkr}.csv"
        last_date = last_saved_date(fp)
        if last_date is None:
            # No readable history; fall back to full window
            start_date, _ = get_date_range(DAYS_BACK)
        else:
            start_date = (last_date + pd.Timedelta(days=1)).strftime("%Y-%m-%d")

        if start_date > end_date:
            print(f"  = {tkr} already up-to-date")
            continue

        # retry a few times if rate-limited on a single ticker
        for attempt in range(6):
            try:
                df = single_ticker_download(tkr, start_date, end_date)
                save_single_ticker_frame(tkr, df, out_dir)
                break
            except Exception as e:
                if is_rate_limited(e):
                    sleep_backoff(attempt)
                    continue
                logging.exception(f"{tkr}: non-rate-limit error: {e}")
                break


def run_daily(batch_size: int = DEFAULT_BATCH_SIZE, pause_secs: float = DEFAULT_PAUSE_SECS) -> None:
    """
    Daily:
      1 Read current nasdaq.csv
      2 Filter eligible tickers
      3 Split into 'saved' vs 'not_saved' based on the data folder
      4 Update saved tickers incrementally
      5 Resilient batch-download new tickers for the full window
    """
    out_dir = create_monthly_dir()
    universe = filter_tickers(SRC_CSV)
    if not universe:
        print("No tickers passed the filter criteria.")
        return

    saved, not_saved = partition(universe, out_dir)

    print(f"{len(universe)} eligible tickers.")
    print(f" - {len(saved)} already present -> incremental update")
    print(f" - {len(not_saved)} new -> full download ({DAYS_BACK} days)")

    if saved:
        print("\nUpdating existing tickers…")
        update_existing_tickers(saved, out_dir)

    if not_saved:
        print("\nDownloading NEW tickers…")
        download_new_tickers(not_saved, out_dir, batch_size, pause_secs)

    print("\nDone.")
# -----------------------------------------------


# ---------- One-time Repair ----------
def repair_folder(folder: Path) -> None:
    """
    Clean existing CSVs in a folder:
    - collapse tuple-like columns
    - normalize to single-level OHLCV
    - sort by date and write back
    """
    for fp in folder.glob("*.csv"):
        try:
            df = pd.read_csv(fp, index_col=0, parse_dates=True)
            fixed = normalize_ohlcv(df)
            fixed.to_csv(fp)
            print(f"Fixed {fp.name}")
        except Exception as e:
            print(f"Skip {fp.name}: {e}")
# -----------------------------------------------


if __name__ == "__main__":
    run_daily(batch_size=DEFAULT_BATCH_SIZE, pause_secs=DEFAULT_PAUSE_SECS)

    # for repairing folder once
    # repair_folder(create_monthly_dir())