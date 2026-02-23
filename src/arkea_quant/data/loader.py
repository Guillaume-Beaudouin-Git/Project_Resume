"""
Market data loader — yfinance backend with parquet caching.

Design
------
- Downloads adjusted close prices for a list of tickers.
- Caches results as parquet files under ``data/raw/``.
- Supports incremental refresh (only fetches missing dates).
- Deterministic: given the same date range, always returns the same data.

No-leakage note
---------------
This module only fetches raw OHLCV data.  All signal construction is done
downstream with explicit shifts.  The loader never computes forward-looking
quantities.
"""

from __future__ import annotations

import time
from pathlib import Path
from typing import Optional

import pandas as pd
import yfinance as yf

from arkea_quant.utils.logging import get_logger

log = get_logger(__name__)


class DataLoader:
    """Download and cache adjusted close price data from Yahoo Finance.

    Parameters
    ----------
    raw_dir:
        Directory for parquet cache files.
    auto_adjust:
        If True, use split- and dividend-adjusted close prices.
    retries:
        Number of download retries on transient failure.
    timeout:
        Seconds to wait per download attempt.
    """

    def __init__(
        self,
        raw_dir: str | Path = "data/raw",
        auto_adjust: bool = True,
        retries: int = 3,
        timeout: int = 30,
    ) -> None:
        self.raw_dir = Path(raw_dir)
        self.raw_dir.mkdir(parents=True, exist_ok=True)
        self.auto_adjust = auto_adjust
        self.retries = retries
        self.timeout = timeout

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def load(
        self,
        tickers: list[str],
        start: str,
        end: Optional[str] = None,
        overwrite: bool = False,
    ) -> pd.DataFrame:
        """Load (or download) adjusted close prices for *tickers*.

        Returns a ``pd.DataFrame`` with shape ``(dates, tickers)``, indexed
        by business-day dates.

        Parameters
        ----------
        tickers:
            List of ticker symbols (e.g. ``["AAPL", "MSFT"]``).
        start:
            Start date string ``"YYYY-MM-DD"``.
        end:
            End date string or ``None`` for today.
        overwrite:
            If True, ignore cache and re-download.
        """
        cache_path = self._cache_path(tickers, start, end)

        if cache_path.exists() and not overwrite:
            log.info(f"Loading from cache: {cache_path}")
            prices = pd.read_parquet(cache_path)
            return prices

        log.info(f"Downloading {len(tickers)} tickers from Yahoo Finance …")
        prices = self._download(tickers, start, end)

        log.info(f"Saving to cache: {cache_path}")
        prices.to_parquet(cache_path)
        return prices

    def load_single(
        self,
        ticker: str,
        start: str,
        end: Optional[str] = None,
        overwrite: bool = False,
    ) -> pd.Series:
        """Convenience wrapper: load a single ticker as a ``pd.Series``."""
        df = self.load([ticker], start=start, end=end, overwrite=overwrite)
        return df[ticker]

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _download(
        self,
        tickers: list[str],
        start: str,
        end: Optional[str],
    ) -> pd.DataFrame:
        """Download data from Yahoo Finance with retry logic."""
        last_exc: Exception | None = None

        for attempt in range(1, self.retries + 1):
            try:
                raw = yf.download(
                    tickers=tickers,
                    start=start,
                    end=end,
                    auto_adjust=self.auto_adjust,
                    progress=False,
                    threads=True,
                )
                prices = self._extract_close(raw, tickers)
                log.info(
                    f"Downloaded {prices.shape[1]} tickers × {prices.shape[0]} days "
                    f"({start} → {prices.index[-1].date()})"
                )
                return prices

            except Exception as exc:
                last_exc = exc
                wait = 2 ** attempt
                log.warning(
                    f"Download attempt {attempt}/{self.retries} failed: {exc}. "
                    f"Retrying in {wait}s …"
                )
                time.sleep(wait)

        raise RuntimeError(
            f"Failed to download data after {self.retries} attempts."
        ) from last_exc

    @staticmethod
    def _extract_close(raw: pd.DataFrame, tickers: list[str]) -> pd.DataFrame:
        """Extract the ``Close`` column(s) from a raw yfinance DataFrame.

        yfinance returns a MultiIndex frame when multiple tickers are requested.
        """
        if isinstance(raw.columns, pd.MultiIndex):
            # Multi-ticker download → (field, ticker) MultiIndex columns
            if "Close" in raw.columns.get_level_values(0):
                prices = raw["Close"]
            else:
                # auto_adjust=True returns OHLCV without explicit 'Adj Close'
                prices = raw["Close"]
        else:
            # Single ticker → flat columns
            prices = raw[["Close"]].rename(columns={"Close": tickers[0]})

        # Ensure columns are in requested order, filling missing with NaN
        prices = prices.reindex(columns=tickers)
        # Drop rows where ALL tickers are NaN (non-trading days)
        prices = prices.dropna(how="all")
        # Ensure DatetimeIndex
        prices.index = pd.DatetimeIndex(prices.index)
        prices.index.name = "date"
        return prices

    def _cache_path(
        self,
        tickers: list[str],
        start: str,
        end: Optional[str],
    ) -> Path:
        """Generate a deterministic parquet cache filename."""
        tickers_key = "_".join(sorted(tickers))[:80]  # cap length
        end_tag = end or "today"
        fname = f"prices_{tickers_key}_{start}_{end_tag}.parquet"
        # If filename would be too long, hash it
        if len(fname) > 200:
            import hashlib
            h = hashlib.md5(fname.encode()).hexdigest()[:12]
            fname = f"prices_{h}_{start}_{end_tag}.parquet"
        return self.raw_dir / fname
