# yf-gainers-scanner

A small side project I built to practice working with third-party APIs, and basic data wrangling.  
It downloads historical OHLCV data with [yfinance](https://github.com/ranaroussi/yfinance), keeps CSVs up to date, and applies a few simple filters so I can filter out recent Stock gainers.
---

## Why I built this
- I wanted to make a small project that could scan the stock market for top stock gainers 
- and there really aren't many free stock market scanners with the features that I would like to have

---

## What it does 
1. Reads a CSV of tickers (I used a NASDAQ ticker list from [datahub](https://datahub.io/core/nasdaq-listings)).
2. For each ticker:
   - If I don’t already have data for it: download `--days-back` worth of data and save it to a CSV file.
   - If I already have a CSV: only append the new days to the file.
3. Save everything into `data/<Mon>/data/<TICKER>.csv` each month.
4. Optionally filter by price, market cap, and volume to focus. 
5. Try to not get rate limited by the API by adding small pauses and batch sizes (only partially works haven't really found a good solution to this)
