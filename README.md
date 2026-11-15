# Bybit BTC CSV converter (Rust)

This is a Rust port of `convert_to_catalog.py`. It accepts a tagged master CSV,
BTC one-minute candles, option executions, Bybit public option trades, or
timestamped option BBO quote CSVs. It validates rows and timestamps, fetches
public Bybit instrument increments (including the marked current-rule fallback
for expired options), checks fills against those increments, and writes
normalized event CSVs plus instrument metadata and a conversion summary.

## Run

```powershell
cargo run --release -- .\bybit_master_market_data.csv
```

Options:

```powershell
cargo run --release -- --catalog .\bybit_btc_options_catalog --csv-output .\bybit_normalized .\input.csv
```

Outputs are `btc_underlying_bars.csv`, `btc_option_execution_ticks.csv`,
`btc_option_quotes.csv`, `btc_option_greeks.csv` when present, plus
`instrument_specs.json` and `conversion_summary.json`. It does not create
NautilusTrader's native Parquet catalog or desktop file picker. The CSV/JSON
files are useful normalized views, but they are not the Nautilus catalog
format. Native catalog writing must use NautilusTrader's matching Rust model
and persistence crates.
