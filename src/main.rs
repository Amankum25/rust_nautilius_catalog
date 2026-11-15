//! Rust port of `convert_to_catalog.py`.
//!
//! The converter validates Bybit BTC candles/executions, resolves public
//! instrument metadata, and writes normalized CSV views plus metadata JSON.
//! Native NautilusTrader Parquet catalog serialization is left to the
//! consuming project's matching NautilusTrader Rust crate versions. This
//! binary writes normalized event CSVs and public instrument metadata.

use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, NaiveDateTime, TimeZone, Utc};
use clap::Parser;
use csv::{ReaderBuilder, StringRecord, Writer};
use regex::Regex;
use reqwest::blocking::Client;
use rust_decimal::Decimal;
use serde::Serialize;
use serde_json::{json, Value};
use std::{
    collections::{BTreeMap, BTreeSet, HashSet},
    fs,
    path::{Path, PathBuf},
    str::FromStr,
    thread,
    time::Duration,
};

const API: &str = "https://api.bybit.com";
const UNDERLYING_ID: &str = "BTCUSDT-LINEAR.BYBIT";

#[derive(Parser)]
#[command(about = "Convert Bybit BTC market CSV data to normalized Rust outputs")]
struct Args {
    /// CSV paths. Select the tagged master CSV alone or individual market CSVs.
    files: Vec<PathBuf>,
    #[arg(long, default_value = "bybit_btc_options_catalog")]
    catalog: PathBuf,
    #[arg(long = "csv-output", default_value = "bybit_normalized")]
    csv_output: PathBuf,
}

#[derive(Clone)]
struct CsvData {
    path: PathBuf,
    headers: StringRecord,
    rows: Vec<StringRecord>,
}

#[derive(Clone, Serialize)]
struct BarRow {
    instrument_id: String,
    ts_event_ns: i64,
    ts_init_ns: i64,
    open: String,
    high: String,
    low: String,
    close: String,
    volume_btc: String,
}

#[derive(Clone, Serialize)]
struct TickRow {
    instrument_id: String,
    ts_event_ns: i64,
    exec_id: String,
    order_id: String,
    side: String,
    is_maker: String,
    aggressor_side: String,
    price_usdt: String,
    size_btc: String,
    fee_usdt: String,
    mark_price_usdt: String,
    mark_iv: String,
}

#[derive(Clone, Serialize)]
struct QuoteRow {
    instrument_id: String,
    symbol: String,
    ts_event_ns: i64,
    bid_price: String,
    bid_size: String,
    ask_price: String,
    ask_size: String,
}

#[derive(Clone, Serialize)]
struct GreeksRow {
    instrument_id: String,
    symbol: String,
    ts_event_ns: i64,
    delta: String,
    gamma: String,
    vega: String,
    theta: String,
    rho: String,
    mark_iv: String,
    bid_iv: String,
    ask_iv: String,
    underlying_price: String,
}

fn cell<'a>(row: &'a StringRecord, headers: &StringRecord, key: &str) -> Result<&'a str> {
    let i = headers
        .iter()
        .position(|h| h == key)
        .ok_or_else(|| anyhow!("missing column {key}"))?;
    row.get(i).ok_or_else(|| anyhow!("missing {key} value"))
}

fn read_csv(path: &Path) -> Result<CsvData> {
    let mut reader = ReaderBuilder::new()
        .flexible(true)
        .from_path(path)
        .with_context(|| format!("cannot read {}", path.display()))?;
    let headers = reader.headers()?.clone();
    let rows: Vec<_> = reader.records().collect::<std::result::Result<_, _>>()?;
    if rows.is_empty() {
        bail!("{} has no rows", path.display());
    }
    Ok(CsvData {
        path: path.to_owned(),
        headers,
        rows,
    })
}

fn has(headers: &StringRecord, cols: &[&str]) -> bool {
    cols.iter().all(|c| headers.iter().any(|h| h == *c))
}

fn decimal(text: &str, field: &str) -> Result<Decimal> {
    let v = text.trim();
    if v.is_empty() {
        bail!("{field} is empty");
    }
    Decimal::from_str(v).with_context(|| format!("{field} is not numeric: {v:?}"))
}

fn decimal_text(text: &str, field: &str) -> Result<String> {
    Ok(decimal(text, field)?.to_string())
}

fn timestamp_ns(text: &str, field: &str) -> Result<i64> {
    let raw = text.trim();
    if let Ok(value) = Decimal::from_str(raw) {
        if value.fract() != Decimal::ZERO || value <= Decimal::ZERO {
            bail!("{field} must be a positive integer timestamp");
        }
        let n: i64 = value.try_into().context("timestamp out of range")?;
        return if n < 100_000_000_000 {
            n.checked_mul(1_000_000_000)
        } else if n < 100_000_000_000_000 {
            n.checked_mul(1_000_000)
        } else if n < 100_000_000_000_000_000 {
            n.checked_mul(1_000)
        } else {
            Some(n)
        }
        .ok_or_else(|| anyhow!("timestamp out of range"));
    }
    let parsed = DateTime::parse_from_rfc3339(raw)
        .map(|dt| dt.with_timezone(&Utc))
        .or_else(|_| {
            NaiveDateTime::parse_from_str(raw, "%Y-%m-%d %H:%M:%S%.f")
                .map(|dt| Utc.from_utc_datetime(&dt))
        })
        .or_else(|_| {
            NaiveDateTime::parse_from_str(raw, "%Y-%m-%dT%H:%M:%S%.f")
                .map(|dt| Utc.from_utc_datetime(&dt))
        })
        .map_err(|_| anyhow!("{field} is not a supported timestamp: {raw:?}"))?;
    Ok(parsed
        .timestamp_nanos_opt()
        .ok_or_else(|| anyhow!("timestamp out of range"))?)
}

fn bool_value(text: &str) -> Result<bool> {
    match text.trim().to_ascii_lowercase().as_str() {
        "true" | "1" | "yes" => Ok(true),
        "false" | "0" | "no" => Ok(false),
        _ => bail!("is_maker is not a boolean: {text:?}"),
    }
}

fn option_id(symbol: &str) -> Result<String> {
    let s = symbol.trim().to_ascii_uppercase();
    let re = Regex::new(r"^BTC-\d{1,2}[A-Z]{3}\d{2}-\d+(?:\.\d+)?-[CP]-USDT$").unwrap();
    if !re.is_match(&s) {
        bail!("not a supported BTC USDT option symbol: {s:?}");
    }
    Ok(format!("{s}-OPTION.BYBIT"))
}

fn normalize_bars(data: &CsvData) -> Result<Vec<BarRow>> {
    let mut out = Vec::new();
    let mut seen = HashSet::new();
    for (idx, row) in data.rows.iter().enumerate() {
        if data.headers.iter().any(|h| h == "record_type")
            && cell(row, &data.headers, "record_type")? != "underlying_candle"
        {
            continue;
        }
        let ts = timestamp_ns(
            cell(row, &data.headers, "timestamp_ms")?,
            &format!("underlying row {idx} timestamp_ms"),
        )?;
        if !seen.insert(ts) {
            bail!("duplicate BTC underlying candle timestamp at row {idx}");
        }
        let open = decimal_text(cell(row, &data.headers, "open")?, "open")?;
        let high = decimal_text(cell(row, &data.headers, "high")?, "high")?;
        let low = decimal_text(cell(row, &data.headers, "low")?, "low")?;
        let close = decimal_text(cell(row, &data.headers, "close")?, "close")?;
        let volume = decimal_text(cell(row, &data.headers, "volume_btc")?, "volume_btc")?;
        let (o, h, l, c, v) = (
            decimal(&open, "open")?,
            decimal(&high, "high")?,
            decimal(&low, "low")?,
            decimal(&close, "close")?,
            decimal(&volume, "volume")?,
        );
        if o <= Decimal::ZERO
            || h <= Decimal::ZERO
            || l <= Decimal::ZERO
            || c <= Decimal::ZERO
            || v < Decimal::ZERO
            || h < o.max(l).max(c)
            || l > o.min(h).min(c)
        {
            bail!("invalid OHLCV values at underlying row {idx}");
        }
        out.push(BarRow {
            instrument_id: UNDERLYING_ID.into(),
            ts_event_ns: ts,
            ts_init_ns: ts + 60_000_000_000,
            open,
            high,
            low,
            close,
            volume_btc: volume,
        });
    }
    if out.is_empty() {
        bail!("master CSV contains no underlying_candle rows");
    }
    out.sort_by_key(|x| x.ts_event_ns);
    Ok(out)
}

fn normalize_ticks(data: &CsvData, public_trades: bool) -> Result<Vec<TickRow>> {
    let mut out = Vec::new();
    let mut ids = HashSet::new();
    for (idx, row) in data.rows.iter().enumerate() {
        if data.headers.iter().any(|h| h == "record_type")
            && cell(row, &data.headers, "record_type")? != "option_execution"
        {
            continue;
        }
        let symbol = cell(row, &data.headers, "symbol")?
            .trim()
            .to_ascii_uppercase();
        let iid = option_id(&symbol)?;
        let exec = if public_trades {
            data.headers
                .iter()
                .position(|h| h == "execId")
                .or_else(|| data.headers.iter().position(|h| h == "exec_id"))
                .and_then(|i| row.get(i))
                .filter(|v| !v.trim().is_empty())
                .map(str::to_owned)
                .unwrap_or_else(|| {
                    format!("{}-{idx}", cell(row, &data.headers, "time").unwrap_or(""))
                })
        } else {
            cell(row, &data.headers, "exec_id")?.trim().to_string()
        };
        if exec.is_empty() || !ids.insert(exec.clone()) {
            bail!("missing or duplicate execution ID at row {idx}");
        }
        let ts = timestamp_ns(
            cell(
                row,
                &data.headers,
                if public_trades {
                    "time"
                } else {
                    "timestamp_ms"
                },
            )?,
            &format!("execution row {idx} timestamp_ms"),
        )?;
        let price = decimal_text(
            cell(
                row,
                &data.headers,
                if public_trades {
                    "price"
                } else {
                    "exec_price_usdt"
                },
            )?,
            "execution price",
        )?;
        let size = decimal_text(
            cell(
                row,
                &data.headers,
                if public_trades {
                    "size"
                } else {
                    "exec_qty_btc"
                },
            )?,
            "execution quantity",
        )?;
        if decimal(&price, "price")? <= Decimal::ZERO || decimal(&size, "size")? <= Decimal::ZERO {
            bail!("execution row {idx} price and quantity must be positive");
        }
        let side = cell(row, &data.headers, "side")?
            .trim()
            .to_ascii_uppercase();
        if side != "BUY" && side != "SELL" {
            bail!("unsupported order side at execution row {idx}: {side:?}");
        }
        let maker = if public_trades {
            false
        } else {
            bool_value(cell(row, &data.headers, "is_maker")?)?
        };
        let aggressor = if maker {
            if side == "BUY" {
                "2"
            } else {
                "1"
            }
        } else if side == "BUY" {
            "1"
        } else {
            "2"
        };
        let fee = if public_trades {
            "0".to_string()
        } else {
            decimal_text(cell(row, &data.headers, "exec_fee_usdt")?, "execution fee")?
        };
        let opt = |keys: &[&str]| -> String {
            keys.iter()
                .find_map(|k| {
                    data.headers
                        .iter()
                        .position(|h| h == *k)
                        .and_then(|i| row.get(i))
                })
                .unwrap_or("")
                .to_string()
        };
        let order = if public_trades {
            String::new()
        } else {
            cell(row, &data.headers, "order_id")?.to_owned()
        };
        out.push(TickRow {
            instrument_id: iid,
            ts_event_ns: ts,
            exec_id: exec,
            order_id: order,
            side,
            is_maker: if maker { "True" } else { "False" }.to_owned(),
            aggressor_side: aggressor.into(),
            price_usdt: price,
            size_btc: size,
            fee_usdt: fee,
            mark_price_usdt: opt(if public_trades {
                &["mP"]
            } else {
                &["mark_price"]
            }),
            mark_iv: opt(if public_trades {
                &["mIv", "mark_iv"]
            } else {
                &["mark_iv"]
            }),
        });
    }
    if out.is_empty() {
        bail!("master CSV contains no option_execution rows");
    }
    out.sort_by(|a, b| {
        a.ts_event_ns
            .cmp(&b.ts_event_ns)
            .then(a.exec_id.cmp(&b.exec_id))
    });
    Ok(out)
}

fn optional_cell<'a>(
    row: &'a StringRecord,
    headers: &StringRecord,
    names: &[&str],
) -> Option<&'a str> {
    names.iter().find_map(|name| {
        headers
            .iter()
            .position(|h| h.eq_ignore_ascii_case(name))
            .and_then(|i| row.get(i))
    })
}

fn quote_decimal(text: &str, field: &str) -> Result<String> {
    decimal_text(text, field)
}

fn pair_precision(left: &str, right: &str) -> Result<(String, String)> {
    let l = Decimal::from_str(left)?;
    let r = Decimal::from_str(right)?;
    let scale = l.scale().max(r.scale());
    Ok((
        format!("{:.*}", scale as usize, l),
        format!("{:.*}", scale as usize, r),
    ))
}

fn normalize_quotes(data: &CsvData) -> Result<(Vec<QuoteRow>, Vec<GreeksRow>)> {
    let timestamp_names = [
        "timestamp_ms",
        "timestamp",
        "ts",
        "time",
        "datetime",
        "timestamp_utc",
        "exchange_timestamp",
        "ts_event",
        "timestamp_ns",
        "timestamp_us",
        "ts_ms",
        "ts_ns",
    ];
    let mut quotes = Vec::new();
    let mut greeks = Vec::new();
    for (idx, row) in data.rows.iter().enumerate() {
        if optional_cell(row, &data.headers, &["record_type"])
            .is_some_and(|v| v != "option_quote" && v != "option_ticker")
        {
            continue;
        }
        let symbol = match optional_cell(row, &data.headers, &["symbol"]) {
            Some(v) if !v.trim().is_empty() => v.trim().to_ascii_uppercase(),
            _ => continue,
        };
        let bid_price = optional_cell(
            row,
            &data.headers,
            &["bid_price", "bidPrice", "bid1Price", "bid"],
        );
        let bid_size = optional_cell(row, &data.headers, &["bid_size", "bidSize", "bid1Size"]);
        let ask_price = optional_cell(
            row,
            &data.headers,
            &["ask_price", "askPrice", "ask1Price", "ask"],
        );
        let ask_size = optional_cell(row, &data.headers, &["ask_size", "askSize", "ask1Size"]);
        let (Some(bp), Some(bs), Some(ap), Some(asz)) = (bid_price, bid_size, ask_price, ask_size)
        else {
            continue;
        };
        let iid = option_id(&symbol)?;
        let stamp = optional_cell(row, &data.headers, &timestamp_names)
            .ok_or_else(|| anyhow!("option quote data is missing a timestamp column"))?;
        let ts = timestamp_ns(stamp, &format!("option quote row {idx} timestamp"))?;
        let bp = quote_decimal(bp, "bid_price")?;
        let ap = quote_decimal(ap, "ask_price")?;
        let bs = quote_decimal(bs, "bid_size")?;
        let asz = quote_decimal(asz, "ask_size")?;
        let (bpv, apv, bsv, asv) = (
            decimal(&bp, "bid_price")?,
            decimal(&ap, "ask_price")?,
            decimal(&bs, "bid_size")?,
            decimal(&asz, "ask_size")?,
        );
        if bpv <= Decimal::ZERO
            || apv <= Decimal::ZERO
            || bsv <= Decimal::ZERO
            || asv <= Decimal::ZERO
            || bpv > apv
        {
            continue;
        }
        let (bp, ap) = pair_precision(&bp, &ap)?;
        let (bs, asz) = pair_precision(&bs, &asz)?;
        quotes.push(QuoteRow {
            instrument_id: iid.clone(),
            symbol: symbol.clone(),
            ts_event_ns: ts,
            bid_price: bp,
            bid_size: bs,
            ask_price: ap,
            ask_size: asz,
        });

        let greek_names = ["delta", "gamma", "vega", "theta"];
        let values: Option<Vec<String>> = greek_names
            .iter()
            .map(|name| {
                optional_cell(row, &data.headers, &[*name])
                    .filter(|v| !v.trim().is_empty())
                    .map(|v| decimal_text(v, name))
                    .transpose()
            })
            .collect::<Result<Option<Vec<_>>>>()?;
        if let Some(v) = values {
            let optional = |names: &[&str]| -> Result<String> {
                optional_cell(row, &data.headers, names)
                    .filter(|x| !x.trim().is_empty())
                    .map(|x| decimal_text(x, names[0]))
                    .transpose()
                    .map(|x| x.unwrap_or_default())
            };
            greeks.push(GreeksRow {
                instrument_id: iid,
                symbol: symbol.clone(),
                ts_event_ns: ts,
                delta: v[0].clone(),
                gamma: v[1].clone(),
                vega: v[2].clone(),
                theta: v[3].clone(),
                rho: "0".into(),
                mark_iv: optional(&["mark_iv", "markPriceIv", "markIv"])?,
                bid_iv: optional(&["bid_iv", "bidIv"])?,
                ask_iv: optional(&["ask_iv", "askIv"])?,
                underlying_price: optional(&["underlying_price", "underlyingPrice"])?,
            });
        }
    }
    if quotes.is_empty() {
        bail!("no valid two-sided option BBO rows remain after quote validation");
    }
    quotes.sort_by(|a, b| {
        a.ts_event_ns
            .cmp(&b.ts_event_ns)
            .then(a.symbol.cmp(&b.symbol))
    });
    greeks.sort_by(|a, b| {
        a.ts_event_ns
            .cmp(&b.ts_event_ns)
            .then(a.symbol.cmp(&b.symbol))
    });
    Ok((quotes, greeks))
}

fn write_csv<T: Serialize>(path: &Path, rows: &[T]) -> Result<()> {
    let mut w = Writer::from_path(path)?;
    for row in rows {
        w.serialize(row)?;
    }
    w.flush()?;
    Ok(())
}

fn exact_spec(client: &Client, category: &str, symbol: &str) -> Option<Value> {
    for endpoint in ["instruments-info", "instrument-summary"] {
        let response = client
            .get(format!("{API}/v5/market/{endpoint}"))
            .query(&[("category", category), ("symbol", symbol)])
            .send();
        let Ok(response) = response else { continue };
        let Ok(response) = response.error_for_status() else {
            continue;
        };
        let Ok(body) = response.json::<Value>() else {
            continue;
        };
        if body["retCode"] == 0 {
            if let Some(list) = body["result"]["list"].as_array() {
                if let Some(row) = list.iter().find(|r| {
                    r["symbol"]
                        .as_str()
                        .unwrap_or("")
                        .eq_ignore_ascii_case(symbol)
                }) {
                    return Some(row.clone());
                }
            }
        }
    }
    None
}

fn uniform_option_rules(client: &Client) -> Result<(String, String, i64, usize)> {
    let mut cursor = String::new();
    let mut rows = Vec::new();
    let mut as_of_ms = None;
    loop {
        let mut params = vec![
            ("category", "option"),
            ("baseCoin", "BTC"),
            ("limit", "1000"),
        ];
        if !cursor.is_empty() {
            params.push(("cursor", cursor.as_str()));
        }
        let body: Value = client
            .get(format!("{API}/v5/market/instrument-summary"))
            .query(&params)
            .send()?
            .error_for_status()?
            .json()?;
        if body["retCode"] != 0 {
            bail!("Bybit BTC option summary failed: {}", body["retMsg"]);
        }
        if let Some(list) = body["result"]["list"].as_array() {
            rows.extend(list.iter().cloned());
        }
        as_of_ms = body["time"].as_i64();
        cursor = body["result"]["nextPageCursor"]
            .as_str()
            .unwrap_or("")
            .to_owned();
        if cursor.is_empty() {
            break;
        }
    }
    let ticks: BTreeSet<_> = rows.iter().filter_map(|r| r["tickSize"].as_str()).collect();
    let steps: BTreeSet<_> = rows.iter().filter_map(|r| r["qtyStep"].as_str()).collect();
    if rows.is_empty() || ticks.len() != 1 || steps.len() != 1 {
        bail!("active BTC options do not share one tickSize/qtyStep; archived option specs are required");
    }
    Ok((
        (*ticks.iter().next().unwrap()).to_owned(),
        (*steps.iter().next().unwrap()).to_owned(),
        as_of_ms.unwrap_or_default(),
        rows.len(),
    ))
}

fn expired_option_spec(
    symbol: &str,
    tick: &str,
    step: &str,
    as_of_ms: i64,
    count: usize,
) -> Result<Value> {
    let re = Regex::new(r"^BTC-(\d{1,2}[A-Z]{3}\d{2})-(\d+(?:\.\d+)?)-([CP])-USDT$").unwrap();
    let cap = re
        .captures(symbol)
        .ok_or_else(|| anyhow!("unsupported BTC option symbol {symbol}"))?;
    let date = chrono::NaiveDate::parse_from_str(&cap[1], "%d%b%y")?;
    let delivery_ms = date
        .and_hms_opt(8, 0, 0)
        .ok_or_else(|| anyhow!("invalid option expiry"))?
        .and_utc()
        .timestamp_millis();
    Ok(json!({
        "symbol":symbol,"category":"option","baseCoin":"BTC","quoteCoin":"USDT","settleCoin":"USDT",
        "launchTime":"0","deliveryTime":delivery_ms.to_string(),"priceFilter":{"tickSize":tick},
        "lotSizeFilter":{"qtyStep":step},"metadataSource":"Bybit current uniform BTC option instrument-summary rule",
        "currentRulesAsOfMs":as_of_ms,"currentRuleSymbolsChecked":count,
        "historicalLaunchTimeUnavailable":true,"historicalSpecFallback":true
    }))
}

fn load_archived_specs(files: &[CsvData]) -> BTreeMap<String, Value> {
    let tick_names = ["tickSize", "tick_size", "ticksize", "price_tick_size"];
    let step_names = [
        "qtyStep",
        "qty_step",
        "qtystep",
        "quantity_step",
        "size_increment",
    ];
    let mut specs = BTreeMap::new();
    let option_re = Regex::new(r"^BTC-(\d{1,2}[A-Z]{3}\d{2})-\d+(?:\.\d+)?-[CP]-USDT$").unwrap();
    for file in files {
        let (Some(tick_col), Some(step_col)) = (
            tick_names
                .iter()
                .find(|n| file.headers.iter().any(|h| h == **n)),
            step_names
                .iter()
                .find(|n| file.headers.iter().any(|h| h == **n)),
        ) else {
            continue;
        };
        for row in &file.rows {
            let Some(symbol) = optional_cell(row, &file.headers, &["symbol"])
                .map(|s| s.trim().to_ascii_uppercase())
            else {
                continue;
            };
            let Some(cap) = option_re.captures(&symbol) else {
                continue;
            };
            let (Some(tick), Some(step)) = (
                optional_cell(row, &file.headers, &[*tick_col]),
                optional_cell(row, &file.headers, &[*step_col]),
            ) else {
                continue;
            };
            if tick.trim().is_empty() || step.trim().is_empty() {
                continue;
            }
            let Ok(date) = chrono::NaiveDate::parse_from_str(&cap[1], "%d%b%y") else {
                continue;
            };
            let delivery_ms = date
                .and_hms_opt(8, 0, 0)
                .unwrap()
                .and_utc()
                .timestamp_millis();
            let launch = optional_cell(row, &file.headers, &["launchTime", "launch_time_ms"])
                .filter(|x| !x.trim().is_empty())
                .unwrap_or("0");
            let delivery = optional_cell(row, &file.headers, &["deliveryTime", "delivery_time_ms"])
                .filter(|x| !x.trim().is_empty())
                .unwrap_or("");
            specs.insert(symbol.clone(),json!({
                "symbol":symbol,"category":"option","baseCoin":"BTC","quoteCoin":"USDT","settleCoin":"USDT",
                "launchTime":launch,"deliveryTime":if delivery.is_empty(){delivery_ms.to_string()}else{delivery.to_owned()},
                "priceFilter":{"tickSize":tick},"lotSizeFilter":{"qtyStep":step},
                "metadataSource":format!("archived CSV: {}",file.path.file_name().map(|x|x.to_string_lossy()).unwrap_or_default())
            }));
        }
    }
    specs
}

fn specs_for(
    symbols: &BTreeSet<String>,
    need_linear: bool,
    archived: &BTreeMap<String, Value>,
) -> Result<BTreeMap<String, Value>> {
    let client = Client::builder()
        .user_agent("bybit-nautilus-csv-converter-rust/1.0")
        .timeout(Duration::from_secs(20))
        .build()?;
    let mut all = BTreeMap::new();
    let mut requests = Vec::new();
    if need_linear {
        requests.push(("linear", "BTCUSDT".to_string()));
    }
    for s in symbols {
        requests.push(("option", s.clone()));
    }
    let mut unresolved = Vec::new();
    for (category, symbol) in requests {
        if category == "option" {
            if let Some(spec) = archived.get(&symbol) {
                all.insert(symbol, spec.clone());
                continue;
            }
        }
        let Some(spec) = exact_spec(&client, category, &symbol) else {
            if category == "option" {
                unresolved.push(symbol);
                continue;
            }
            bail!("Bybit returned no exact instrument specification for {category}:{symbol}");
        };
        let tick = spec["priceFilter"]["tickSize"]
            .as_str()
            .or(spec["tickSize"].as_str())
            .ok_or_else(|| anyhow!("{symbol} spec lacks tickSize"))?;
        let step = spec["lotSizeFilter"]["qtyStep"]
            .as_str()
            .or(spec["qtyStep"].as_str())
            .ok_or_else(|| anyhow!("{symbol} spec lacks qtyStep"))?;
        if decimal(tick, "tickSize")? <= Decimal::ZERO || decimal(step, "qtyStep")? <= Decimal::ZERO
        {
            bail!("{symbol} has invalid instrument increments");
        }
        all.insert(symbol, spec);
        thread::sleep(Duration::from_millis(50));
    }
    if !unresolved.is_empty() {
        let (tick, step, as_of_ms, count) = uniform_option_rules(&client)?;
        for symbol in unresolved {
            all.insert(
                symbol.clone(),
                expired_option_spec(&symbol, &tick, &step, as_of_ms, count)?,
            );
        }
    }
    Ok(all)
}

fn run() -> Result<()> {
    let args = Args::parse();
    if args.files.is_empty() {
        bail!("provide one or more CSV paths; the Rust version uses CLI selection instead of a desktop file picker");
    }
    let files: Vec<CsvData> = args
        .files
        .iter()
        .map(|p| read_csv(p))
        .collect::<Result<_>>()?;
    let masters: Vec<_> = files
        .iter()
        .filter(|f| {
            f.headers.iter().any(|h| h == "record_type")
                && f.rows.iter().any(|r| {
                    r.get(
                        f.headers
                            .iter()
                            .position(|h| h == "record_type")
                            .unwrap_or(usize::MAX),
                    )
                    .is_some_and(|x| {
                        matches!(
                            x,
                            "underlying_candle"
                                | "option_execution"
                                | "option_quote"
                                | "option_ticker"
                        )
                    })
                })
        })
        .collect();
    let (bar_data, tick_data, quote_data, public_trades) = if !masters.is_empty() {
        if files.len() != 1 || masters.len() != 1 {
            bail!("select the master CSV by itself, or select original CSVs; do not mix both");
        }
        let f = masters[0];
        let has_c = f
            .rows
            .iter()
            .any(|r| cell(r, &f.headers, "record_type").is_ok_and(|v| v == "underlying_candle"));
        let has_e = f
            .rows
            .iter()
            .any(|r| cell(r, &f.headers, "record_type").is_ok_and(|v| v == "option_execution"));
        let has_q = f.rows.iter().any(|r| {
            cell(r, &f.headers, "record_type")
                .is_ok_and(|v| v == "option_quote" || v == "option_ticker")
        });
        (
            has_c.then_some(f),
            has_e.then_some(f),
            has_q.then_some(f),
            false,
        )
    } else {
        let mut b = None;
        let mut t = None;
        let mut raw = None;
        let mut q = None;
        for f in &files {
            if has(
                &f.headers,
                &["timestamp_ms", "open", "high", "low", "close", "volume_btc"],
            ) {
                b = Some(f)
            }
            if has(
                &f.headers,
                &[
                    "exec_id",
                    "order_id",
                    "timestamp_ms",
                    "symbol",
                    "side",
                    "exec_qty_btc",
                    "exec_price_usdt",
                    "exec_fee_usdt",
                    "is_maker",
                ],
            ) {
                t = Some(f)
            }
        }
        for f in &files {
            if has(&f.headers, &["symbol", "price", "size", "side", "time"]) {
                raw = Some(f)
            }
            let time_cols = [
                "timestamp_ms",
                "timestamp",
                "timestamp_utc",
                "timestamp_ns",
                "timestamp_us",
                "ts",
                "ts_ms",
                "ts_ns",
                "time",
                "datetime",
                "exchange_timestamp",
                "ts_event",
            ];
            let has_time = time_cols
                .iter()
                .any(|name| f.headers.iter().any(|h| h.eq_ignore_ascii_case(name)));
            if has_time
                && ((has(
                    &f.headers,
                    &["symbol", "bid_price", "bid_size", "ask_price", "ask_size"],
                )) || has(
                    &f.headers,
                    &["symbol", "bidPrice", "bidSize", "askPrice", "askSize"],
                ))
            {
                q = Some(f);
            }
        }
        if t.is_some() && raw.is_some() {
            bail!("select one option execution/trade CSV at a time");
        }
        if t.is_none() {
            t = raw;
        }
        if b.is_none() && t.is_none() && q.is_none() {
            bail!("no supported market CSV found (need BTC OHLCV, option trades, or timestamped option quotes)");
        }
        if b.is_some() && t.is_some() && std::ptr::eq(b.unwrap(), t.unwrap()) {
            bail!("underlying candles and option trades must be separate CSV files");
        }
        (b, t, q, raw.is_some())
    };
    let bars = bar_data
        .map(normalize_bars)
        .transpose()?
        .unwrap_or_default();
    let ticks = tick_data
        .map(|d| normalize_ticks(d, public_trades))
        .transpose()?
        .unwrap_or_default();
    let (quotes, greeks) = quote_data
        .map(normalize_quotes)
        .transpose()?
        .unwrap_or_default();
    let mut symbols: BTreeSet<String> = ticks
        .iter()
        .map(|x| x.instrument_id.replace("-OPTION.BYBIT", ""))
        .collect();
    symbols.extend(quotes.iter().map(|x| x.symbol.clone()));
    let archived = load_archived_specs(&files);
    let specs = specs_for(&symbols, !bars.is_empty(), &archived)
        .context("instrument metadata lookup failed; no output was written")?;
    for tick in &ticks {
        let symbol = tick.instrument_id.replace("-OPTION.BYBIT", "");
        let s = &specs[&symbol];
        let tick_size = decimal(
            s["priceFilter"]["tickSize"]
                .as_str()
                .or(s["tickSize"].as_str())
                .unwrap(),
            "tickSize",
        )?;
        let qty_step = decimal(
            s["lotSizeFilter"]["qtyStep"]
                .as_str()
                .or(s["qtyStep"].as_str())
                .unwrap(),
            "qtyStep",
        )?;
        if decimal(&tick.price_usdt, "price")? % tick_size != Decimal::ZERO
            || decimal(&tick.size_btc, "size")? % qty_step != Decimal::ZERO
        {
            bail!("{symbol}: execution conflicts with fetched tickSize/qtyStep");
        }
    }
    fs::create_dir_all(&args.csv_output)?;
    fs::create_dir_all(&args.catalog)?;
    if !bars.is_empty() {
        write_csv(&args.csv_output.join("btc_underlying_bars.csv"), &bars)?;
    }
    if !ticks.is_empty() {
        write_csv(
            &args.csv_output.join("btc_option_execution_ticks.csv"),
            &ticks,
        )?;
    }
    if !quotes.is_empty() {
        write_csv(&args.csv_output.join("btc_option_quotes.csv"), &quotes)?;
    }
    if !greeks.is_empty() {
        write_csv(&args.csv_output.join("btc_option_greeks.csv"), &greeks)?;
    }
    fs::write(
        args.catalog.join("instrument_specs.json"),
        serde_json::to_vec_pretty(&specs)?,
    )?;
    let summary = json!({"underlying_instrument_id":UNDERLYING_ID,"bar_count":bars.len(),"option_tick_count":ticks.len(),"option_quote_count":quotes.len(),"option_greeks_count":greeks.len(),"option_instrument_ids":symbols.iter().map(|s|format!("{s}-OPTION.BYBIT")).collect::<Vec<_>>(),"native_nautilus_parquet_written":false,"note":"Rust port writes normalized CSV and instrument_specs.json. Use NautilusTrader's catalog API to ingest these rows into a native catalog."});
    fs::write(
        args.catalog.join("conversion_summary.json"),
        serde_json::to_vec_pretty(&summary)?,
    )?;
    println!(
        "Wrote {} bars, {} option executions, {} quotes, and {} Greeks records",
        bars.len(),
        ticks.len(),
        quotes.len(),
        greeks.len()
    );
    println!("Normalized CSVs: {}", args.csv_output.display());
    println!("Metadata and summary: {}", args.catalog.display());
    Ok(())
}

fn main() {
    if let Err(err) = run() {
        eprintln!("error: {err:#}");
        std::process::exit(2);
    }
}
