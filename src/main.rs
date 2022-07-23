//#![deny(warnings)]
//#![warn(clippy::all, clippy::restriction, clippy::pedantic, clippy::nursery)]
use anyhow::{Context, Result};
use clap::{Parser, Subcommand};

use futures::prelude::*;
use influxdb::InfluxDbWriteable;
use influxdb::{Client, ReadQuery, WriteQuery};
use std::env;
use yahoo_finance::{history, Interval, Timestamped};

use chrono::{DateTime, NaiveDateTime, Utc};
use serde::{Deserialize, Serialize};

/// Search for a pattern in a file and display the lines that contain it.
#[derive(Parser, Debug)]
#[clap(about, version, author)]
struct Cli {
    /// Operation to perform on database
    #[clap(subcommand)]
    op: Operation,

    /// Start data for stock price search
    #[clap(short = 'b', long)]
    start: String,

    /// End data for stock price search
    #[clap(short = 'e', long)]
    end: String,

    /// Stock symbol ticker to lookup prices for
    #[clap(short = 's', long)]
    symbol: String,
}

#[derive(Subcommand, Debug)]
enum Operation {
    Max,
    Min,
}

#[derive(Debug, InfluxDbWriteable, Serialize, Deserialize)]
struct StockPrice {
    time: DateTime<Utc>,
    open: f64,
    high: f64,
    low: f64,
    close: f64,
    volume: Option<u64>,
    #[influxdb(tag)]
    symbol: String,
}

#[derive(Debug)]
struct BarWrapper(String, yahoo_finance::Bar);

impl From<(String, yahoo_finance::Bar)> for BarWrapper {
    fn from(pair: (String, yahoo_finance::Bar)) -> Self {
        BarWrapper(pair.0, pair.1)
    }
}

impl From<(String, &yahoo_finance::Bar)> for BarWrapper {
    fn from(bar: (String, &yahoo_finance::Bar)) -> Self {
        BarWrapper(bar.0, *bar.1)
    }
}

impl Default for BarWrapper {
    fn default() -> Self {
        Self(
            String::default(),
            yahoo_finance::Bar {
                timestamp: Default::default(),
                open: Default::default(),
                high: Default::default(),
                low: Default::default(),
                close: Default::default(),
                volume: Default::default(),
            },
        )
    }
}

impl InfluxDbWriteable for BarWrapper {
    fn into_query<I: Into<String>>(self, name: I) -> WriteQuery {
        WriteQuery::new(
            influxdb::Timestamp::Milliseconds(
                <u128 as TryFrom<i64>>::try_from(self.1.timestamp)
                    .expect("Could not convert i64 to u128"),
            ),
            name.into(),
        )
        .add_field("open", self.1.open)
        .add_field("high", self.1.high)
        .add_field("low", self.1.low)
        .add_field("close", self.1.close)
        .add_field("volume", self.1.volume)
        .add_tag("symbol", self.0)
    }
}

async fn query_database(
    client: Client,
    op: &Operation,
    bucket: &str,
    symbol: &str,
    start_date: NaiveDateTime,
    end_date: NaiveDateTime,
) -> Result<Vec<f64>, Box<dyn std::error::Error>> {
    let qs = format!(
        "SELECT * FROM {} WHERE time >= '{}' and time < '{}' and \"symbol\" = '{}'",
        bucket, start_date, end_date, symbol
    );
    let read_query = ReadQuery::new(qs.to_string());
    let mut db_result = client.json_query(read_query).await?;

    let result: Vec<f64>;

    match op {
        Operation::Max => {
            println!(
                "Searching for {:?} highest close price between {:?} and {:?}",
                symbol, start_date, end_date
            );

            result = db_result
                .deserialize_next::<StockPrice>()?
                .series
                .into_iter()
                .map(|i| i.values[0].high)
                .collect::<Vec<_>>();
        }
        Operation::Min => {
            println!(
                "Searching for {:?} lowest close price between {:?} and {:?}",
                symbol, start_date, end_date
            );
            result = db_result
                .deserialize_next::<StockPrice>()?
                .series
                .into_iter()
                .map(|i| i.values[0].low)
                .collect::<Vec<_>>();
        }
    }
    Ok(result)
}

async fn get_symbol_data(
    symbol: &str,
    interval: Interval,
) -> Result<Vec<yahoo_finance::Bar>, Box<dyn std::error::Error>> {
    Ok(history::retrieve_interval(symbol, interval).await?)
}

async fn get_symbol_list_data(
    symbols: &[String],
    interval: Interval,
) -> Result<Vec<Vec<yahoo_finance::Bar>>, Box<dyn std::error::Error>> {
    let mut symbols_data: Vec<Vec<yahoo_finance::Bar>> = Vec::with_capacity(symbols.len());
    for symbol in symbols.iter() {
        symbols_data.push(get_symbol_data(symbol, interval).await?)
    }
    Ok(symbols_data)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Cli::parse();

    let start_date =
        NaiveDateTime::parse_from_str(&args.start, "%Y-%m-%d %H:%M:%S").with_context(|| {
            format!(
                "could not convert {:?} to YYYY-MM-DD HH:MM:SS format",
                &args.start
            )
        })?;
    let end_date =
        NaiveDateTime::parse_from_str(&args.end, "%Y-%m-%d %H:%M:%S").with_context(|| {
            format!(
                "could not convert {:?} to YYYY-MM-DD HH:MM:SS format",
                &args.end
            )
        })?;

    let host =
        env::var("INFLUX_HOST").unwrap_or("https://us-east-1-1.aws.cloud2.influxdata.com".into());
    let org = env::var("INFLUX_ORG").unwrap_or("test".into());
    let token = env::var("INFLUX_TOKEN").unwrap_or(
        "ktA-cBpdbqfGNdGs5KiZ3JYk4uE5TxtsnfHu6HGnM109vVG323J-J-5VnBFZA0KlAjsFmosmRpKJgJni_gi9ww=="
            .into(),
    );
    let bucket = env::var("INFLUX_BUCKET").unwrap_or("i".into());

    let client =
        Client::new("https://us-east-1-1.aws.cloud2.influxdata.com", &bucket).with_token(&token);
    println!("database_name is: {:?}", client.database_name());
    let (build_name, version_num) = client.ping().await?;
    println!(
        "Build Name: {:?}\nVersion Num is: {:?}",
        build_name, version_num
    );
    
    let interval = Interval::_6mo;
    let symbols = vec!["AAPL".to_string(), "MSFT".to_string()];
    let data_vec = get_symbol_list_data(&symbols, interval).await?;
    /*
    {
        let mut symbol_iter = symbols.iter();
        for bar_vec in &data_vec {
            let symbol = symbol_iter.next();
            println!("Number of symbols in data_vec is {}", &data_vec.len());
            println!("Symbol is:  {:?}", symbol);
            for bar in bar_vec {
                client.clone()
                    .query(
                        &BarWrapper::from((symbol.expect("Symbol Bar mismatch").clone(), bar))
                            .into_query(&bucket),
                    )
                    .await?;
            }
        }
    }
    */
    

    let result = query_database(
      client.clone(),
      &args.op,
      &bucket,
      &args.symbol,
      start_date,
      end_date,
  )
  .await?;
  println!("Op result is: {:?}", result);
    Ok(())
}
