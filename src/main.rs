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
/*
async fn query_database(
    client: Client,
    op: &Operation,
    bucket: &str,
    symbol: &str,
    start_date: NaiveDateTime,
    end_date: NaiveDateTime,
) -> Result<f64, Box<dyn std::error::Error>> {
    let qs = format!(
        "from(bucket: \"test1\")
   |> range(start: -1w)
   |> filter(fn: (r) => r.ticker == \"{}\")
   |> last()
",
        symbol
    );

    let mut qs = format!(
        "from(bucket: {})
        |> range(start: {}, end: {})
        |> filter(fn: (r) => r.symbol == \"{}\")",
         bucket,
        symbol,
        start_date.timestamp_millis(),
        end_date.timestamp_millis()
    );

    println!("Query suggestions are: {:#?}", client.query_suggestions().await?);

    match op {
        Operation::Max => {
            println!(
                "Searching for {:?} highest close price between {:?} and {:?}",
                symbol, start_date, end_date
            );
            qs.push_str("|> max()");
        }
        Operation::Min => {
            println!(
                "Searching for {:?} lowest close price between {:?} and {:?}",
                symbol, start_date, end_date
            );
            qs.push_str("|> min()");
        }
    }
    let query = Query::new(qs.to_string());
    let res: Vec<StockPrice> = client.query::<StockPrice>(Some(query)).await?;
    println!("result is: {:?}", res);
    //Ok(res[0].1.high)

    Ok(1.0)
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
*/

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
    let bucket = env::var("INFLUX_BUCKET").unwrap_or("g".into());

    let client =
        Client::new("https://us-east-1-1.aws.cloud2.influxdata.com", &bucket).with_token(&token);
    println!("database_name is: {:?}", client.database_name());
    let (build_name, version_num) = client.ping().await?;
    println!(
        "Build Name: {:?}\nVersion Num is: {:?}",
        build_name, version_num
    );

    //let interval = Interval::_6mo;
    //let symbols = vec!["AAPL".to_string()];
    //let data_vec = get_symbol_list_data(&symbols, interval).await?;
    //let mut symbol_iter = symbols.iter();
    /*
    let data = history::retrieve_interval("AAPL", Interval::_6mo)
        .await
        .unwrap();
    let data_wrapper: Vec<BarWrapper> = data
        .iter()
        .map(|d| BarWrapper::from(("AAPL".to_string(), d)))
        .collect();

    println!("Downloaded {} bars", data_wrapper.len());
    let write_time = std::time::Instant::now();
    for wrapper in data_wrapper {
        let write_result = client.query(&wrapper.into_query(&bucket)).await?;
    }
    println!("Writing took: {:?}", write_time.elapsed().as_secs());
    */

    // Let's see if the data we wrote is there
    let qs = format!("SELECT * FROM {} WHERE time >= '{}' and time < '{}'", bucket, start_date, end_date);
    let read_query = ReadQuery::new(qs.to_string());

    let read_time = std::time::Instant::now();
    let mut db_result = client.json_query(read_query).await?;
    let _result = db_result
        .deserialize_next::<StockPrice>()?
        .series
        .into_iter()
        .map(|i| i.values)
        .collect::<Vec<_>>();
    println!("Reading took: {:?}", read_time.elapsed().as_secs());
    println!("Result is: {:?}", _result);
    println!("Read {:?} bars", _result[0].len());

    /*
    for bar_vec in &data_vec {
      let mut points: Vec<DataPoint> = Vec::with_capacity(bar_vec.len());
      let symbol = symbol_iter.next();
      println!("Number of symbols in data_vec is {}", &data_vec.len());

      for bar in bar_vec {
         points.push(
             DataPoint::builder("bar")
                 //bar timestamp is in millis, influxdb stores timestamp as nano
                 .timestamp(bar.timestamp_millis() * 1_000_000)
                 .tag("symbol", symbol.expect("No symbol provided"))
                 .field("open", bar.open)
                 .field("high", bar.high)
                 .field("low", bar.low)
                 .field("close", bar.close)
                 .field("volume", bar.volume.unwrap_or_default() as i64)
                 .build()?,
         );
     }
      println!("Writing {} bars", points.len());

    let points: Vec<DataPoint> = vec![DataPoint::builder("bar")
        //bar timestamp is in millis, influxdb stores timestamp as nano
        .tag("ticker", "AAPL")
        .field("value", 123.46)
        .field("open", 200.0)
        .field("time", 0)
        .build()?];
    client.write(&bucket, stream::iter(points)).await?;
    //}
    let max = query_database(
        client,
        &args.op,
        &bucket,
        &args.symbol,
        start_date,
        end_date,
    )
    .await?;
    println!("Max price in interval is: {}", max);
    */

    Ok(())
}
