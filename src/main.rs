#![deny(warnings)]
#![warn(clippy::all, clippy::pedantic, clippy::nursery)]
use anyhow::{Context, Result};
use clap::{Parser, Subcommand};

//use futures::prelude::*;
use influxdb::InfluxDbWriteable;
use influxdb::{Client, ReadQuery, WriteQuery};
//use std::env;
use yahoo_finance::{history, Interval};

use chrono::{DateTime, NaiveDateTime, Utc};
use serde::{Deserialize, Serialize};

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

//This struct will hold the result from a database query
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

//Can't implement traits on structs I don't own, wrap Bar
#[derive(Debug)]
struct BarWrapper(String, yahoo_finance::Bar);

impl From<(String, yahoo_finance::Bar)> for BarWrapper {
    fn from(pair: (String, yahoo_finance::Bar)) -> Self {
        Self(pair.0, pair.1)
    }
}

impl From<(String, &yahoo_finance::Bar)> for BarWrapper {
    fn from(bar: (String, &yahoo_finance::Bar)) -> Self {
        Self(bar.0, *bar.1)
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
                volume: std::option::Option::default(),
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

//This function will query the database for the provided operation, currently only MAX or MIN
// Need to change query so that it returns that entire struct back at the matching point so that
// the query return type is always the same.  OR make the deserializing smarter so that it knows
// what it should expect to receive
async fn query_database(
    client: Client,
    op: &Operation,
    bucket: &str,
    symbol: &str,
    start_date: NaiveDateTime,
    end_date: NaiveDateTime,
) -> Result<f64, Box<dyn std::error::Error>> {
    let result: f64 = match op {
        Operation::Max => {
            #[derive(Serialize, Deserialize)]
            struct MaxResult {
                max: f64,
            }
            println!(
                "Searching for {:?} highest close price between {:?} and {:?}",
                symbol, start_date, end_date
            );

            let qs = format!(
                "SELECT MAX(high) FROM {} WHERE time >= '{}' and time < '{}' and \"symbol\" = '{}'",
                bucket, start_date, end_date, symbol
            );
            let read_query = ReadQuery::new(qs.to_string());
            let mut db_result = client.json_query(read_query).await?;

            db_result
                .deserialize_next::<MaxResult>()?
                .series
                .into_iter()
                .next()
                .expect("Could not iterate into query result")
                .values
                .first()
                .expect("Could not get entry from result values")
                .max
        }
        Operation::Min => {
            #[derive(Serialize, Deserialize)]
            struct MinResult {
                min: f64,
            }
            println!(
                "Searching for {:?} lowest close price between {:?} and {:?}",
                symbol, start_date, end_date
            );

            let qs = format!(
                "SELECT MIN(low) FROM {} WHERE time >= '{}' and time < '{}' and \"symbol\" = '{}'",
                bucket, start_date, end_date, symbol
            );
            let read_query = ReadQuery::new(qs.to_string());
            let mut db_result = client.json_query(read_query).await?;
            db_result
                .deserialize_next::<MinResult>()?
                .series
                .into_iter()
                .next()
                .expect("Could not iterate into query result")
                .values
                .first()
                .expect("Could not get entry from result values")
                .min
        }
    };
    Ok(result)
}

// queries yahoo_finance for the bars it has in the provided interval
async fn get_symbol_data(
    symbol: &str,
    interval: Interval,
) -> Result<Vec<yahoo_finance::Bar>, Box<dyn std::error::Error>> {
    Ok(history::retrieve_interval(symbol, interval).await?)
}

//This function gets all the bars for a given interval for all the provided symbols
async fn get_symbol_list_data(
    symbols: &[String],
    interval: Interval,
) -> Result<Vec<Vec<yahoo_finance::Bar>>, Box<dyn std::error::Error>> {
    let mut symbols_data: Vec<Vec<yahoo_finance::Bar>> = Vec::with_capacity(symbols.len());
    for symbol in symbols.iter() {
        symbols_data.push(get_symbol_data(symbol, interval).await?);
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
    /*
    let host = env::var("INFLUX_HOST")
        .expect("Please set INFLUX_HOST environment variable to your influxDB token");
    let token = env::var("INFLUX_TOKEN")
        .expect("Please set INFLUX_TOKEN environment variable to your influxDB token");
    let bucket = env::var("INFLUX_BUCKET")
        .expect("Please set INFLUX_BUCKET environment variable to your influxDB token");
        */

    let host = "https://us-east-1-1.aws.cloud2.influxdata.com".to_string();
    let token =
        "NpTygduFyn2V95hjrIx8gkZ2kLGpglmWoIe4Ulnhn520kbWrbaqi1iWn59B6PE3p0wI7LmwC_AVZXqEUuppqKQ=="
            .to_string();
    let bucket = "j".to_string();
    let client = Client::new(host, &bucket).with_token(&token);

    //Health check to see if we can at least communicate with database
    //if not we will bail here
    let (_build_name, _version_num) = client.ping().await?;
    let interval = Interval::_6mo;
    let symbols = vec!["AAPL".to_string(), "MSFT".to_string()];
    let _data_vec = get_symbol_list_data(&symbols, interval).await?;
    /*
    //Need to implement influxdb2 api to allow for streaming datapoints to the database, much faster
    {
        let mut symbol_iter = symbols.iter();
        for bar_vec in &data_vec {
            let symbol = symbol_iter.next();
            println!("Number of symbols in data_vec is {}", &data_vec.len());
            println!("Symbol is:  {:?}", symbol);
            for bar in bar_vec {
                client
                    .clone()
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
