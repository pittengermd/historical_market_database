//#![deny(warnings)]
//#![warn(clippy::all, clippy::restriction, clippy::pedantic, clippy::nursery)]
use anyhow::{Context, Result};
use chrono::NaiveDateTime;
use clap::{Parser, Subcommand};
//use influxdb2::models::Query;
use influxdb2::Client;
use influxdb2_structmap::FromMap;
use std::env;
use yahoo_finance::{history, Interval};
use futures::prelude::*;
use influxdb2::models::DataPoint;
use influxdb2::models::Query;
use yahoo_finance::Timestamped;
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

#[derive(Debug, influxdb2_structmap_derive::FromMap, Default)]
pub struct StockPrice {
   ticker: String,
   value: f64,
   time: i64,
}

async fn query_database(
    client: Client,
    op: &Operation,
    bucket: &str,
    symbol: &str,
    start_date: NaiveDateTime,
    end_date: NaiveDateTime,
) -> Result<f64, Box<dyn std::error::Error>> {
   let qs = format!("from(bucket: \"test1\") 
   |> range(start: -1w)
   |> filter(fn: (r) => r.ticker == \"{}\") 
   |> last()
", "AAPL");
/*
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
    }*/
    let query = Query::new(qs.to_string());
    let res: Vec<StockPrice> = client.query::<StockPrice>(Some(query))
        .await?;
   println!("result is: {:?}", res);
    //Ok(res[0].1.high)
    Ok(1.0)
}


fn connect_to_influxdb(
    host: &str,
    org: &str,
    token: &str,
) -> Result<Client, Box<dyn std::error::Error>> {
    Ok(Client::new(host, org.clone(), token))
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

    let host = env::var("INFLUX_HOST").unwrap_or("http://localhost:8086".into());
    let org = env::var("INFLUX_ORG").unwrap_or("example-org".into());
    let token = env::var("INFLUX_TOKEN").unwrap_or(
        "iPRI-RQKTiCjnjXV3lFaMbnR8lg3h43-HmHjlZkKYF1XzKdviyl4sOP2xl_h53YOSDaUIhi0NNdZDqYTrXpqXw=="
            .into(),
    );
    let bucket = env::var("INFLUX_BUCKET").unwrap_or("test1".into());

    let client = connect_to_influxdb(&host, &org, &token)?;
    let interval = Interval::_6mo;
    let symbols = vec!["AAPL".to_string()];
    let data_vec = get_symbol_list_data(&symbols, interval).await?;
    let mut symbol_iter = symbols.iter();
    
    println!("HealthCheck: {:#?}", client.health().await?);
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
      */
      let points: Vec<DataPoint> = vec![ DataPoint::builder("bar")
      //bar timestamp is in millis, influxdb stores timestamp as nano
      .tag("ticker", "AAPL")
      .field("time", 0)
      .field("value", 123.45)
      .build()?, ];
      client.write(&bucket, stream::iter(points)).await?;
    //}
    let max = query_database(client, &args.op, &bucket, &args.symbol, start_date, end_date).await?;
    println!("Max price in interval is: {}", max);

    Ok(())
}
