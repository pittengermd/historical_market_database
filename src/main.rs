#![deny(warnings)]
#![warn(clippy::all, clippy::restriction, clippy::pedantic, clippy::nursery)]
use influxdb2::write::WriteQuery;
use influxdb2::models::Query;
use influxdb2::Client;
use influxdb_line_protocol::{Field, Point};
use std::env;
use yahoo_finance::{history, Interval};
use anyhow::{Context, Result};
use chrono::NaiveDateTime;
use clap::{Parser, Subcommand};
use influxdb2_structmap::FromMap;


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
struct Test(f32);

async fn query_database(
    client: Client,
    op: &Operation,
    symbol: &str,
    start_date: NaiveDateTime,
    end_date: NaiveDateTime,
) -> Result<f32, Box<dyn std::error::Error>> {
    let mut qs = format!(
        "from(bucket: \"stock-prices\") 
        |> range(start: {}, end: {})
        |> filter(fn: (r) => r.ticker == \"{}\")",
        symbol,
        start_date.timestamp_millis(),
        end_date.timestamp_millis()
    );
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
    let res: Vec<Test> = client.query(Some(query)).await
    .with_context(|| format!("failed to query database for {:?} value", op))?;
    Ok(res[0].0)
}

fn connect_to_influxdb(host: &str, org: &str, token: &str) -> Result<Client, Box<dyn std::error::Error>> {
   Ok(Client::new(host, org.clone(), token)?)
}

async fn get_symbol_data(symbol: &str, interval: Interval) -> Result<Vec<yahoo_finance::Bar>, Box<dyn std::error::Error>> {
   Ok(history::retrieve_interval(symbol, interval).await?)
}

async fn get_symbol_list_data(symbols: &[String], intervals: &[Interval]) -> Result<Vec<Vec<yahoo_finance::Bar>>, Box<dyn std::error::Error>> {
   let mut symbols_data: Vec<Vec<yahoo_finance::Bar>> = Vec::with_capacity(symbols.len());
   for (symbol, interval) in symbols.iter().zip(intervals) {
      symbols_data.push(get_symbol_data(symbol, *interval).await?)
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
    let bucket = env::var("INFLUX_BUCKET").unwrap_or("test".into());

    let client = connect_to_influxdb(&host, &org, &token)?;
    let interval = vec![Interval::_6mo];
    let symbol = vec!["AAPL".to_string()];
    let data_vec = get_symbol_list_data(&symbol, &interval).await?;

    for bar_vec in data_vec {
      for bar in bar_vec {
         let fields = vec![
            Field::new("open", bar.open).expect("Failed to create open field"),
            Field::new("high", bar.high).expect("Failed to create high field"),
            Field::new("low", bar.low).expect("Failed to create low field"),
            Field::new("close", bar.close).expect("Failed to create close field"),
            Field::new("volume", bar.volume.expect("No Volume Data"))
                .expect("Failed to create volume field"),
        ];
        let point = Point::builder(bar.timestamp.to_string())
            .unwrap()
            .add_fields(fields)
            .build()
            .unwrap();

        let query = WriteQuery::with_org(&bucket, &org);
        let _result = client.clone().write(point, query).await?;
      }        
    }

    Ok(())
}
