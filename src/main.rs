use yahoo_finance::{history, Interval, Timestamped};
use influxdb::{Client, Query, Timestamp, ReadQuery};
use influxdb::InfluxDbWriteable;
use chrono::{DateTime, Utc};
use influxdb::WriteQuery;
use std::time::{Duration, SystemTime};

#[derive(Debug)]
struct BarWrapper {
   bar: yahoo_finance::Bar, 
}

impl From<yahoo_finance::Bar> for BarWrapper {
   fn from(item: yahoo_finance::Bar) -> Self {
      BarWrapper { bar: item}
   }
}

impl From<&yahoo_finance::Bar> for BarWrapper {
   fn from(item: &yahoo_finance::Bar) -> Self {
      BarWrapper { bar: *item}
   }
}

impl From<&mut yahoo_finance::Bar> for BarWrapper {
   fn from(item: &mut yahoo_finance::Bar) -> Self {
      BarWrapper { bar: *item}
   }
}

impl InfluxDbWriteable for BarWrapper {
   fn into_query<I: Into<String>>(self, name: I) -> WriteQuery {
      WriteQuery::new(Timestamp::Hours(1), name.into())
   }
}



#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>>{
    // Connect to db `historical_market_data` on `http://localhost:8086`
    let client = Client::new("http://localhost:8086", "historical_market_data")
                  .with_auth("pittengermdp", "ST@rred040!20!!");
    let (build_type, version_num) = client.ping().await?;
    println!("{:?}, {:?}", build_type, version_num);
    
    let data = history::retrieve_interval("AAPL", Interval::_6mo).await.unwrap();
    
    let data_wrapper: Vec<BarWrapper> = data
                        .iter()
                        .map(|bar| bar.into())
                        .collect();


   for wrapper in data_wrapper {
      let write_result = client
      .query(wrapper.into_query("bars")
               .add_field("bar", wrapper)
      )
      
      .await?;
      //assert!(write_result.is_ok(), "Write result was not okay");

      // Let's see if the data we wrote is there
      let read_query = ReadQuery::new("SELECT * FROM bars");

      let read_result = client.query(read_query).await;
      assert!(read_result.is_ok(), "Read result was not ok");
      println!("{}", read_result.unwrap());
    }  
    
    
    Ok(())
}