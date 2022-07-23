# historical_market_database
Project that retrieves end of day data from yahoo finance and stores in influxDB
```
USAGE:
    historical_market_database --start <START> --end <END> --symbol <SYMBOL> <SUBCOMMAND>

OPTIONS:
    -b, --start <START>      Start data for stock price search
    -e, --end <END>          End data for stock price search
    -h, --help               Print help information
    -s, --symbol <SYMBOL>    Stock symbol ticker to lookup prices for
    -V, --version            Print version information

SUBCOMMANDS:
    help    Print this message or the help of the given subcommand(s)
    max     
    min 
```    