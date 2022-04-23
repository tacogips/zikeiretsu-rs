## zikeiretsu-rs
A toy time series DB

## Features
- Nanoseconds accuracy timestamps
- Multiple values in a datapoint
- Sync with Cloud storage(only Google Cloud Storge yet)

## Install
### cargo install
```
cargo install --git https://github.com/tacogips/zikeiretsu-rs --tag 0.0.1 zikeiretsu
```


### Cargo.toml
```toml
zikeiretsu = {git = "https://github.com/tacogips/zikeiretsu-rs", tag = "0.0.1" }
```

## Usage

```
USAGE:
    zikeiretsu [OPTIONS] [QUERY]

ARGS:
    <QUERY>

OPTIONS:
    -b, --bucket <BUCKET>                                [env: ZDB_BUCKET=]
    -c, --config <CONFIG>
    -d, --db_dir <DB_DIR>                                [env: ZDB_DIR=]
        --df_col <DF_COL_NUM>                            [env: ZDB_DATAFRAME_COL=]
        --df_row <DF_ROW_NUM>                            [env: ZDB_DATAFRAME_ROW=]
        --df_width <DF_WIDTH>                            [env: ZDB_DATAFRAME_WIDTH=]
    -h, --help                                           Print help information
    -p, --bucket_sub_path <BUCKET_SUB_PATH>              [env: ZDB_BUCKET_SUBPATH=]
        --service_account <SERVICE_ACCOUNT_FILE_PATH>    [env: ZDB_SERVICE_ACCOUNT=]
    -t, --cloud_type <CLOUD_TYPE>                        [env: ZDB_CLOUD_TYPE=]
    -V, --version                                        Print version information
```


### Pass the parameters
You can pass the parameters zikeiretsu DB via followings

- Config file
- Command arguments
- Environment variable

###


### Query Example
```



```
