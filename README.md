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
zikeiretsu 0.0.1

USAGE:
    zikeiretsu [OPTIONS] [QUERY]

ARGS:
    <QUERY>

OPTIONS:
    -c, --config <CONFIG>
            config for server and client.

    -d, --data_dir <DATA_DIR>
            [env: ZDB_DIR=]

        --databases <DATABASES>
            config for server. pass pair of database name and the bucket name join by '=' or just
            database name.
                    the value be separated by comma if pass multiple setting. e.g.
            databases=test_db_name=gs://test_bucket,test_db2,test_db3=gs://aaaa/bbb/cccc [env:
            ZDB_DATABASES=]

    -h, --help
            Print help information

        --host <HOST>
            config for server and client.

        --https
            config for server and client.

    -m, --mode <MODE>


        --port <PORT>
            config for server and client.

        --service_account <SERVICE_ACCOUNT_FILE_PATH>
            config for server. path to google service account file [env: ZDB_SERVICE_ACCOUNT=]

    -V, --version
            Print version information
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
