## zikeiretsu-rs
A toy time series DB

## Features
- Nanoseconds accuracy timestamps
- Multiple values in a datapoint
- Sync with Cloud storage

## Install
```toml
zikeiretsu = {git = "https://github.com/tacogips/zikeiretsu-rs", tag = "0.1.10" }
```

```
# install as binary
git clone https://github.com/tacogips/zikeiretsu-rs
cd zikeiretsu-rs/zikeiretsu
cargo install --bin zikeiretsu --path .

```

## Usage

```

See `zikreitsu --help`
zikreitsu -x -e {your_env_files} list # show all metrics loading from Cloud storage
```

