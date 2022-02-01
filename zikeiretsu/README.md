## Run sample script
```
cargo run --bin zikeiretsu -- -x -e sample_env list
```

## query example
### 1. show datapoints in today
```
with
	cols = [is_buy, volume, price]
	tz = +9

select ts, is_buy, volume, price
from trades
where ts in today()

```

### 2. show datapoints in specific range
```
with
	cols = [_, volume, price]
	tz = JST

select ts, volume, price
from trades
where ts in ('2012-12-13 9:00:00+09:00', '2012-12-13 9:00:00+09:00')

```
