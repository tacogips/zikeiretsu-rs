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

## Structure of Files

```

 zkdb
  ├─ wal (not implemented yet though..)
  │   │
  │   └─ ...
  │
  ├─ error
  │   │
  │   └── ...
  │
  │
  ├─ blocklist
  │   │
  │   ├── {metrics_1}.list
  │   └── {metrics_2}.list
  │
  └── block
      │
      │
      ├── [metrics_1]
      │       ├── 1626
      │       │    ├─ 162688734_162688740
      │       │    │      └─ block
      │       │    │
      │       │    └─ 162688736_162688750
      │       │          └─ block
      │       ├── 1627
      │       │    └─ ...
      │       │
      │       ├── 1628
      │       │    ├─ 162888734_162788735
      │       │    │     └─ block
      │       │    │
      │       │    └─ 162888735_162788790
      │       │          └─ block
      │       ├─ ...
      │
      ├── [metrics_2]

```

## Block list
### File name
`{metrics_1}.list`

timestamp of the block files. Its file name identical with the metrics ID (thatg contains only [a-zA-Z_-0-9]).

```
┌──────────────────────────────┬──────────────────────────────────┬───────────────────────────────────────────┬──────────────────────────────────────────────┐
│ (1)updated timestamp(8 byte) | (2)number of timestamp (n bytes) │ (3) timestamp second head(since) (v byte) │ (4) timestamp second deltas (since)(v byte)  │
└──────────────────────────────┴──────────────────────────────────┴───────────────────────────────────────────┴──────────────────────────────────────────────┘
┌────────────────────────────────────────────┬──────────────────────────────────────────────┐
│ (5) timestamp second head (until) (v byte) │ (6) timestamp second deltas (until)(v byte)  │
└────────────────────────────────────────────┴──────────────────────────────────────────────┘

```

(1) latest updated timestamp of this block list file

(2) number of timestamps encoding by [Base 128 Variants](https://developers.google.com/protocol-buffers/docs/encoding#varints)

(4) head timestamp (second) encoding by [Base 128 Variants](https://developers.google.com/protocol-buffers/docs/encoding#varints)

(4) `timestamp second deltas` are timestamp followed by deleta-encoded and compressed by `simple8b-rle`.

(5) head timestamp (second) encoding by [Base 128 Variants](https://developers.google.com/protocol-buffers/docs/encoding#varints)

(6) `timestamp second deltas` are timestamp followed by deleta-encoded and compressed by `simple8b-rle`.


## Block Data

### File name

`block`

## Format of a metrics data block

A metrics that contains timestamps ,multiple datas along with its own size.

```
┌───────────────────────────────────┬─────────────────────────────┬────────────────────────────┬─────────────────────────────────────────────────────────────┐
│ (1)number of datapoints (n bytes) │ (2)data fields num (1 byte) │ (3)type of field_1(1 byte) │ ... (type of field block repeated over the number of fields)│
└───────────────────────────────────┴─────────────────────────────┴────────────────────────────┴─────────────────────────────────────────────────────────────┘
┌─────────────────────────────┬──────────────────────────────────────┬────────────────────────────────────────────────────────┐
│ (4)head timestamp (8 byte)  │ (5)timestamp deltas(sec)(8 byte * n) │ (6) common trailing zero num of timestamp nano (8 bits)│
└─────────────────────────────┴──────────────────────────────────────┴────────────────────────────────────────────────────────┘
┌────────────────────────────────────────────────────────────┬───────────────────────────────┬──────────────────────────────────────┐
│ (7) timestamp (sub nanosec since latest second ) (n bytes) │ (8)datas of field 1(n bytes)  │ ... (reapeat over number of fields)  │
└────────────────────────────────────────────────────────────┴───────────────────────────────┴──────────────────────────────────────┘

```

### (1) number of data
Number of timestamps in the metrics block

encoding by [Base 128 Variants](https://developers.google.com/protocol-buffers/docs/encoding#varints)

### (2) data field num
Number of fields. 1 - 256

### (3) Type of fields
Data types of each fields.

#### data and compression type
- integer   					 ... 1 (not supported yet)
- float   	  			   ... 2
- string      			   ... 3 (not supported yet)
- timestamp nono   		 ... 4 (not supported yet)
- bool        			   ... 5
- unsigned int   			 ... 6 (not supported yet)
- timestamp sec   		 ... 7 (not supported yet)

### (4) Head timestamp

the head of the timestamps in the format of Unix-timestamp in nano seconds in 64 bits

### (5) Timestamps deltas(second)

The encoded timestamps deltas following the head timemstamp in seconds .

these values are compressed with [simple8b-rle](https://github.com/lemire/FastPFor/blob/master/headers/simple8b_rle.h)
The delta value that is greater than (1 << 60) - 1 can't be stored (error will occured when encoding)

#### Example
Say, there are 4 timestamps as below.

##### Timestamps
-  1627467076_985012256
-  1627467250_785267037
-  1627468063_600895795
-  1627468158_257010309

##### Timestamps (floor by seconds)
-  1627467076[985012000] ->  1627467076
-  1627467257[85267000]  ->  1627467257
-  1627468063[600895000] ->  1627468063
-  1627468158[257010000] ->  1627468158

##### Deltas
- 1627467257 - 1627467076 -> 181
- 1627468063 - 1627467257 -> 806
- 1627468158 - 1627468063 -> 95

### (6) common trailing zero num of timestamp nano (8 bits)

common trailing zero num that all values in `(7) timestamp (< sec as nano sec)` have.

#### Example
##### Nano seconds that put to (7) fields

-  [1627467076]985012000  -> 985012000
-  [1627467250]785267000  -> 785267000
-  [1627468063]600895000  -> 600895000
-  [1627468158]257010000  -> 257010000

##### to binary

- 111010101101100001011100100000
- 101110110011100011100100111000
- 100011110100001110111000011000
- 1111010100011010100101010000


##### pick common trailing zeros

- 111010101101100001011100100[000]
- 101110110011100011100100111[000]
- 100011110100001110111000011[000]
- 1111010100011010100101010[000]

every value has at least 3 trailing-zeros,so `3` will be stored .

#### About field size
the max value of nano seconds that put to `(7)` fields is 999999999,
`111011100110101100100111111111` in binary.so `29` is the max value that will be this field. 5 bits in the 8 bits is sufficient but we're remaining heading 3 bits for extension in the future.

### (7) timestamp (sub nano sec since latest second)

nano second value without its seconds value and common trailing zeros that complements to `(5) Timestamps deltas(second)`.
the values will be encoded by simple8b-rle

#### example

##### timestamps
- 1627467076_985012000
- 1627467250_785267000
- 1627468063_600895000
- 1627468158_257010000

##### to nano seconds

- [1627467076]985012000  -> 985012000
- [1627467250]785267000  -> 785267000
- [1627468063]600895000  -> 600895000
- [1627468158]257010000  -> 257010000

##### to binary

- 111010101101100001011100100000
- 101110110011100011100100111000
- 100011110100001110111000011000
- 1111010100011010100101010000


##### drop common trailing zeros
every value has at least 3 trailing-zeros.drop these zeros.

- 111010101101100001011100100[000]
- 101110110011100011100100111[000]
- 100011110100001110111000011[000]
- 1111010100011010100101010[000]

( then `3` will be stored to `(6) common trailing zero num of timestamp nano`)

### (8) Datas of field

#### Compressing Algorithms of each type

- integer ... (not implemented yet) if the value is less than (1 << 60) - 1 ,convert with [ZigZag Encoding](https://developers.google.com/protocol-buffers/docs/encoding) then compress with  [simple8b-rle](https://github.com/lemire/FastPFor/blob/master/headers/simple8b_rle.h).We haven't decide how to handle with the outlier values(we are considering uncompress all values if the datas contains at least one value that exceed the maximum value, but it seems very unefficient...)
- float   ... XOR encoding of Facebook Gorilla (http://www.vldb.org/pvldb/vol8/p1816-teller.pdf)
- string      ...  (not implemented yet) consider to compress with [snappy](https://github.com/google/snappy)
- timestamp   ...  (not implemented yet) delta encoding and [simple8b-rle](https://github.com/lemire/FastPFor/blob/master/headers/simple8b_rle.h)
- bool        ...  Simply packing 1bit values into 64bits space

### WAL (WIP)
### format (WIP)

```
┌────────────────────────┬─────────────────────────────────────────┬────────────────────────────┬──────────────────────────┬─────────────────────────┬──────┐
│ (1)timestamp (8 bytes) │ (2)number of data fields num (1 byte)   │ (3)type of field_1(4 bit)  │ (4)field value (v bytes) │  type of field_2(4 bit) │ ...  │
└────────────────────────┴─────────────────────────────────────────┴────────────────────────────┴──────────────────────────┴─────────────────────────┴──────┘

```


## TODO
- [ ] Validations
	- [ ] Metrics
	- [ ] Number of field
- [ ] WAL
- [ ] Support field types
- [ ] Deduplication


### References
[nakabonne's article](https://zenn.dev/nakabonne/articles/d300838a1500c7)

