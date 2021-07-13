## zikeiretsu-rs
A  toy time seriese DB

## Motivation
TODO wrtie

##  Data files structure

```

 zkdb
  ├─ wal (not implemented yet though..)
  │  └─ ...
  │
  ├─ blocklist
  │   │
  │   ├── {metrics_1}.list
  │   └── {metrics_2}.list
  │
  └── data
      │
      │
      ├── [metrics_1]
      │       ├── 16268
      │       │    ├─ 162688734_162688740_1
      │       │ 	 │      └─ block
      │       │    └─ 162688736_162688750_1
      │       │          └─ block
      │       ├── 16269
      │       │    └─ ...
      │       │
      │       ├── 16278
      │       │    ├─ 162788734_162788735_1
      │       │    │     └─ block
      │       │    │
      │       │    └─ 162788735_162788790_1
      │       │          └─ block
      │       ├─ ...
      │
      ├── [metrics_2]

```

## Block list
### File name
`{metrics_1}.list`

timestamp of the block files. Its file name identical with the metrics ID (thatg contains only [a-zA-Z_-]).
This file will be compressed with `ZStandard`

```
┌──────────────────────────────┬──────────────────────────────────┬───────────────────────────────────────────┬──────────────────────────────────────────────┐
│ (1)updated timestamp(8 byte) | (2)number of timestamp (n bytes) │ (3) timestamp second head(since) (v byte) │ (4) timestamp second deltas (until)(v byte)  │
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
- integer   					 ... 1 (not implemented yet)
- float   	  			   ... 2
- string      			   ... 3 (not implemented yet)
- timestamp   			   ... 4 (not implemented yet)
- bool        			   ... 5 (not implemented yet)
- unsigned int   			 ... 6 (not implemented yet)

### (4) Head timestamp

the head of the timestamps in the format of Unix-timestamp in nano seconds in 64 bits

### (5) Timestamps deltas(second)

The encoded timestamps deltas following the head timemstamp in seconds .

these values are compressed with [simple8b-rle](https://github.com/lemire/FastPFor/blob/master/headers/simple8b_rle.h)
The delta value that is greater than (1 << 60) - 1 can't be stored (error will occured when encoding)

#### example
Say, there are 3 timestamps as below.

##### timestamps
-  1627467076985012256 (head timestamp)
-  1627467250785267037
-  1627468063600895795
-  1627468158257010309

##### timestamps (round by seconds)
-  162746707 (head timestamp)
-  162746725
-  162746806
-  162746815

##### deltas
-  18
-  81
-  9

### (6) common trailing zero num of timestamp nano (8 bits)

common trailing zero num that all values in `(7) timestamp (< sec as nano sec )` have.

#### example
##### nano seconds that put to (7) fields

-  6985012000
-  785267000
-  3600895000
-  8257010000


##### to binary

- 110100000010101101101001100100000
- 101110110011100011100100111000
- 11010110101000010100110000011000
- 111101100001001111111100101010000

every value has at least 3 trailing-zeros,so `3` will be stored .


#### the field size
the max value of nano seconds that put to `(7)` fields is 999999999,
`111011100110101100100111111111` in binary.so `29` is the max value that will be this field. 5 bits in the 8 bits is sufficient but we're remaining heading 3 bits for future extension.

### (7) timestamp (sub nano sec since latest second)

nano second value without its seconds value and common trailing zeros that complements to `(5) Timestamps deltas(second)`.
the values will be encoded by simple8b-rle

#### example

##### timestamps
-  1627467076_985012000
-  1627467250_785267000
-  1627468063_600895000
-  1627468158_257010000

##### to nano seconds

-  985012000  (remove 1627467076 at the head)
-  785267000  (remove 1627467250 at the head)
-  600895000  (remove 1627468063 at the head)
-  257010000  (remove 1627468158 at the head)

##### to binary

- 111010101101100001011100100000
- 101110110011100011100100111000
- 100011110100001110111000011000
- 1111010100011010100101010000


##### drop common trailing zeros
every value has at least 3 trailing-zeros.drop these zeros.

- 111010101101100001011100100
- 101110110011100011100100111
- 100011110100001110111000011
- 1111010100011010100101010

( then `3` will be stored to `(6) common trailing zero num of timestamp nano`)

### (8) Datas of field

#### Compressing methods

- integer ... (not implemented yet) if the value is less than (1 << 60) - 1 ,convert with [ZigZag Encoding](https://developers.google.com/protocol-buffers/docs/encoding) then compress with  [simple8b-rle](https://github.com/lemire/FastPFor/blob/master/headers/simple8b_rle.h).We haven't decide how to handle with the outlier values(we are considering uncompress all values if the datas contains at least one value that exceed the maximum value, but it seems very unefficient...)
- float   ... Facebook Gorilla of XOR encoding (http://www.vldb.org/pvldb/vol8/p1816-teller.pdf)
- string      ...  (not implemented yet) consider to compress with [snappy](https://github.com/google/snappy)
- timestamp   ...  (not implemented yet) delta encoding and [simple8b-rle](https://github.com/lemire/FastPFor/blob/master/headers/simple8b_rle.h)
- bool        ...  (not implemented yet) each boolean express by 1 bit.



### WAL (WIP)
### format (WIP)

```
┌────────────────────────┬─────────────────────────────────────────┬────────────────────────────┬──────────────────────────┬─────────────────────────┬──────┐
│ (1)timestamp (8 bytes) │ (2)number of data fields num (1 byte)   │ (3)type of field_1(4 bit)  │ (4)field value (v bytes) │  type of field_2(4 bit) │ ...  │
└────────────────────────┴─────────────────────────────────────────┴────────────────────────────┴──────────────────────────┴─────────────────────────┴──────┘

```

### references
[nakabonne's article](https://zenn.dev/nakabonne/articles/d300838a1500c7)
