# go-ycsb/kvbeelog
TODO

## Usage
TODO

```
make
./bin/go-ycsb run kvbeelog -P workloads/workloadb -p kvbeelog.config=/home/lzgustavo/go/src/go-ycsb/db/kvbeelog/client-config.toml
```

Enable output flag:
```
-p kvbeelog.output=/home/lzgustavo/go/src/go-ycsb/db/kvbeelog/latency.out
```

Multiple threads, diff records, num operations, target ops/sec:
```
-p threadcount=1 -p recordcount=1000000 -p operationcount=600000 -p target=10000
```

**NOTE:** call ```operationscount=threadcount*600000```

**TODO:** account threadcount on target to avoid early saturation