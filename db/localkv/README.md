# go-ycsb/localkv
TODO

## Usage
TODO

```
make
./bin/go-ycsb run localkv -P workloads/workloadb -p localkv.config=/home/lzgustavo/go/src/go-ycsb/db/localkv/client-config.toml
```

Enable output flag:
```
-p localkv.output=/home/lzgustavo/go/src/go-ycsb/db/localkv/latency.out
```

Multiple threads, diff records, num operations, target ops/sec:
```
-p threadcount=1 -p recordcount=1000000 -p operationcount=600000 -p target=10000
```

**NOTE:** call ```operationscount=threadcount*600000```

**TODO:** account threadcount on target to avoid early saturation