# go-ycsb/localkv
TODO

## Usage
TODO

```
make
./bin/go-ycsb run localkv -P workloads/workloadb -p localkv.output=/home/lzgustavo/go/src/go-ycsb/db/localkv/ -p localkv.interval=10000 -p threadcount=1 -p recordcount=1000000 -p operationcount=600000
```

Enable traditional log scheme:
```
-p localkv.logfolder=/tmp/
```
