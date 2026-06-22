#!/bin/bash

ycsbPath=~/go/src/github.com/Lz-Gustavo/go-ycsb/

etcdHostname="127.0.0.1:2370"
etcdLatFilename=/tmp/client-latency.out
etcdStatusFilename=/tmp/client-status-codes.out

workloads=("workloada")
numDiffKeys=1000000 # 1kk
iterations=1

main() {
    echo "running..."
    for (( i = 0; i < ${iterations}; i++ )); do
        increaseByTargetThroughput $i
    done
    echo "finished!"
}

increaseByTargetThroughput() {
    local i=$1
    echo "#${i}: started target thr iteration"

    numThreads=10

    # same size arrays
    numOps=(100)
    targetThrs=(10)

    for workload in ${workloads[*]}; do
        for (( j = 0; j < ${#targetThrs[*]}; j++ )); do
            t=${targetThrs[$j]}
            n=${numOps[$j]}

            echo "#${i}-${workload}/${j}: executing $t target throughput"
            ${ycsbPath}/bin/go-ycsb run etcd -P ${ycsbPath}/workloads/${workload} -p target=${t} -p threadcount=${numThreads} -p recordcount=${numDiffKeys} -p operationcount=${n} -p etcd.endpoints=${etcdHostname} \
                -p etcd.latfilename=${etcdLatFilename} -p etcd.statusfilename=${etcdStatusFilename}
        done
    done
    echo "#${i}: finished target thr iteration"; echo ""
}

main "$@"; exit
