module github.com/pingcap/go-ycsb

require (
	cloud.google.com/go/spanner v1.1.0
	github.com/AndreasBriese/bbloom v0.0.0-20180913140656-343706a395b7 // indirect
	github.com/BurntSushi/toml v0.3.1
	github.com/Lz-Gustavo/beelog v0.0.0-20200806172403-b4e603daa4cf
	github.com/XiaoMi/pegasus-go-client v0.0.0-20181029071519-9400942c5d1c
	github.com/aerospike/aerospike-client-go v1.35.2
	github.com/apache/thrift v0.0.0-20171203172758-327ebb6c2b6d // indirect
	github.com/apple/foundationdb/bindings/go v0.0.0-20200112054404-407dc0907f4f
	github.com/bitly/go-hostpool v0.0.0-20171023180738-a3a6125de932 // indirect
	github.com/bmizerany/assert v0.0.0-20160611221934-b7ed37b82869 // indirect
	github.com/boltdb/bolt v1.3.1
	github.com/chzyer/logex v1.1.10 // indirect
	github.com/chzyer/readline v0.0.0-20180603132655-2972be24d48e
	github.com/chzyer/test v0.0.0-20180213035817-a1ea475d72b1 // indirect
	github.com/dgraph-io/badger v1.5.4
	github.com/dgryski/go-farm v0.0.0-20180109070241-2de33835d102 // indirect
	github.com/facebookgo/ensure v0.0.0-20160127193407-b4ab57deab51 // indirect
	github.com/facebookgo/stack v0.0.0-20160209184415-751773369052 // indirect
	github.com/facebookgo/subset v0.0.0-20150612182917-8dac2c3c4870 // indirect
	github.com/fortytw2/leaktest v1.3.0 // indirect
	github.com/go-ini/ini v1.49.0 // indirect
	github.com/go-redis/redis v6.15.1+incompatible
	github.com/go-sql-driver/mysql v1.4.1
	github.com/gocql/gocql v0.0.0-20181124151448-70385f88b28b
	github.com/golang/groupcache v0.0.0-20191027212112-611e8accdfc9 // indirect
	github.com/golang/protobuf v1.4.2
	github.com/json-iterator/go v1.1.10 // indirect
	github.com/jstemmer/go-junit-report v0.9.1 // indirect
	github.com/lib/pq v0.0.0-20181016162627-9eb73efc1fcc
	github.com/magiconair/properties v1.8.0
	github.com/mattn/go-sqlite3 v1.10.0
	github.com/minio/minio-go v6.0.14+incompatible
	github.com/mitchellh/go-homedir v1.1.0 // indirect
	github.com/onsi/ginkgo v1.14.2 // indirect
	github.com/onsi/gomega v1.10.4 // indirect
	github.com/pingcap/errors v0.11.1
	github.com/smartystreets/goconvey v0.0.0-20190330032615-68dc04aab96a // indirect
	github.com/spf13/cobra v0.0.3
	github.com/spf13/pflag v1.0.3 // indirect
	github.com/tecbot/gorocksdb v0.0.0-20191217155057-f0fad39f321c
	github.com/tidwall/pretty v1.0.0 // indirect
	github.com/xdg/scram v0.0.0-20180814205039-7eeb5667e42c // indirect
	github.com/xdg/stringprep v1.0.0 // indirect
	github.com/yuin/gopher-lua v0.0.0-20181031023651-12c4817b42c5 // indirect
	go.etcd.io/etcd v0.5.0-alpha.5.0.20210123184945-d51c6c689ba3
	go.mongodb.org/mongo-driver v1.0.2
	go.opencensus.io v0.22.2 // indirect
	golang.org/x/exp v0.0.0-20191129062945-2f5052295587 // indirect
	golang.org/x/lint v0.0.0-20191125180803-fdd1cda4f05f // indirect
	golang.org/x/oauth2 v0.0.0-20191202225959-858c2ad4c8b6 // indirect
	golang.org/x/sync v0.0.0-20190911185100-cd5d95a43a6e // indirect
	golang.org/x/tools v0.0.0-20191210221141-98df12377212 // indirect
	google.golang.org/api v0.14.0
	google.golang.org/appengine v1.6.5 // indirect
	google.golang.org/genproto v0.0.0-20200526211855-cb27e3aa2013
	google.golang.org/protobuf v1.25.0 // indirect
	gopkg.in/check.v1 v1.0.0-20190902080502-41f04d3bba15 // indirect
	gopkg.in/ini.v1 v1.42.0 // indirect
	gopkg.in/tomb.v2 v2.0.0-20161208151619-d5d1b5820637 // indirect
	sigs.k8s.io/yaml v1.2.0 // indirect
)

replace github.com/apache/thrift => github.com/apache/thrift v0.0.0-20171203172758-327ebb6c2b6d

replace github.com/coreos/bbolt => go.etcd.io/bbolt v1.3.5

replace google.golang.org/grpc v1.30.0 => google.golang.org/grpc v1.26.0

go 1.15
