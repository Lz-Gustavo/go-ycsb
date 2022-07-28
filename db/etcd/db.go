package etcd

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"math/rand"
	"strconv"
	"strings"
	"time"

	"github.com/magiconair/properties"
	"github.com/pingcap/go-ycsb/pkg/prop"
	"github.com/pingcap/go-ycsb/pkg/ycsb"
	"go.etcd.io/etcd/clientv3"
)

const (
	// Currently supporting a single node, and alway defaults to port ":2379"
	etcdNodeHostname = "etcd.hostname"

	// An empty value indicates none latency output.
	etcdLatencyFilename = "etcd.latfilename"

	// Number of different connections to distributed threads, same implementation as in
	// the benchmark:
	// https://github.com/etcd-io/etcd/blob/main/tools/benchmark/cmd/util.go#L137
	etcdNumberOfConns    = "etcd.conns"
	defaultNumberOfConns = 1

	// One client has a '1/measureChance' chance to capture latency of it's next requisition.
	measureChance = 30

	// The ceil of 'clients/watcherRatio' indicates the number of clients recording latency
	// based on 'measureChance'. A ratio greater then the number of clients indicates that all
	// clients will be recording latency.
	watcherRatio = 3

	// Overwrites all previous configurations and enables latency measurement on all requests,
	// if a 'etcdLatencyFilename' was configured.
	recordAll = true

	// Sleeps up to thinkTime msec after each request. If no etcdThinkingTime property is informed,
	// defaultThinkTime value is assumed. A zero or negative number completely disables it.
	etcdThinkingTime     = "etcd.thinktime"
	defaultThinkTime int = 10
)

type contextKey int

const ctxThreadID contextKey = 0

func getContextThreadID(ctx context.Context) (int, bool) {
	v := ctx.Value(ctxThreadID)
	if v == nil {
		return 0, false
	}
	return v.(int), true
}

type etcdDB struct {
	cl    []*Client
	maxC  int
	props *properties.Properties

	thinkEnabled bool
	thinkTime    int

	lat bool
	msr *LatencyMsr
}

// Read reads a record from the database and returns a map of each field/value pair.
func (ed *etcdDB) Read(ctx context.Context, table string, key string, fields []string) (map[string][]byte, error) {
	id, ok := getContextThreadID(ctx)
	if !ok {
		return nil, fmt.Errorf("could not load threadid from context")
	}

	keyB, err := convertKeyToInt64Binary(key)
	if err != nil {
		return nil, fmt.Errorf("could not convert key, err: %w", err)
	}

	var rep *clientv3.GetResponse

	// if measuring latency for this request
	if ed.lat && (recordAll || ed.mustCheckLat(id)) {
		st := time.Now()
		rep, err = ed.cl[id].cl.Get(ctx, string(keyB), clientv3.WithSerializable())
		if err != nil {
			return nil, err
		}

		err = ed.msr.Record(time.Since(st))
		if err != nil {
			return nil, err
		}

	} else {
		rep, err = ed.cl[id].cl.Get(ctx, string(keyB), clientv3.WithSerializable())
		if err != nil {
			return nil, err
		}
	}

	// if got any value, return
	var val []byte
	if len(rep.Kvs) > 0 {
		val = rep.Kvs[0].Value
	}

	if ed.thinkEnabled {
		time.Sleep(time.Duration(rand.Intn(ed.thinkTime+1)) * time.Millisecond)
	}
	return map[string][]byte{
		key: val,
	}, nil
}

// Insert inserts a record in the database. Any field/value pairs will be written into the
// database.
func (ed *etcdDB) Insert(ctx context.Context, table string, key string, values map[string][]byte) error {
	id, ok := getContextThreadID(ctx)
	if !ok {
		return fmt.Errorf("could not load threadid from context")
	}

	keyB, err := convertKeyToInt64Binary(key)
	if err != nil {
		return fmt.Errorf("could not convert key, err: %w", err)
	}

	// get a single value from values map
	var val []byte
	for k := range values {
		val = values[k]
		break
	}

	// if measuring latency for this request
	if ed.lat && (recordAll || ed.mustCheckLat(id)) {
		st := time.Now()
		_, err := ed.cl[id].cl.Put(ctx, string(keyB), string(val))
		if err != nil {
			return err
		}

		err = ed.msr.Record(time.Since(st))
		if err != nil {
			return err
		}

	} else {
		_, err := ed.cl[id].cl.Put(ctx, string(keyB), string(val))
		if err != nil {
			return err
		}
	}

	if ed.thinkEnabled {
		time.Sleep(time.Duration(rand.Intn(ed.thinkTime+1)) * time.Millisecond)
	}
	return nil
}

// Update updates a record in the database. Any field/value pairs will be written into the
// database or overwritten the existing values with the same field name.
func (ed *etcdDB) Update(ctx context.Context, table string, key string, values map[string][]byte) error {
	// Threating as the same procedure. Updates and Inserts are never present on the same
	// workload, so its ok.
	return ed.Insert(ctx, table, key, values)
}

// Same procedure as etcd's benchmark: https://github.com/etcd-io/etcd/blob/main/tools/benchmark/cmd/put.go#L110
//  1. removes "user" preffix from "user9083170225680086660"
//  2. convert "9083170225680086660" string to int64
//  3. binary encode 9083170225680086660
//
// IMPORTANT: using binary.Size() doesnt work to calculate the size of the bytes
// buffer, since sometimes it would lead to a panic from binary.PutVarint(). Instead,
// i've set to a fixed size 10B, due to this doc:
//
//   'At most 10 bytes are needed for 64-bit values. The encoding could
//   be more dense: a full 64-bit value needs an extra byte just to hold bit 63.
//   Instead, the msb of the previous byte could be used to hold bit 63 since we
//   know there can't be more than 64 bits. This is a trivial improvement and
//   would reduce the maximum encoding length to 9 bytes. However, it breaks the
//   invariant that the msb is always the "continuation bit" and thus makes the
//   format incompatible with a varint encoding for larger numbers (say 128-bit).'
func convertKeyToInt64Binary(key string) ([]byte, error) {
	key = strings.TrimPrefix(key, "user")
	num, err := strconv.ParseInt(key, 10, 64)
	if err != nil {
		return nil, err
	}

	k := make([]byte, 10)
	binary.PutVarint(k, num)
	return k, nil
}

// InitThread initializes the state associated to the goroutine worker.
// The Returned context will be passed to the following usage.
//
// Initializes a new client on ed.clients, returns threadID in context to be used by
// operation methods. Safe workflow since threadIDs ARE monotonically increased.
func (ed *etcdDB) InitThread(ctx context.Context, threadID int, threadCount int) context.Context {
	return context.WithValue(ctx, ctxThreadID, threadID)
}

// Close closes the database layer.
func (ed *etcdDB) Close() error {
	for _, cl := range ed.cl {
		cl.shutdown()
	}
	if !ed.lat {
		return nil
	}

	if err := ed.msr.Flush(); err != nil {
		return err
	}
	return ed.msr.Close()
}

// CleanupThread cleans up the state when the worker finished.
func (ed *etcdDB) CleanupThread(ctx context.Context) {
	// TODO: call ed.clients[id].Disconnect maybe?
}

// Scan scans records from the database.
func (ed *etcdDB) Scan(ctx context.Context, table string, startKey string, count int, fields []string) ([]map[string][]byte, error) {
	return nil, nil
}

// Delete deletes a record from the database.
func (ed *etcdDB) Delete(ctx context.Context, table string, key string) error {
	return nil
}

func (ed *etcdDB) mustCheckLat(id int) bool {
	return id < ed.maxC && rand.Intn(measureChance) == 0
}

type etcdDBCreator struct {
}

func (ec etcdDBCreator) Create(p *properties.Properties) (ycsb.DB, error) {
	numClients := p.GetInt(prop.ThreadCount, -1)
	if numClients < 0 {
		return nil, fmt.Errorf("could not interpret number of threads from properties")
	}

	var exists bool
	parsedEtcdHostname, exists = p.Get(etcdNodeHostname)
	if !exists {
		parsedEtcdHostname = defaultEtcdIP
	}

	numConns := p.GetInt(etcdNumberOfConns, defaultNumberOfConns)
	clients, err := createClients(numClients, numConns)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize clients, err: %w", err)
	}

	var latMsr *LatencyMsr
	fn, informedLat := p.Get(etcdLatencyFilename)
	if informedLat {
		latMsr, err = NewLatencyMsr(fn)
		if err != nil {
			return nil, err
		}
	}

	thinkTime := p.GetInt(etcdThinkingTime, defaultThinkTime)
	return &etcdDB{
		cl:           clients,
		maxC:         int(math.Ceil(float64(numClients) / watcherRatio)),
		props:        p,
		thinkEnabled: thinkTime > 0,
		thinkTime:    thinkTime,
		lat:          informedLat,
		msr:          latMsr,
	}, nil
}

func createClients(totalClients, totalConns int) ([]*Client, error) {
	conns := make([]*Client, totalConns)
	for i := range conns {
		cl, err := NewClient(context.Background())
		if err != nil {
			return nil, err
		}
		conns[i] = cl
	}

	clients := make([]*Client, totalClients)
	for i := range clients {
		clients[i] = conns[i%totalConns]
	}
	return clients, nil
}

func init() {
	ycsb.RegisterDBCreator("etcd", etcdDBCreator{})
}
