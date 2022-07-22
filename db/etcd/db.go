package etcd

import (
	"context"
	"fmt"
	"math"
	"math/rand"
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

	var rep *clientv3.GetResponse
	var err error

	// if measuring latency for this request
	if ed.lat && (recordAll || ed.mustCheckLat(id)) {
		st := time.Now()
		rep, err = ed.cl[id].cl.Get(ctx, key)
		if err != nil {
			return nil, err
		}

		err = ed.msr.Record(time.Since(st))
		if err != nil {
			return nil, err
		}

	} else {
		rep, err = ed.cl[id].cl.Get(ctx, key)
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

	// get a single value from values map
	var val []byte
	for k := range values {
		val = values[k]
		break
	}

	// if measuring latency for this request
	if ed.lat && (recordAll || ed.mustCheckLat(id)) {
		st := time.Now()
		_, err := ed.cl[id].cl.Put(ctx, key, string(val))
		if err != nil {
			return err
		}

		err = ed.msr.Record(time.Since(st))
		if err != nil {
			return err
		}

	} else {
		_, err := ed.cl[id].cl.Put(ctx, key, string(val))
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
	// Threating as the same procedure for now. Updates and Inserts are never present on the
	// same workload, so its ok.
	return ed.Insert(ctx, table, key, values)
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
