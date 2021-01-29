package etcd

import (
	"context"
	"fmt"
	"log"
	"math"
	"math/rand"
	"os"
	"time"

	"github.com/magiconair/properties"
	"github.com/pingcap/go-ycsb/pkg/prop"
	"github.com/pingcap/go-ycsb/pkg/ycsb"
	"go.etcd.io/etcd/clientv3"
)

const (
	// TODO: parse etcd ip from commandline args

	// An empty value indicates none latency output.
	etcdLatencyFilename = "etcd.latfilename"

	// One client has a '1/measureChance' chance to capture latency of it's next requisition.
	measureChance = 30

	// The ceil of 'clients/watcherRatio' indicates the number of clients recording latency
	// based on 'measureChance'. A ratio greater then the number of clients indicates that all
	// clients will be recording latency.
	watcherRatio = 3

	// Sleeps up to thinkTime msec after each request.
	thinkTime = 10
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
	cl    []Client
	maxC  int
	props *properties.Properties

	lat     bool
	latFile *os.File
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
	if ed.lat && id < ed.maxC && checkLat() {
		st := time.Now()
		rep, err = ed.cl[id].cl.Get(ctx, key)
		if err != nil {
			return nil, err
		}

		err = ed.recordLat(time.Since(st) / time.Nanosecond)
		if err != nil {
			return nil, err
		}

	} else {
		rep, err = ed.cl[id].cl.Get(ctx, key)
		if err != nil {
			return nil, err
		}
	}

	if thinkTime > 0 {
		time.Sleep(time.Duration(rand.Intn(thinkTime+1)) * time.Millisecond)
	}
	return map[string][]byte{
		key: rep.Kvs[0].Value,
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
	if ed.lat && id < ed.maxC && checkLat() {
		st := time.Now()
		_, err := ed.cl[id].cl.Put(ctx, key, string(val))
		if err != nil {
			return err
		}

		err = ed.recordLat(time.Since(st) / time.Nanosecond)
		if err != nil {
			return err
		}

	} else {
		_, err := ed.cl[id].cl.Put(ctx, key, string(val))
		if err != nil {
			return err
		}
	}

	if thinkTime > 0 {
		time.Sleep(time.Duration(rand.Intn(thinkTime+1)) * time.Millisecond)
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
	cl, err := NewClient(ctx)
	if err != nil {
		log.Fatalln("could not init thread, err:", err.Error())
	}

	ed.cl[threadID] = *cl
	return context.WithValue(ctx, ctxThreadID, threadID)
}

// Close closes the database layer.
func (ed *etcdDB) Close() error {
	for _, cl := range ed.cl {
		cl.shutdown()
	}
	if ed.lat {
		return ed.latFile.Close()
	}
	return nil
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

func (ed *etcdDB) recordLat(dur time.Duration) error {
	_, err := fmt.Fprintf(ed.latFile, "%d\n", dur)
	return err
}

type etcdDBCreator struct {
}

func (ec etcdDBCreator) Create(p *properties.Properties) (ycsb.DB, error) {
	var fd *os.File
	var err error

	ths := p.GetInt(prop.ThreadCount, -1)
	if ths < 0 {
		log.Fatalln("could not interpret number of threads from properties")
	}

	fn, ok := p.Get(etcdLatencyFilename)
	if ok {
		fd, err = os.OpenFile(fn, os.O_CREATE|os.O_TRUNC|os.O_WRONLY|os.O_APPEND, 0600)
		if err != nil {
			return nil, err
		}
	}

	return &etcdDB{
		cl:      make([]Client, ths, ths),
		maxC:    int(math.Ceil(float64(ths) / watcherRatio)),
		props:   p,
		lat:     ok,
		latFile: fd,
	}, nil
}

func init() {
	ycsb.RegisterDBCreator("etcd", etcdDBCreator{})
}

func checkLat() bool {
	return rand.Intn(measureChance) == 0
}
