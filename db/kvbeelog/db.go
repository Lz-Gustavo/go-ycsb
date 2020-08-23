package kvbeelog

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"os"
	"time"

	"github.com/Lz-Gustavo/beelog/pb"
	"github.com/magiconair/properties"
	"github.com/pingcap/go-ycsb/pkg/ycsb"
)

const (
	defaultConfigFn  = "client-config.toml"
	kvbeelogConfigFn = "kvbeelog.config"

	// An empty value indicates none latency output.
	kvbeelogOutputFn = "kvbeelog.output"

	// One client has a '1/measureChance' chance to capture latency of it's next requisition.
	measureChance int = 30
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

// beelogKV
type beelogKV struct {
	clients []Info
	out     bool
	outFile *os.File
	prop    *properties.Properties
}

// Read reads a record from the database and returns a map of each field/value pair.
func (bk *beelogKV) Read(ctx context.Context, table string, key string, fields []string) (map[string][]byte, error) {
	id, ok := getContextThreadID(ctx)
	if !ok {
		return nil, fmt.Errorf("could not load threadid from context")
	}

	cmd := &pb.Command{
		Op:  pb.Command_GET,
		Key: key,
	}
	err := bk.sendProtoBuff(cmd, id)
	if err != nil {
		return nil, err
	}

	rep, err := bk.clients[id].ReadUDP()
	if err != nil {
		return nil, err
	}
	return map[string][]byte{
		key: []byte(rep),
	}, nil
}

// Insert inserts a record in the database. Any field/value pairs will be written into the
// database.
func (bk *beelogKV) Insert(ctx context.Context, table string, key string, values map[string][]byte) error {
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

	cmd := &pb.Command{
		Op:    pb.Command_SET,
		Key:   key,
		Value: string(val),
	}
	err := bk.sendProtoBuff(cmd, id)
	if err != nil {
		return err
	}

	if _, err = bk.clients[id].ReadUDP(); err != nil {
		return err
	}
	return nil
}

// Update updates a record in the database. Any field/value pairs will be written into the
// database or overwritten the existing values with the same field name.
func (bk *beelogKV) Update(ctx context.Context, table string, key string, values map[string][]byte) error {
	// Threating as the same procedure for now. Updates and Inserts are never present on the
	// same workload, so its ok.
	return bk.Insert(ctx, table, key, values)
}

// InitThread initializes the state associated to the goroutine worker.
// The Returned context will be passed to the following usage.
//
// Initializes a new client on bk.clients, returns threadID in context to be used by
// operation methods. Safe workflow since threadIDs ARE monotonically increased.
func (bk *beelogKV) InitThread(ctx context.Context, threadID int, threadCount int) context.Context {
	fn, ok := bk.prop.Get(kvbeelogConfigFn)
	if !ok {
		fn = defaultConfigFn
	}

	cl, err := New(fn)
	if err != nil {
		log.Fatalln("could not init thread, err:", err.Error())
	}

	if err = cl.Connect(); err != nil {
		log.Fatalln("could not init thread, err:", err.Error())
	}
	if err = cl.StartUDP(threadID); err != nil {
		log.Fatalln("could not init thread, err:", err.Error())
	}

	bk.clients = append(bk.clients, *cl)
	return context.WithValue(ctx, ctxThreadID, threadID)
}

// Close closes the database layer.
func (bk *beelogKV) Close() error {
	for _, cl := range bk.clients {
		cl.Disconnect()
	}
	if bk.out {
		return bk.outFile.Close()
	}
	return nil
}

// CleanupThread cleans up the state when the worker finished.
func (bk *beelogKV) CleanupThread(ctx context.Context) {
	// TODO: call bk.clients[id].Disconnect maybe?
}

// Scan scans records from the database.
func (bk *beelogKV) Scan(ctx context.Context, table string, startKey string, count int, fields []string) ([]map[string][]byte, error) {
	return nil, nil
}

// Delete deletes a record from the database.
func (bk *beelogKV) Delete(ctx context.Context, table string, key string) error {
	return nil
}

func (bk *beelogKV) sendProtoBuff(cmd *pb.Command, id int) error {
	if bk.out && checkLat() {
		st := time.Now()
		if err := bk.clients[id].BroadcastProtobuf(cmd, bk.clients[id].Udpport); err != nil {
			return err
		}
		return bk.recordLat(time.Since(st))
	}
	return bk.clients[id].BroadcastProtobuf(cmd, bk.clients[id].Udpport)
}

func (bk *beelogKV) recordLat(dur time.Duration) error {
	_, err := fmt.Fprintf(bk.outFile, "%d\n", dur)
	return err
}

type beelogKVCreator struct {
}

func (bc beelogKVCreator) Create(p *properties.Properties) (ycsb.DB, error) {
	var fd *os.File
	var err error

	outFn, ok := p.Get(kvbeelogOutputFn)
	if ok {
		fd, err = os.OpenFile(outFn, os.O_CREATE|os.O_TRUNC|os.O_WRONLY|os.O_APPEND, 0400)
		if err != nil {
			return nil, err
		}
	}

	return &beelogKV{
		clients: make([]Info, 0, 0),
		out:     ok,
		outFile: fd,
		prop:    p,
	}, nil
}

func init() {
	ycsb.RegisterDBCreator("kvbeelog", beelogKVCreator{})
}

func checkLat() bool {
	return rand.Intn(measureChance) == 0
}
