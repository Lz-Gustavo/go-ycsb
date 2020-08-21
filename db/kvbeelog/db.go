package kvbeelog

import (
	"context"
	"fmt"
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

// beelogKV
type beelogKV struct {
	client  Info
	out     bool
	outFile *os.File
}

// Read reads a record from the database and returns a map of each field/value pair.
func (bk *beelogKV) Read(ctx context.Context, table string, key string, fields []string) (map[string][]byte, error) {
	cmd := &pb.Command{
		Op:  pb.Command_GET,
		Key: key,
	}
	err := bk.sendProtoBuff(cmd)
	if err != nil {
		return nil, err
	}

	rep, err := bk.client.ReadUDP()
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
	err := bk.sendProtoBuff(cmd)
	if err != nil {
		return err
	}

	if _, err = bk.client.ReadUDP(); err != nil {
		return err
	}
	return nil
}

// Update updates a record in the database. Any field/value pairs will be written into the
// database or overwritten the existing values with the same field name.
func (bk *beelogKV) Update(ctx context.Context, table string, key string, values map[string][]byte) error {
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
	err := bk.sendProtoBuff(cmd)
	if err != nil {
		return err
	}

	if _, err = bk.client.ReadUDP(); err != nil {
		return err
	}
	return nil
}

// InitThread initializes the state associated to the goroutine worker.
// The Returned context will be passed to the following usage.
func (bk *beelogKV) InitThread(ctx context.Context, threadID int, threadCount int) context.Context {
	return ctx
}

// Close closes the database layer.
func (bk *beelogKV) Close() error {
	bk.client.Disconnect()
	if bk.out {
		return bk.outFile.Close()
	}
	return nil
}

// CleanupThread cleans up the state when the worker finished.
func (bk *beelogKV) CleanupThread(ctx context.Context) {}

// Scan scans records from the database.
func (bk *beelogKV) Scan(ctx context.Context, table string, startKey string, count int, fields []string) ([]map[string][]byte, error) {
	return nil, nil
}

// Delete deletes a record from the database.
func (bk *beelogKV) Delete(ctx context.Context, table string, key string) error {
	return nil
}

func (bk *beelogKV) sendProtoBuff(cmd *pb.Command) error {
	if bk.out && checkLat() {
		st := time.Now()
		if err := bk.client.BroadcastProtobuf(cmd, bk.client.Udpport); err != nil {
			return err
		}
		return bk.recordLat(time.Since(st))
	}
	return bk.client.BroadcastProtobuf(cmd, bk.client.Udpport)
}

func (bk *beelogKV) recordLat(dur time.Duration) error {
	_, err := fmt.Fprintf(bk.outFile, "%d\n", dur)
	return err
}

type beelogKVCreator struct {
}

func (bc beelogKVCreator) Create(p *properties.Properties) (ycsb.DB, error) {
	fn, ok := p.Get(kvbeelogConfigFn)
	if !ok {
		fn = defaultConfigFn
	}

	cl, err := New(fn)
	if err != nil {
		return nil, err
	}

	if err = cl.Connect(); err != nil {
		return nil, err
	}
	if err = cl.StartUDP(); err != nil {
		return nil, err
	}

	var fd *os.File
	outFn, ok := p.Get(kvbeelogOutputFn)
	if ok {
		fd, err = os.OpenFile(outFn, os.O_CREATE|os.O_TRUNC|os.O_WRONLY|os.O_APPEND, 0400)
		if err != nil {
			return nil, err
		}
	}

	return &beelogKV{
		client:  *cl,
		out:     ok,
		outFile: fd,
	}, nil
}

func init() {
	ycsb.RegisterDBCreator("kvbeelog", beelogKVCreator{})
}

func checkLat() bool {
	return rand.Intn(measureChance) == 0
}
