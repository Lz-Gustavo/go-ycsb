package loadgenkv

import (
	"context"
	"log"
	"os"
	"sync/atomic"

	"github.com/Lz-Gustavo/beelog"
	"github.com/Lz-Gustavo/beelog/pb"
	"github.com/magiconair/properties"
	"github.com/pingcap/go-ycsb/pkg/prop"
	"github.com/pingcap/go-ycsb/pkg/ycsb"
)

const (
	loadgenOutputFn = "loadgenkv.outfilename"
	byteBlocks      = 1
)

type loadgenKV struct {
	props   *properties.Properties
	cmds    []pb.Command
	outFile *os.File
	index   uint64 // atomic
}

// Read reads a record from the database and returns a map of each field/value pair.
func (lk *loadgenKV) Read(ctx context.Context, table string, key string, fields []string) (map[string][]byte, error) {
	cmd := pb.Command{
		Op:  pb.Command_GET,
		Id:  atomic.AddUint64(&lk.index, 1),
		Key: key,
	}
	lk.cmds = append(lk.cmds, cmd)

	return map[string][]byte{
		key: nil,
	}, nil
}

// Insert inserts a record in the database. Any field/value pairs will be written into the
// database.
func (lk *loadgenKV) Insert(ctx context.Context, table string, key string, values map[string][]byte) error {
	var val []byte
	count := 0
	for k := range values {
		val = append(val, values[k]...)
		count++

		// count blocks of 100B value
		if count > byteBlocks {
			break
		}
	}

	cmd := pb.Command{
		Op:    pb.Command_SET,
		Id:    atomic.AddUint64(&lk.index, 1),
		Key:   key,
		Value: string(val),
	}
	lk.cmds = append(lk.cmds, cmd)
	return nil
}

// Update updates a record in the database. Any field/value pairs will be written into the
// database or overwritten the existing values with the same field name.
func (lk *loadgenKV) Update(ctx context.Context, table string, key string, values map[string][]byte) error {
	// Threating as the same procedure for now. Updates and Inserts are never present on the
	// same workload, so its ok.
	return lk.Insert(ctx, table, key, values)
}

// InitThread initializes the state associated to the goroutine worker.
// The Returned context will be passed to the following usage.
func (lk *loadgenKV) InitThread(ctx context.Context, threadID int, threadCount int) context.Context {
	return ctx
}

// Close closes the database layer.
func (lk *loadgenKV) Close() error {
	err := lk.flushToFile()
	if err != nil {
		log.Fatalln("failed while creating log file")
	}
	return lk.outFile.Close()
}

// CleanupThread cleans up the state when the worker finished.
func (lk *loadgenKV) CleanupThread(ctx context.Context) {}

// Scan scans records from the database.
func (lk *loadgenKV) Scan(ctx context.Context, table string, startKey string, count int, fields []string) ([]map[string][]byte, error) {
	return nil, nil
}

// Delete deletes a record from the database.
func (lk *loadgenKV) Delete(ctx context.Context, table string, key string) error {
	return nil
}

func (lk *loadgenKV) flushToFile() error {
	return beelog.MarshalLogIntoWriter(lk.outFile, &lk.cmds, 0, 0)
}

type loadgenKVCreator struct{}

func (lc loadgenKVCreator) Create(p *properties.Properties) (ycsb.DB, error) {
	ths := p.GetInt(prop.ThreadCount, -1)
	if ths < 0 {
		log.Fatalln("could not interpret number of threads from properties")
	}

	work := p.GetString(prop.Workload, "")
	if work == "" {
		log.Fatalln("could not identify workload")
	}

	outFn, ok := p.Get(loadgenOutputFn)
	if !ok {
		log.Fatalln("could not identify output location, run with -p loadgen.output=/folder/")
	}

	fd, err := os.OpenFile(outFn, os.O_CREATE|os.O_TRUNC|os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		return nil, err
	}

	return &loadgenKV{
		outFile: fd,
		cmds:    make([]pb.Command, 0),
		props:   p,
	}, nil
}

func init() {
	ycsb.RegisterDBCreator("loadgenkv", loadgenKVCreator{})
}
