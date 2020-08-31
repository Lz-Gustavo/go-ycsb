package localkv

import (
	"context"
	"encoding/binary"
	"fmt"
	"log"
	"os"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/Lz-Gustavo/beelog"
	"github.com/Lz-Gustavo/beelog/pb"
	"github.com/golang/protobuf/proto"
	"github.com/magiconair/properties"
	"github.com/pingcap/go-ycsb/pkg/prop"
	"github.com/pingcap/go-ycsb/pkg/ycsb"
)

const (
	localkvOutputDir = "localkv.output"

	// if none passed, beelog is used instead.
	localkvLogDir         = "localkv.logfolder"
	localkvBeelogInterval = "localkv.interval"
)

type localKV struct {
	outFile *os.File
	props   *properties.Properties
	cancel  context.CancelFunc

	trad    bool
	logFile *os.File
	ct      *beelog.ConcTable

	index uint64 // atomic
	count uint32 // atomic
	t     *time.Ticker
}

// Read reads a record from the database and returns a map of each field/value pair.
func (lk *localKV) Read(ctx context.Context, table string, key string, fields []string) (map[string][]byte, error) {
	cmd := &pb.Command{
		Op:  pb.Command_GET,
		Key: key,
	}
	if err := lk.logCommand(cmd); err != nil {
		return nil, err
	}

	return map[string][]byte{
		key: nil,
	}, nil
}

// Insert inserts a record in the database. Any field/value pairs will be written into the
// database.
func (lk *localKV) Insert(ctx context.Context, table string, key string, values map[string][]byte) error {
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
	return lk.logCommand(cmd)
}

// Update updates a record in the database. Any field/value pairs will be written into the
// database or overwritten the existing values with the same field name.
func (lk *localKV) Update(ctx context.Context, table string, key string, values map[string][]byte) error {
	// Threating as the same procedure for now. Updates and Inserts are never present on the
	// same workload, so its ok.
	return lk.Insert(ctx, table, key, values)
}

// InitThread initializes the state associated to the goroutine worker.
// The Returned context will be passed to the following usage.
func (lk *localKV) InitThread(ctx context.Context, threadID int, threadCount int) context.Context {
	return ctx
}

// Close closes the database layer.
func (lk *localKV) Close() error {
	lk.cancel()
	if lk.trad {
		lk.logFile.Close()
	}
	return lk.outFile.Close()
}

// CleanupThread cleans up the state when the worker finished.
func (lk *localKV) CleanupThread(ctx context.Context) {
	// TODO: call bk.clients[id].Disconnect maybe?
}

// Scan scans records from the database.
func (lk *localKV) Scan(ctx context.Context, table string, startKey string, count int, fields []string) ([]map[string][]byte, error) {
	return nil, nil
}

// Delete deletes a record from the database.
func (lk *localKV) Delete(ctx context.Context, table string, key string) error {
	return nil
}

// log command on a std file, emulating traditional approach, or utilize beelog
func (lk *localKV) logCommand(cmd *pb.Command) error {
	// must set any command index
	cmd.Id = atomic.AddUint64(&lk.index, 1)

	if lk.trad {
		rawCmd, err := proto.Marshal(cmd)
		if err != nil {
			return err
		}

		err = binary.Write(lk.logFile, binary.BigEndian, int32(len(rawCmd)))
		if err != nil {
			return err
		}

		_, err = lk.logFile.Write(rawCmd)
		if err != nil {
			return err
		}

	} else {
		if err := lk.ct.Log(*cmd); err != nil {
			return err
		}
	}
	atomic.AddUint32(&lk.count, 1)
	return nil
}

func (lk *localKV) monitorThroughput(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return nil

		case <-lk.t.C:
			t := atomic.SwapUint32(&lk.count, 0)
			_, err := fmt.Fprintf(lk.outFile, "%d\n", t)
			if err != nil {
				return err
			}
		}
	}
}

type localKVCreator struct {
}

func (lc localKVCreator) Create(p *properties.Properties) (ycsb.DB, error) {
	var fd *os.File
	var err error

	ths := p.GetInt(prop.ThreadCount, -1)
	if ths < 0 {
		log.Fatalln("could not interpret number of threads from properties")
	}

	outDir, ok := p.Get(localkvOutputDir)
	if ok {
		outFn := outDir + strconv.Itoa(ths) + "w-throughput.out"
		fd, err = os.OpenFile(outFn, os.O_CREATE|os.O_TRUNC|os.O_WRONLY|os.O_APPEND, 0600)
		if err != nil {
			return nil, err
		}
	}

	ctx, cn := context.WithCancel(context.Background())
	lk := &localKV{
		outFile: fd,
		props:   p,
		cancel:  cn,
		t:       time.NewTicker(time.Second),
	}

	logD := p.GetString(localkvLogDir, "")
	if ths < 0 {
		log.Fatalln("could not interpret number of threads from properties")
	}

	if logD != "" {
		fn := logD + "logfile.log"
		fd, err = os.OpenFile(fn, os.O_CREATE|os.O_TRUNC|os.O_WRONLY|os.O_APPEND, 0600)
		if err != nil {
			return nil, err
		}
		lk.trad = true
		lk.logFile = fd

	} else {
		pd := p.GetInt(localkvBeelogInterval, -1)
		if pd < 0 {
			log.Fatalln("could not interpret beelog interval from properties")
		}

		cfg := &beelog.LogConfig{
			Alg:     beelog.IterConcTable,
			Tick:    beelog.Interval,
			Period:  uint32(pd),
			KeepAll: true,
			Fname:   "/tmp/beelog-local.log",
		}
		lk.ct, err = beelog.NewConcTableWithConfig(ctx, cfg)
		if err != nil {
			return nil, err
		}
	}

	go lk.monitorThroughput(ctx)
	return lk, nil
}

func init() {
	ycsb.RegisterDBCreator("localkv", localKVCreator{})
}
