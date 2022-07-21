package etcd

import (
	"bytes"
	"fmt"
	"os"
	"time"
)

// 100k entrys of 64b
const defaultInitBuffSize = 100000 * 8

type LatencyMsr struct {
	buff *bytes.Buffer
	file *os.File
}

func NewLatencyMsr(filename string) (*LatencyMsr, error) {
	lm := &LatencyMsr{
		buff: &bytes.Buffer{},
	}
	lm.buff.Grow(defaultInitBuffSize)

	fd, err := os.OpenFile(filename, os.O_CREATE|os.O_TRUNC|os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		return nil, err
	}
	lm.file = fd
	return lm, nil
}

func (rm *LatencyMsr) Record(lat time.Duration) error {
	_, err := fmt.Fprintf(rm.buff, "%d\n", lat.Nanoseconds())
	return err
}

func (rm *LatencyMsr) Flush() error {
	if _, err := rm.buff.WriteTo(rm.file); err != nil {
		return err
	}

	if err := rm.file.Sync(); err != nil {
		return err
	}
	return nil
}

func (rm *LatencyMsr) Close() error {
	return rm.file.Close()
}
