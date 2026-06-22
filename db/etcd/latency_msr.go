package etcd

import (
	"bytes"
	"fmt"
	"os"
	"time"
)

const initBuffCap = 1 << 11

type LatencyMsr struct {
	buff *bytes.Buffer
	file *os.File
}

func NewLatencyMsr(filename string) (*LatencyMsr, error) {
	lm := &LatencyMsr{
		buff: &bytes.Buffer{},
	}
	lm.buff.Grow(initBuffCap)

	fd, err := os.OpenFile(filename, os.O_CREATE|os.O_TRUNC|os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		return nil, err
	}
	lm.file = fd
	return lm, nil
}

func (lm *LatencyMsr) Record(lat time.Duration) error {
	_, err := fmt.Fprintf(lm.buff, "%d\n", lat.Nanoseconds())
	return err
}

func (lm *LatencyMsr) Flush() error {
	if _, err := lm.buff.WriteTo(lm.file); err != nil {
		return err
	}

	if err := lm.file.Sync(); err != nil {
		return err
	}
	return nil
}

func (lm *LatencyMsr) Close() error {
	return lm.file.Close()
}
