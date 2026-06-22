package etcd

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"sync/atomic"
	"time"
)

const statusFmt = "200:%d-408:%d-500:%d\n"

type StatusMsr struct {
	tick         *time.Ticker
	countSuccess atomic.Uint32
	countTimeout atomic.Uint32
	countFail    atomic.Uint32

	buff *bytes.Buffer
	file *os.File
}

func NewStatusMsr(filename string) (*StatusMsr, error) {
	sm := &StatusMsr{
		tick: time.NewTicker(time.Second),
		buff: &bytes.Buffer{},
	}
	sm.buff.Grow(initBuffCap)

	fd, err := os.OpenFile(filename, os.O_CREATE|os.O_TRUNC|os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		return nil, err
	}
	sm.file = fd
	return sm, nil
}

func (sm *StatusMsr) CountStatusFromErr(err error) {
	if err == nil {
		sm.countSuccess.Add(1)

		// TODO (Gus): must evaluate if this error assert works
	} else if errors.Is(err, context.DeadlineExceeded) {
		sm.countTimeout.Add(1)

	} else {
		sm.countFail.Add(1)
	}
}

func (sm *StatusMsr) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return

		case <-sm.tick.C:
			countS := sm.countSuccess.Swap(0)
			countT := sm.countTimeout.Swap(0)
			countF := sm.countFail.Swap(0)

			if _, err := fmt.Fprintf(sm.buff, statusFmt, countS, countT, countF); err != nil {
				log.Fatalln("failed writing status measurement, err:", err.Error())
				return
			}
		}
	}
}

func (sm *StatusMsr) Flush() error {
	if _, err := sm.buff.WriteTo(sm.file); err != nil {
		return err
	}

	if err := sm.file.Sync(); err != nil {
		return err
	}
	return nil
}

func (sm *StatusMsr) Close() error {
	return sm.file.Close()
}
