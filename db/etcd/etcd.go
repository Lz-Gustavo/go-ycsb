package etcd

import (
	"context"
	"time"

	"go.etcd.io/etcd/clientv3"
)

const (
	defaultEtcdIP   = "127.0.0.1"
	defaultEtcdPort = ":2379"

	dialTimeout = 5 * time.Second
)

var parsedEtcdHostname = ""

// Client ...
type Client struct {
	cl     *clientv3.Client
	cancel context.CancelFunc
}

// NewClient ...
func NewClient(ctx context.Context) (*Client, error) {
	ct, cn := context.WithCancel(ctx)
	ec := &Client{cancel: cn}

	cl, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{parsedEtcdHostname + defaultEtcdPort},
		Context:     ct,
		DialTimeout: dialTimeout,
	})
	if err != nil {
		return nil, err
	}
	ec.cl = cl
	return ec, nil
}

func (ec *Client) shutdown() {
	ec.cl.Close()
	ec.cancel()
}
