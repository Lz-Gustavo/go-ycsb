package etcd

import (
	"context"
	"time"

	"go.etcd.io/etcd/clientv3"
)

const (
	defaultEtcdIP   = "127.0.0.1"
	defaultEtcdPort = ":2379"
)

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
		Endpoints:   []string{defaultEtcdIP + defaultEtcdPort},
		Context:     ct,
		DialTimeout: 3 * time.Second,
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
