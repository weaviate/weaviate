package etcd

import (
	"context"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/mvcc/mvccpb"
)

type fakeETCDClient struct {
	kv map[string]string
}

func newFakeETCDClient() *fakeETCDClient {
	return &fakeETCDClient{
		kv: map[string]string{},
	}
}
func (c *fakeETCDClient) Put(ctx context.Context, key, val string, opts ...clientv3.OpOption) (*clientv3.PutResponse, error) {
	c.kv[key] = val
	return &clientv3.PutResponse{}, nil
}

func (c *fakeETCDClient) Get(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
	val, ok := c.kv[key]
	if !ok {
		return &clientv3.GetResponse{
			Count: 0,
		}, nil
	}

	return &clientv3.GetResponse{
		Count: 1,
		Kvs:   []*mvccpb.KeyValue{&mvccpb.KeyValue{Key: []byte(key), Value: []byte(val)}},
	}, nil
}
