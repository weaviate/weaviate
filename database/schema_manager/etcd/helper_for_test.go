/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2018 Weaviate. All rights reserved.
 * LICENSE: https://github.com/creativesoftwarefdn/weaviate/blob/develop/LICENSE.md
 * AUTHOR: Bob van Luijt (bob@kub.design)
 * See www.creativesoftwarefdn.org for details
 * Contact: @CreativeSofwFdn / bob@kub.design
 */
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
