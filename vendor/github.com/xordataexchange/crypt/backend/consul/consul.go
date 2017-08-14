package consul

import (
	"fmt"
	"strings"
	"time"

	"github.com/xordataexchange/crypt/backend"

	"github.com/armon/consul-api"
)

type Client struct {
	client    *consulapi.KV
	waitIndex uint64
}

func New(machines []string) (*Client, error) {
	conf := consulapi.DefaultConfig()
	if len(machines) > 0 {
		conf.Address = machines[0]
	}
	client, err := consulapi.NewClient(conf)
	if err != nil {
		return nil, err
	}
	return &Client{client.KV(), 0}, nil
}

func (c *Client) Get(key string) ([]byte, error) {
	kv, _, err := c.client.Get(key, nil)
	if err != nil {
		return nil, err
	}
	if kv == nil {
		return nil, fmt.Errorf("Key ( %s ) was not found.", key)
	}
	return kv.Value, nil
}

func (c *Client) List(key string) (backend.KVPairs, error) {
	pairs, _, err := c.client.List(key, nil)
	if err != nil {
		return nil, err
	}
	if err != nil {
		return nil, err
	}
	ret := make(backend.KVPairs, len(pairs), len(pairs))
	for i, kv := range pairs {
		ret[i] = &backend.KVPair{Key: kv.Key, Value: kv.Value}
	}
	return ret, nil
}

func (c *Client) Set(key string, value []byte) error {
	key = strings.TrimPrefix(key, "/")
	kv := &consulapi.KVPair{
		Key:   key,
		Value: value,
	}
	_, err := c.client.Put(kv, nil)
	return err
}

func (c *Client) Watch(key string, stop chan bool) <-chan *backend.Response {
	respChan := make(chan *backend.Response, 0)
	go func() {
		for {
			opts := consulapi.QueryOptions{
				WaitIndex: c.waitIndex,
			}
			keypair, meta, err := c.client.Get(key, &opts)
			if keypair == nil && err == nil {
				err = fmt.Errorf("Key ( %s ) was not found.", key)
			}
			if err != nil {
				respChan <- &backend.Response{nil, err}
				time.Sleep(time.Second * 5)
				continue
			}
			c.waitIndex = meta.LastIndex
			respChan <- &backend.Response{keypair.Value, nil}
		}
	}()
	return respChan
}
