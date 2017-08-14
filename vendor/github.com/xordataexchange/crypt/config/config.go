package config

import (
	"bytes"
	"io"
	"io/ioutil"

	"github.com/xordataexchange/crypt/backend"
	"github.com/xordataexchange/crypt/backend/consul"
	"github.com/xordataexchange/crypt/backend/etcd"
	"github.com/xordataexchange/crypt/encoding/secconf"
)

type KVPair struct {
	backend.KVPair
}

type KVPairs []*KVPair

type configManager struct {
	keystore []byte
	store    backend.Store
}

// A ConfigManager retrieves and decrypts configuration from a key/value store.
type ConfigManager interface {
	Get(key string) ([]byte, error)
	List(key string) (KVPairs, error)
	Set(key string, value []byte) error
	Watch(key string, stop chan bool) <-chan *Response
}

type standardConfigManager struct {
	store backend.Store
}

func NewStandardConfigManager(client backend.Store) (ConfigManager, error) {
	return standardConfigManager{client}, nil
}

func NewConfigManager(client backend.Store, keystore io.Reader) (ConfigManager, error) {
	bytes, err := ioutil.ReadAll(keystore)
	if err != nil {
		return nil, err
	}
	return configManager{bytes, client}, nil
}

// NewStandardEtcdConfigManager returns a new ConfigManager backed by etcd.
func NewStandardEtcdConfigManager(machines []string) (ConfigManager, error) {
	store, err := etcd.New(machines)
	if err != nil {
		return nil, err
	}

	return NewStandardConfigManager(store)
}

// NewStandardConsulConfigManager returns a new ConfigManager backed by consul.
func NewStandardConsulConfigManager(machines []string) (ConfigManager, error) {
	store, err := consul.New(machines)
	if err != nil {
		return nil, err
	}
	return NewStandardConfigManager(store)
}

// NewEtcdConfigManager returns a new ConfigManager backed by etcd.
// Data will be encrypted.
func NewEtcdConfigManager(machines []string, keystore io.Reader) (ConfigManager, error) {
	store, err := etcd.New(machines)
	if err != nil {
		return nil, err
	}
	return NewConfigManager(store, keystore)
}

// NewConsulConfigManager returns a new ConfigManager backed by consul.
// Data will be encrypted.
func NewConsulConfigManager(machines []string, keystore io.Reader) (ConfigManager, error) {
	store, err := consul.New(machines)
	if err != nil {
		return nil, err
	}
	return NewConfigManager(store, keystore)
}

// Get retrieves and decodes a secconf value stored at key.
func (c configManager) Get(key string) ([]byte, error) {
	value, err := c.store.Get(key)
	if err != nil {
		return nil, err
	}
	return secconf.Decode(value, bytes.NewBuffer(c.keystore))
}

// Get retrieves a value stored at key.
// convenience function, no additional value provided over
// `etcdctl`
func (c standardConfigManager) Get(key string) ([]byte, error) {
	value, err := c.store.Get(key)
	if err != nil {
		return nil, err
	}
	return value, err
}

// List retrieves and decodes all secconf value stored under key.
func (c configManager) List(key string) (KVPairs, error) {
	list, err := c.store.List(key)
	retList := make(KVPairs, len(list))
	if err != nil {
		return nil, err
	}
	for i, kv := range list {
		retList[i].Key = kv.Key
		retList[i].Value, err = secconf.Decode(kv.Value, bytes.NewBuffer(c.keystore))
		if err != nil {
			return nil, err
		}
	}
	return retList, nil
}

// List retrieves all values under key.
// convenience function, no additional value provided over
// `etcdctl`
func (c standardConfigManager) List(key string) (KVPairs, error) {
	list, err := c.store.List(key)
	retList := make(KVPairs, len(list))
	if err != nil {
		return nil, err
	}
	for i, kv := range list {
		retList[i].Key = kv.Key
		retList[i].Value = kv.Value
	}
	return retList, err
}

// Set will put a key/value into the data store
// and encode it with secconf
func (c configManager) Set(key string, value []byte) error {
	encodedValue, err := secconf.Encode(value, bytes.NewBuffer(c.keystore))
	if err == nil {
		err = c.store.Set(key, encodedValue)
	}
	return err
}

// Set will put a key/value into the data store
func (c standardConfigManager) Set(key string, value []byte) error {
	err := c.store.Set(key, value)
	return err
}

type Response struct {
	Value []byte
	Error error
}

func (c configManager) Watch(key string, stop chan bool) <-chan *Response {
	resp := make(chan *Response, 0)
	backendResp := c.store.Watch(key, stop)
	go func() {
		for {
			select {
			case <-stop:
				return
			case r := <-backendResp:
				if r.Error != nil {
					resp <- &Response{nil, r.Error}
					continue
				}
				value, err := secconf.Decode(r.Value, bytes.NewBuffer(c.keystore))
				resp <- &Response{value, err}
			}
		}
	}()
	return resp
}

func (c standardConfigManager) Watch(key string, stop chan bool) <-chan *Response {
	resp := make(chan *Response, 0)
	backendResp := c.store.Watch(key, stop)
	go func() {
		for {
			select {
			case <-stop:
				return
			case r := <-backendResp:
				if r.Error != nil {
					resp <- &Response{nil, r.Error}
					continue
				}
				resp <- &Response{r.Value, nil}
			}
		}
	}()
	return resp
}
