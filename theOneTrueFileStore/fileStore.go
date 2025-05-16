package theOneTrueFileStore

import (
	"encoding/json"
	"fmt"
    "github.com/donomii/ensemblekv"

	"bytes"
	"context"
	"sync"
)

var theOneTrueFileStore ensemblekv.KvLike

// directory string, N, maxBlock, maxKeys, filesie int64, createStore CreatorFunc)
func init() {
	var err error

	// Use multiple bbolt databases to store all data
	theOneTrueFileStore, err = ensemblekv.NewEnsembleKv("/tmp/ensemblekv", 200, 999999999, 999999999, 9999999999, ensemblekv.BoltDbCreator)

	// Use S3 as the backend (or minio)
	//theOneTrueFileStore, err = ensemblekv.NewS3Shim("http://localhost:55096", "minioadmin", "minioadmin", "defaultRegion", "weaviatebucket", "")

	if err != nil {
		panic(err)
	}
}

func TheOneTrueFileStore() ensemblekv.KvLike {
	return theOneTrueFileStore
}

func JsonPrintf(format string, args ...interface{}) {
	jsonArgs := make([]interface{}, len(args))
	for i, arg := range args {
		jsonData, _ := json.Marshal(arg)
		jsonArgs[i] = string(jsonData)
	}
	fmt.Printf(format, jsonArgs...)
}

type StreamingCursor struct {
	keys    [][]byte
	stream  chan kvPair
	cancel  context.CancelFunc
	ctx     context.Context
	keyOnly bool

	mu     sync.Mutex
	closed bool

	mapFunc func(func([]byte, []byte) error) error
	started bool
	advance chan struct{}
	done    chan struct{}
	index int
	prefix []byte
}

type kvPair struct {
	key   []byte
	value []byte
	err   error
}

func NewStreamingCursor(ctx context.Context, prefix string, keyOnly bool) *StreamingCursor {
	ctx, cancel := context.WithCancel(ctx)

	fmt.Printf("Creating streaming cursor with prefix: %s\n", prefix)

	// Fetch all the keys from the bucket, with prefix
	var keys [][]byte
	TheOneTrueFileStore().MapFunc(func(k, v []byte) error {
		if bytes.HasPrefix(k, []byte(prefix)) {
			keys = append(keys, k)
		}
		return nil
	})

	return &StreamingCursor{
		index: 0,
		keys:    keys,
		cancel:  cancel,
		ctx:     ctx,
		keyOnly: keyOnly,
	}
}


func (c *StreamingCursor) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.closed {
		c.cancel()
		c.closed = true
	}
}

func (c *StreamingCursor) First() ([]byte, []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()

	fmt.Printf("Moving to first item in bucket %v\n", c.prefix)

	c.index = 0
	if len(c.keys) == 0 {
		c.closed = true
		return nil, nil
	}
	key := c.keys[c.index]
	val, err := TheOneTrueFileStore().Get(key)
	if err != nil {
		fmt.Printf("Error getting value for key %s: %v\n", key, err)
		return nil, nil
	}
	trimmedKey := bytes.TrimPrefix(key, c.prefix)
	return trimmedKey, val
}

func (c *StreamingCursor) Next() ([]byte, []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	fmt.Printf("Moving to next element in bucket %v\n", c.prefix)
	c.index = c.index +1
	if c.index+1>len(c.keys) {
		c.closed = true
	}

	if c.closed {
		return nil, nil
	}


key := c.keys[c.index]
	val,err := TheOneTrueFileStore().Get(key)
	if err != nil {
		fmt.Printf("Error getting value for key %s: %v\n", key, err)
		return nil, nil
	}
	trimmedKey := bytes.TrimPrefix(key, c.prefix)
	if c.keyOnly {
		return trimmedKey, nil
	}
	return trimmedKey, val
}

func (c *StreamingCursor) Seek(target []byte) ([]byte, []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	fmt.Printf("Seeking to element %v in bucket %v\n",target, c.prefix)

	if c.closed {
		return nil, nil
	}

	for i:= 0 ; i < len(c.keys)-1; i++ {
key := c.keys[c.index]
	val, err := TheOneTrueFileStore().Get(key)
	if err != nil {
		fmt.Printf("Error getting value for key %s: %v\n", key, err)
		return nil, nil
	}
	trimmedKey := bytes.TrimPrefix(key, c.prefix)



		if bytes.Compare(trimmedKey, target) >= 0 {
			if c.keyOnly {
				return trimmedKey, nil
			}
			return trimmedKey, val
		}
	}
	c.closed = true
	return nil, nil
}

