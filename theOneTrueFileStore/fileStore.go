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
	//theOneTrueFileStore, err = ensemblekv.NewEnsembleKv("/tmp/ensemblekv", 200, 999999999, 999999999, 9999999999, ensemblekv.BoltDbCreator)

	// Use S3 as the backend (or minio)
	theOneTrueFileStore, err = ensemblekv.NewS3Shim("http://localhost:55096", "minioadmin", "minioadmin", "defaultRegion", "weaviatebucket", "")

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
}

type kvPair struct {
	key   []byte
	value []byte
	err   error
}

func NewStreamingCursor(ctx context.Context, prefix string, keyOnly bool) *StreamingCursor {
	ctx, cancel := context.WithCancel(ctx)

	fmt.Printf("Creating streaming cursor with prefix: %s\n", prefix)


	return &StreamingCursor{
		stream:  make(chan kvPair, 1),
		cancel:  cancel,
		ctx:     ctx,
		keyOnly: keyOnly,
		advance: make(chan struct{}),
		done:    make(chan struct{}),
	}
}

func (c *StreamingCursor) run() {
	defer close(c.done)
	TheOneTrueFileStore().MapFunc(func(k, v []byte) error {
		select {
		case <-c.ctx.Done():
			return c.ctx.Err()
		case <-c.advance:
			select {
			case <-c.ctx.Done():
				return c.ctx.Err()
			case c.stream <- kvPair{key: append([]byte(nil), k...), value: append([]byte(nil), v...)}:
			}
		}
		return nil
	})
	close(c.stream)
}

func (c *StreamingCursor) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.closed {
		c.cancel()
		c.closed = true
	}
}

func (c *StreamingCursor) restartLocked() {
	if c.started {
		c.cancel()
	}
	ctx, cancel := context.WithCancel(context.Background())
	c.ctx = ctx
	c.cancel = cancel
	c.stream = make(chan kvPair, 1)
	c.advance = make(chan struct{})
	c.done = make(chan struct{})
	c.closed = false
	c.started = true
	go c.run()
}

func (c *StreamingCursor) First() ([]byte, []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.restartLocked()

	select {
	case c.advance <- struct{}{}:
	case <-c.done:
		return nil, nil
	}

	pair, ok := <-c.stream
	if !ok || pair.err != nil {
		c.closed = true
		return nil, nil
	}

	if c.keyOnly {
		return pair.key, nil
	}
	return pair.key, pair.value
}

func (c *StreamingCursor) Next() ([]byte, []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil, nil
	}

	if !c.started {
		c.started = true
		go c.run()
	}

	select {
	case c.advance <- struct{}{}:
	case <-c.done:
		return nil, nil
	}

	pair, ok := <-c.stream
	if !ok || pair.err != nil {
		c.closed = true
		return nil, nil
	}

	if c.keyOnly {
		return pair.key, nil
	}
	return pair.key, pair.value
}

func (c *StreamingCursor) Seek(target []byte) ([]byte, []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil, nil
	}

	for {
		pair, ok := <-c.stream
		if !ok || pair.err != nil {
			c.closed = true
			return nil, nil
		}

		if bytes.Compare(pair.key, target) >= 0 {
			if c.keyOnly {
				return pair.key, nil
			}
			return pair.key, pair.value
		}
	}
}
