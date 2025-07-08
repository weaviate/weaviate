package spfresh

import (
	"context"
	"fmt"
	"sync"

	"github.com/pkg/errors"
)

// BlockController is the interface for managing I/O of postings on disk.
type BlockController interface {
	// Get reads the entire data of the given posting.
	// The returned data must not be modified by the caller.
	Get(ctx context.Context, id uint64) ([]byte, error)

	// ParallelGet reads multiple postings concurrently.
	// The returned data slices are in the same order as the ids.
	// If any id is not found, the function returns an error.
	ParallelGet(ctx context.Context, ids []uint64) ([][]byte, error)

	// Put writes the full content of a posting, overwriting any existing data.
	Put(ctx context.Context, id uint64, data []byte) error

	// Append appends new data to an existing posting.
	Append(ctx context.Context, id uint64, tail []byte) error

	// Delete removes a posting and reclaims its blocks.
	Delete(ctx context.Context, id uint64) error

	// Flush writes all buffered data to disk (if applicable).
	Flush(ctx context.Context) error
}

type DummyBlockController struct {
	mu   sync.RWMutex
	data map[uint64][]byte
}

func NewDummyBlockController() *DummyBlockController {
	return &DummyBlockController{
		data: make(map[uint64][]byte),
	}
}

func (d *DummyBlockController) Get(ctx context.Context, id uint64) ([]byte, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	data, exists := d.data[id]
	if !exists {
		return nil, errors.Errorf("posting %d not found", id)
	}

	return data, nil
}

func (d *DummyBlockController) ParallelGet(ctx context.Context, ids []uint64) ([][]byte, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	results := make([][]byte, len(ids))
	for i, id := range ids {
		data, exists := d.data[id]
		if !exists {
			return nil, fmt.Errorf("posting %d not found", id)
		}
		results[i] = data
	}

	return results, nil
}

func (d *DummyBlockController) Put(ctx context.Context, id uint64, data []byte) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.data[id] = data
	return nil
}

func (d *DummyBlockController) Append(ctx context.Context, id uint64, tail []byte) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if _, exists := d.data[id]; !exists {
		return errors.Errorf("posting %d not found", id)
	}

	d.data[id] = append(d.data[id], tail...)
	return nil
}

func (d *DummyBlockController) Delete(ctx context.Context, id uint64) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if _, exists := d.data[id]; !exists {
		return errors.Errorf("posting %d not found", id)
	}

	delete(d.data, id)
	return nil
}

func (d *DummyBlockController) Flush(ctx context.Context) error {
	// No-op for dummy implementation
	return nil
}
