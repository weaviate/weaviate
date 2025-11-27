package common

import (
	"sync"
	"sync/atomic"
)

// Sequence represents a monotonic uint64 generator that can be used to
// generate unique incrementing ids for stored postings and other entities.
// It is inspired by Postgres Sequences, although much simpler
// and it's designed to ensure the following properties:
// 1. Monotonicity: Each generated id is guaranteed to be greater than
// the previously generated id.
// 2. Persistence: The state of the generator is persisted to disk.
// 3. High Throughput: The generator is optimized for high performance
// 4. Concurrency: The generator is safe for concurrent access.
// However because of its design, it does not guarantee gapless ids.
// A Sequence first allocates a range of ids and reserves them in the
// persistent storage. It then serves those ids from memory without
// touching the disk.
// If the sequence is closed gracefully, if persists the last used id, however
// in case of a crash, the sequence will start from the upper bound of the reserved range
// potentially leaving gaps.
// The range is configurable and can be tuned based on the expected throughput requirements.
type Sequence struct {
	counter    *MonotonicCounter
	upperBound atomic.Uint64
	rangeSize  uint64
	store      SequenceStore
	mu         sync.Mutex
}

// SequenceStore defines the interface for persisting the state of a Sequence.
// Implementations don't need to be thread-safe.
type SequenceStore interface {
	Store(upperBound uint64) error
	Load() (uint64, error)
}

// NewSequence loads the upper bound from the store and returns a ready to use Sequence.
func NewSequence(store SequenceStore, rangeSize uint64) (*Sequence, error) {
	upperBound, err := store.Load()
	if err != nil {
		return nil, err
	}

	seq := Sequence{
		counter:   NewMonotonicCounter(upperBound),
		rangeSize: rangeSize,
		store:     store,
	}
	seq.upperBound.Store(upperBound)

	return &seq, nil
}

// Next returns the next value in the sequence.
// Most of the time it will be served from memory without
// touching the disk.
func (s *Sequence) Next() (uint64, error) {
	next := s.counter.Next()
	// fast path
	if next <= s.upperBound.Load() {
		return next, nil
	}

	for next > s.upperBound.Load() {
		s.mu.Lock()
		// re-check after acquiring the lock
		if next <= s.upperBound.Load() {
			s.mu.Unlock()
			break
		}

		// allocate a new range
		newUpperBound := s.upperBound.Load() + s.rangeSize
		err := s.store.Store(newUpperBound)
		if err != nil {
			s.mu.Unlock()
			return 0, err
		}

		s.upperBound.Store(newUpperBound)
		s.mu.Unlock()
	}

	return next, nil
}
