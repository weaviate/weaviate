package common

import (
	"encoding/binary"
	"errors"
	"sync"
	"sync/atomic"

	bolt "go.etcd.io/bbolt"
)

// Sequence represents a monotonic uint64 generator that can be used to
// generate unique incrementing ids for stored postings and other entities.
// It is inspired by Postgres Sequences, although much simpler
// and it's designed to ensure the following properties:
// 1. Monotonicity: Each generated id is guaranteed to be greater than
// the previously generated id.
// 2. Persistence: The state of the generator is persisted to disk.
// 3. Low disk I/O: The generator is designed to minimize
// the number of disk writes by allocating ranges of ids in memory.
// This allows it to serve most requests from memory without touching
// the disk.
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

// NewSequence loads the upper bound from the store and returns a ready to use Sequence.
func NewSequence(store SequenceStore, rangeSize uint64) (*Sequence, error) {
	if rangeSize == 0 {
		return nil, errors.New("rangeSize must be greater than zero")
	}

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

	s.mu.Lock()
	upperBound := s.upperBound.Load()
	// re-check after acquiring the lock
	if next > upperBound {
		// allocate a new range, higher than the current value
		newUpperBound := upperBound
		for next > newUpperBound {
			newUpperBound += s.rangeSize
		}

		err := s.store.Store(newUpperBound)
		if err != nil {
			s.mu.Unlock()
			return 0, err
		}

		s.upperBound.Store(newUpperBound)
	}
	s.mu.Unlock()

	return next, nil
}

// Flush persists the last used id to the store, to be used
// as the next starting point when the sequence is re-opened.
// This must be called to gracefully close the sequence and
// avoid gaps after a restart.
// Callers need to ensure no concurrent calls to Next() are
// happening while Flush() is in progress.
func (s *Sequence) Flush() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	lastUsed := s.counter.value.Load()
	err := s.store.Store(lastUsed)
	if err != nil {
		return err
	}

	s.upperBound.Store(lastUsed)
	return nil
}

// SequenceStore defines the interface for persisting the state of a Sequence.
// Implementations don't need to be thread-safe.
type SequenceStore interface {
	Store(upperBound uint64) error
	Load() (uint64, error)
}

var _ SequenceStore = (*BoltStore)(nil)

// BoltStore is a SequenceStore implementation that uses BoltDB as the backend.
type BoltStore struct {
	db     *bolt.DB
	bucket []byte
	key    []byte
}

func NewBoltStore(db *bolt.DB, bucket, key []byte) *BoltStore {
	return &BoltStore{
		db:     db,
		bucket: bucket,
		key:    key,
	}
}

func (s *BoltStore) Store(upperBound uint64) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(s.bucket)
		if b == nil {
			var err error
			b, err = tx.CreateBucketIfNotExists(s.bucket)
			if err != nil {
				return err
			}
		}
		buf := make([]byte, 8)
		binary.LittleEndian.PutUint64(buf, upperBound)
		return b.Put(s.key, buf)
	})
}

func (s *BoltStore) Load() (uint64, error) {
	var upperBound uint64
	err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(s.bucket)
		if b == nil {
			upperBound = 0
			return nil
		}
		data := b.Get(s.key)
		if data == nil {
			upperBound = 0
			return nil
		}
		upperBound = binary.LittleEndian.Uint64(data)
		return nil
	})
	if err != nil {
		return 0, err
	}
	return upperBound, nil
}
