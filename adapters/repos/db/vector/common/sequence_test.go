package common

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"
)

var _ SequenceStore = (*dummyStore)(nil)

type dummyStore struct {
	upperBound uint64
}

func (d *dummyStore) Store(upperBound uint64) error {
	d.upperBound = upperBound
	return nil
}

func (d *dummyStore) Load() (uint64, error) {
	return d.upperBound, nil
}

func TestSequence(t *testing.T) {
	var store dummyStore

	seq, err := NewSequence(&store, 3)
	require.NoError(t, err)

	v, err := seq.Next()
	require.NoError(t, err)
	require.Equal(t, uint64(1), v)
	require.Equal(t, uint64(3), store.upperBound)

	v, err = seq.Next()
	require.NoError(t, err)
	require.Equal(t, uint64(2), v)
	require.Equal(t, uint64(3), store.upperBound)

	v, err = seq.Next()
	require.NoError(t, err)
	require.Equal(t, uint64(3), v)
	require.Equal(t, uint64(3), store.upperBound)

	v, err = seq.Next()
	require.NoError(t, err)
	require.Equal(t, uint64(4), v)
	require.Equal(t, uint64(6), store.upperBound)
}

func TestSequence_Concurrent(t *testing.T) {
	var store dummyStore

	seq, err := NewSequence(&store, 10)
	require.NoError(t, err)

	const routines = 50
	const perRoutine = 100
	results := make(chan uint64, routines*perRoutine)

	for i := 0; i < routines; i++ {
		go func() {
			for j := 0; j < perRoutine; j++ {
				v, err := seq.Next()
				require.NoError(t, err)
				results <- v
			}
		}()
	}

	collected := make(map[uint64]struct{})
	for range routines * perRoutine {
		v := <-results
		_, exists := collected[v]
		require.False(t, exists, "duplicate value: %d", v)
		collected[v] = struct{}{}
	}

	require.Equal(t, uint64(routines*perRoutine), uint64(len(collected)))
}

type errorStore struct {
	dummyStore

	failAfter int
}

func (e *errorStore) Store(upperBound uint64) error {
	e.failAfter--
	if e.failAfter > 0 {
		e.upperBound = upperBound
		return nil
	}
	return fmt.Errorf("disk error!")
}

func TestSequence_StoreError(t *testing.T) {
	var store errorStore
	store.failAfter = 2

	seq, err := NewSequence(&store, 2)
	require.NoError(t, err)

	_, err = seq.Next()
	require.NoError(t, err)

	_, err = seq.Next()
	require.NoError(t, err)

	_, err = seq.Next()
	require.Error(t, err)
}

func TestSequence_1RangeSize(t *testing.T) {
	var store dummyStore

	// zero range size should return an error
	_, err := NewSequence(&store, 0)
	require.Error(t, err)

	seq, err := NewSequence(&store, 1)
	require.NoError(t, err)

	for i := 1; i <= 5; i++ {
		v, err := seq.Next()
		require.NoError(t, err)
		require.Equal(t, uint64(i), v)
		require.Equal(t, uint64(i), store.upperBound)
	}
}

func TestSequence_Flush(t *testing.T) {
	var store dummyStore

	seq, err := NewSequence(&store, 3)
	require.NoError(t, err)

	for i := 1; i <= 5; i++ {
		v, err := seq.Next()
		require.NoError(t, err)
		require.Equal(t, uint64(i), v)
	}

	require.Equal(t, uint64(6), store.upperBound)

	err = seq.Flush()
	require.NoError(t, err)
	require.Equal(t, uint64(5), store.upperBound)
}

func TestSequence_BoltStore(t *testing.T) {
	db, err := bolt.Open(filepath.Join(t.TempDir(), "bolt.db"), 0600, nil)
	require.NoError(t, err)
	defer db.Close()

	store := NewBoltStore(db, []byte("bucket"), []byte("key"))
	seq, err := NewSequence(store, 4)
	require.NoError(t, err)

	for i := 1; i <= 7; i++ {
		v, err := seq.Next()
		require.NoError(t, err)
		require.Equal(t, uint64(i), v)
	}

	upperBound, err := store.Load()
	require.NoError(t, err)
	require.Equal(t, uint64(8), upperBound)

	err = seq.Flush()
	require.NoError(t, err)

	upperBound, err = store.Load()
	require.NoError(t, err)
	require.Equal(t, uint64(7), upperBound)
}

func TestSequence_Restart(t *testing.T) {
	var store dummyStore

	seq, err := NewSequence(&store, 4)
	require.NoError(t, err)

	for i := 1; i <= 7; i++ {
		v, err := seq.Next()
		require.NoError(t, err)
		require.Equal(t, uint64(i), v)
	}

	require.Equal(t, uint64(8), store.upperBound)

	err = seq.Flush()
	require.NoError(t, err)
	require.Equal(t, uint64(7), store.upperBound)

	// create a new sequence to simulate a restart
	seq, err = NewSequence(&store, 4)
	require.NoError(t, err)

	require.Equal(t, uint64(7), seq.upperBound.Load())

	for i := 8; i <= 10; i++ {
		v, err := seq.Next()
		require.NoError(t, err)
		require.Equal(t, uint64(i), v)
	}

	require.Equal(t, uint64(11), store.upperBound)

	// restart again, but this time without flushing
	seq, err = NewSequence(&store, 4)
	require.NoError(t, err)

	require.Equal(t, uint64(11), seq.upperBound.Load())

	v, err := seq.Next()
	require.NoError(t, err)
	require.Equal(t, uint64(12), v)
	require.Equal(t, uint64(15), store.upperBound)
}

func BenchmarkSequence_Next(b *testing.B) {
	rngs := []uint64{1, 64, 128, 512, 1024, 4096}
	for _, r := range rngs {
		b.Run(fmt.Sprintf("sequence=%d", r), func(b *testing.B) {
			db, err := bolt.Open(filepath.Join(b.TempDir(), "bolt.db"), 0600, nil)
			require.NoError(b, err)
			defer db.Close()

			store := NewBoltStore(db, []byte("bucket"), []byte("key"))

			seq, err := NewSequence(store, r)
			require.NoError(b, err)

			for b.Loop() {
				_, err := seq.Next()
				require.NoError(b, err)
			}
		})
	}
}
