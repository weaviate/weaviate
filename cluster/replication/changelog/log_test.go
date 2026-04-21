//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package changelog_test

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"sort"
	"sync"
	"testing"
	"time"

	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/replication/changelog"
	enterrors "github.com/weaviate/weaviate/entities/errors"
)

func newLog(t *testing.T) *changelog.ChangeLog {
	t.Helper()
	logger, _ := logrustest.NewNullLogger()
	path := filepath.Join(t.TempDir(), "op.log")
	cl, err := changelog.Open(path, logger)
	require.NoError(t, err)
	t.Cleanup(func() { _ = cl.Deactivate() })
	return cl
}

// uuidForSeq returns a deterministic UUID encoding seq in its first 8 bytes.
func uuidForSeq(seq uint64) [16]byte {
	var id [16]byte
	binary.BigEndian.PutUint64(id[:8], seq)
	return id
}

// TestChangeLog_ConcurrentAppend_Monotonic is the workhorse test: it
// validates LSN monotonicity under contention (the core replication
// correctness invariant) and end-to-end exercises Append → file → Tailer.
func TestChangeLog_ConcurrentAppend_Monotonic(t *testing.T) {
	const (
		writers    = 16
		perWriter  = 100
		totalWrite = writers * perWriter
	)
	cl := newLog(t)
	logger, _ := logrustest.NewNullLogger()

	type record struct {
		writer int
		seq    int
		uuid   [16]byte
		body   string
	}

	var (
		wg            sync.WaitGroup
		recordsMu     sync.Mutex
		records       = make(map[uint64]record, totalWrite)
		perWriterLSNs = make([][]uint64, writers)
	)
	wg.Add(writers)

	for w := range writers {
		perWriterLSNs[w] = make([]uint64, 0, perWriter)
		enterrors.GoWrapper(func() {
			defer wg.Done()
			for i := range perWriter {
				id := uuidForSeq(uint64(w)*perWriter + uint64(i) + 1)
				body := fmt.Sprintf("w%d-%d", w, i)
				lsn, err := cl.AppendPut(id, int64(i), []byte(body))
				if err != nil {
					t.Errorf("writer %d append: %v", w, err)
					return
				}
				recordsMu.Lock()
				records[lsn] = record{writer: w, seq: i, uuid: id, body: body}
				perWriterLSNs[w] = append(perWriterLSNs[w], lsn)
				recordsMu.Unlock()
			}
		}, logger)
	}
	wg.Wait()

	// Every LSN 1..totalWrite must have been assigned exactly once.
	require.Len(t, records, totalWrite)
	for i := uint64(1); i <= totalWrite; i++ {
		require.Containsf(t, records, i, "LSN %d missing", i)
	}

	// Each writer's own appends must have received monotonically increasing
	// LSNs (per-producer order preservation — mirrors how docIdLock gives us
	// per-UUID commit order in production).
	for w, lsns := range perWriterLSNs {
		require.Truef(t, sort.SliceIsSorted(lsns, func(i, j int) bool { return lsns[i] < lsns[j] }),
			"writer %d LSNs not monotonic: %v", w, lsns)
	}

	// Read every entry back via a single tailer; LSNs must emerge in order
	// 1..totalWrite and each payload must match what the writer recorded.
	tailer, err := cl.NewTailer(0)
	require.NoError(t, err)
	defer tailer.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	for i := uint64(1); i <= totalWrite; i++ {
		e, err := tailer.Next(ctx)
		require.NoError(t, err)
		require.Equal(t, i, e.LSN)
		expected := records[i]
		require.Equal(t, expected.uuid, e.UUID)
		require.Equal(t, expected.body, string(e.Payload))
	}
}

// TestChangeLog_Finalize_TailerDrainsAndEOFs combines the finalize contract
// (Phase 5 depends on finalLSN), the tailer drain+EOF path (Phase 4 gRPC
// hang risk), and the post-finalize rejection.
func TestChangeLog_Finalize_TailerDrainsAndEOFs(t *testing.T) {
	const k = 25
	cl := newLog(t)
	logger, _ := logrustest.NewNullLogger()

	tailer, err := cl.NewTailer(0)
	require.NoError(t, err)
	defer tailer.Close()

	// Start a tailer goroutine that collects everything until io.EOF.
	type result struct {
		entries []*changelog.Entry
		err     error
	}
	resCh := make(chan result, 1)
	enterrors.GoWrapper(func() {
		var got []*changelog.Entry
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		for {
			e, err := tailer.Next(ctx)
			if err != nil {
				resCh <- result{got, err}
				return
			}
			got = append(got, e)
		}
	}, logger)

	for i := range k {
		_, err := cl.AppendPut(uuidForSeq(uint64(i+1)), int64(i), []byte("x"))
		require.NoError(t, err)
	}

	finalLSN, err := cl.Finalize()
	require.NoError(t, err)
	require.Equal(t, uint64(k), finalLSN)

	// Post-finalize Append must reject.
	_, err = cl.AppendPut([16]byte{}, 0, nil)
	require.ErrorIs(t, err, changelog.ErrLogFinalized)

	// Tailer must have drained all k entries then returned EOF.
	select {
	case res := <-resCh:
		require.ErrorIs(t, res.err, io.EOF)
		require.Len(t, res.entries, k)
		for i, e := range res.entries {
			require.Equal(t, uint64(i+1), e.LSN)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("tailer did not terminate within 5s")
	}

	// Finalize is idempotent and returns the same LSN.
	again, err := cl.Finalize()
	require.NoError(t, err)
	require.Equal(t, finalLSN, again)
}

// TestChangeLog_TailerCancellation covers the two non-EOF escape hatches a
// blocked tailer needs to honor: ctx cancellation and Deactivate.
func TestChangeLog_TailerCancellation(t *testing.T) {
	t.Run("ctx cancel unblocks Next", func(t *testing.T) {
		cl := newLog(t)
		tailer, err := cl.NewTailer(0)
		require.NoError(t, err)
		defer tailer.Close()

		ctx, cancel := context.WithCancel(context.Background())
		logger, _ := logrustest.NewNullLogger()
		done := make(chan error, 1)
		enterrors.GoWrapper(func() {
			_, err := tailer.Next(ctx)
			done <- err
		}, logger)

		// Give the tailer a moment to block.
		time.Sleep(20 * time.Millisecond)
		cancel()

		select {
		case err := <-done:
			require.ErrorIs(t, err, context.Canceled)
		case <-time.After(2 * time.Second):
			t.Fatal("Next did not unblock on ctx cancel")
		}
	})

	t.Run("Deactivate unblocks Next", func(t *testing.T) {
		cl := newLog(t)
		tailer, err := cl.NewTailer(0)
		require.NoError(t, err)
		defer tailer.Close()

		logger, _ := logrustest.NewNullLogger()
		done := make(chan error, 1)
		enterrors.GoWrapper(func() {
			_, err := tailer.Next(context.Background())
			done <- err
		}, logger)

		time.Sleep(20 * time.Millisecond)
		require.NoError(t, cl.Deactivate())

		select {
		case err := <-done:
			require.True(t, errors.Is(err, changelog.ErrLogDeactivated),
				"want ErrLogDeactivated got %v", err)
		case <-time.After(2 * time.Second):
			t.Fatal("Next did not unblock on Deactivate")
		}

		// Subsequent Append rejects.
		_, err = cl.AppendPut([16]byte{}, 0, nil)
		require.ErrorIs(t, err, changelog.ErrLogDeactivated)
	})
}
