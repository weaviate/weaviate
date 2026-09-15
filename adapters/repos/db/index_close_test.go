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

package db

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newClosableIndex() *Index {
	idx := &Index{}
	idx.closingCtx, idx.closingCancel = context.WithCancel(context.Background())
	return idx
}

func TestIndexCloseGuards(t *testing.T) {
	tests := []struct {
		name        string
		closeBefore bool
		op          func(idx *Index, ran *bool) error
		wantErr     error
		wantRan     bool
	}{
		{
			name: "enterRead admits on a live index",
			op: func(idx *Index, ran *bool) error {
				err := idx.enterRead()
				if err == nil {
					*ran = true
					idx.exitRead()
				}
				return err
			},
			wantRan: true,
		},
		{
			name:        "enterRead rejects after close",
			closeBefore: true,
			op: func(idx *Index, ran *bool) error {
				err := idx.enterRead()
				if err == nil {
					*ran = true
					idx.exitRead()
				}
				return err
			},
			wantErr: errAlreadyShutdown,
		},
		{
			name: "withCloseRLockGuard runs fn on a live index",
			op: func(idx *Index, ran *bool) error {
				return idx.withCloseRLockGuard(func() error { *ran = true; return nil })
			},
			wantRan: true,
		},
		{
			name:        "withCloseRLockGuard skips fn after close",
			closeBefore: true,
			op: func(idx *Index, ran *bool) error {
				return idx.withCloseRLockGuard(func() error { *ran = true; return nil })
			},
			wantErr: context.Canceled,
		},
		{
			name: "beginClose succeeds once",
			op: func(idx *Index, _ *bool) error {
				return idx.beginClose()
			},
		},
		{
			name:        "beginClose is idempotent",
			closeBefore: true,
			op: func(idx *Index, _ *bool) error {
				return idx.beginClose()
			},
			wantErr: errAlreadyShutdown,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			idx := newClosableIndex()
			if tc.closeBefore {
				require.NoError(t, idx.beginClose())
			}

			ran := false
			err := tc.op(idx, &ran)

			if tc.wantErr != nil {
				assert.ErrorIs(t, err, tc.wantErr)
			} else {
				assert.NoError(t, err)
			}
			assert.Equal(t, tc.wantRan, ran)
		})
	}
}

// TestBeginCloseDoesNotBlockNewcomers pins the deadlock root cause: drop() used
// to hold closeLock for writing across teardown, so a peer's RPC handler blocked
// behind it while this node's own pre-filter blocked on that peer.
func TestBeginCloseDoesNotBlockNewcomers(t *testing.T) {
	tests := []struct {
		name     string
		newcomer func(idx *Index) error
		wantErr  error
	}{
		{
			name:     "enterRead",
			newcomer: func(idx *Index) error { return idx.enterRead() },
			wantErr:  errAlreadyShutdown,
		},
		{
			name:     "withCloseRLockGuard",
			newcomer: func(idx *Index) error { return idx.withCloseRLockGuard(func() error { return nil }) },
			wantErr:  context.Canceled,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			idx := newClosableIndex()
			require.NoError(t, idx.enterRead())

			closed := make(chan struct{})
			go func() {
				_ = idx.beginClose()
				close(closed)
			}()

			// closingCtx is cancelled under the write lock, so this also proves the
			// lock is released by the time we probe.
			<-idx.closingCtx.Done()

			bail := make(chan error, 1)
			go func() { bail <- tc.newcomer(idx) }()

			select {
			case err := <-bail:
				assert.ErrorIs(t, err, tc.wantErr)
			case <-time.After(5 * time.Second):
				t.Fatal("newcomer blocked while beginClose drained; a peer's RPC handler would deadlock")
			}

			select {
			case <-closed:
				t.Fatal("beginClose returned while an in-flight reader was still running")
			case <-time.After(50 * time.Millisecond):
			}

			idx.exitRead()

			select {
			case <-closed:
			case <-time.After(5 * time.Second):
				t.Fatal("beginClose did not return after its in-flight reader exited")
			}
		})
	}
}

// TestEnterReadRacesBeginClose: an Add must never land after Wait starts, which
// would panic the WaitGroup.
func TestEnterReadRacesBeginClose(t *testing.T) {
	tests := []struct {
		readers    int
		iterations int
	}{
		{readers: 1, iterations: 50},
		{readers: 8, iterations: 50},
		{readers: 64, iterations: 10},
	}

	for _, tc := range tests {
		t.Run(fmt.Sprintf("%d readers", tc.readers), func(t *testing.T) {
			for range tc.iterations {
				idx := newClosableIndex()

				var wg sync.WaitGroup
				for range tc.readers {
					wg.Add(1)
					go func() {
						defer wg.Done()
						if err := idx.enterRead(); err == nil {
							idx.exitRead()
						}
					}()
				}

				wg.Add(1)
				go func() {
					defer wg.Done()
					_ = idx.beginClose()
				}()

				wg.Wait()
				assert.ErrorIs(t, idx.enterRead(), errAlreadyShutdown)
			}
		})
	}
}
