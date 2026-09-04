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

package replica

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/storobj"
)

// fullReadFetcher implements only FetchObjects; the embedded nil RClient makes
// any other call panic rather than silently return a zero value.
type fullReadFetcher struct {
	RClient
	fn    func(ids []strfmt.UUID) ([]Replica, error)
	calls atomic.Int32
	// maxInFlight tracks observed concurrency
	inFlight    atomic.Int32
	maxInFlight atomic.Int32
}

func (f *fullReadFetcher) FetchObjects(_ context.Context, _, _, _ string,
	ids []strfmt.UUID,
) ([]Replica, error) {
	f.calls.Add(1)
	cur := f.inFlight.Add(1)
	for {
		old := f.maxInFlight.Load()
		if cur <= old || f.maxInFlight.CompareAndSwap(old, cur) {
			break
		}
	}
	defer f.inFlight.Add(-1)
	return f.fn(ids)
}

// seqID builds a UUID that encodes i, so a misattribution names the position it
// came from.
func seqID(i int) strfmt.UUID {
	return strfmt.UUID(fmt.Sprintf("00000000-0000-4000-8000-%012d", i))
}

func seqIDs(n int) []strfmt.UUID {
	ids := make([]strfmt.UUID, n)
	for i := range ids {
		ids[i] = seqID(i)
	}
	return ids
}

// replicaFor builds an honest response element: both the envelope ID and the
// carried object's own ID are the requested id.
func replicaFor(id strfmt.UUID) Replica {
	return Replica{
		ID: id,
		Object: &storobj.Object{Object: models.Object{
			ID: id, Class: "C1", LastUpdateTimeUnix: 42,
		}},
		LastUpdateTimeUnixMilli: 42,
	}
}

func newFullReadClient(t *testing.T, f *fullReadFetcher) FinderClient {
	t.Helper()
	logger, _ := test.NewNullLogger()
	return NewFinderClient(f, logger)
}

// TestFullReadsIdentityAtEveryPosition pins that the object at each position is
// the one requested at that position, across the chunk boundaries. Asserting
// only the length passes while every object is misattributed, so identity is
// checked at each index on both the envelope and the carried payload.
func TestFullReadsIdentityAtEveryPosition(t *testing.T) {
	sizes := []int{0, 1, 2, 15, 16, 17, 255, 256, 257, 511, 512, 513, 4096, 4097}
	for _, n := range sizes {
		t.Run(fmt.Sprintf("n=%d", n), func(t *testing.T) {
			ids := seqIDs(n)
			f := &fullReadFetcher{fn: func(chunk []strfmt.UUID) ([]Replica, error) {
				// honest server, but with jitter so chunks complete out of order
				time.Sleep(time.Duration(rand.Intn(400)) * time.Microsecond)
				out := make([]Replica, len(chunk))
				for i, id := range chunk {
					out[i] = replicaFor(id)
				}
				return out, nil
			}}
			rs, err := newFullReadClient(t, f).FullReads(context.Background(), "h", "C1", "S1", ids)
			require.NoError(t, err)
			require.Len(t, rs, n)
			for i := range ids {
				require.Equal(t, ids[i], rs[i].ID,
					"envelope id at position %d does not match requested id", i)
				require.NotNil(t, rs[i].Object, "object missing at position %d", i)
				require.Equal(t, ids[i], rs[i].Object.ID(),
					"OBJECT CONTENT at position %d belongs to a different id", i)
			}
			// fan-out bound must hold
			require.LessOrEqual(t, int(f.maxInFlight.Load()), MaxConcurrentFullReadRequests,
				"observed in-flight requests exceeded the bound")
		})
	}
}

// TestFullReadsRejectsMalformedChunks covers the response shapes a buggy peer
// can return. Each must produce an error and a nil result: a partial result
// that reads as complete is what lets the repairer write the wrong content.
func TestFullReadsRejectsMalformedChunks(t *testing.T) {
	tests := []struct {
		name    string
		n       int
		badMuts func(chunk []strfmt.UUID, out []Replica) []Replica
		// which chunk index to corrupt; -1 = all
		badChunk int
	}{
		{
			name: "chunk returns fewer objects than requested",
			n:    600,
			badMuts: func(_ []strfmt.UUID, out []Replica) []Replica {
				return out[:len(out)-1]
			},
			badChunk: 1,
		},
		{
			name: "chunk returns empty slice",
			n:    600,
			badMuts: func(_ []strfmt.UUID, out []Replica) []Replica {
				return nil
			},
			badChunk: 1,
		},
		{
			name: "chunk returns MORE objects than requested",
			n:    600,
			badMuts: func(_ []strfmt.UUID, out []Replica) []Replica {
				return append(out, replicaFor(seqID(99999)))
			},
			badChunk: 2,
		},
		{
			name: "chunk shuffles objects internally",
			n:    600,
			badMuts: func(_ []strfmt.UUID, out []Replica) []Replica {
				if len(out) >= 2 {
					out[0], out[len(out)-1] = out[len(out)-1], out[0]
				}
				return out
			},
			badChunk: 1,
		},
		{
			name: "first chunk shuffled",
			n:    600,
			badMuts: func(_ []strfmt.UUID, out []Replica) []Replica {
				if len(out) >= 2 {
					out[0], out[1] = out[1], out[0]
				}
				return out
			},
			badChunk: 0,
		},
		{
			name: "last chunk shuffled",
			n:    600,
			badMuts: func(_ []strfmt.UUID, out []Replica) []Replica {
				if len(out) >= 2 {
					out[0], out[len(out)-1] = out[len(out)-1], out[0]
				}
				return out
			},
			badChunk: 2,
		},
		{
			name: "single-chunk path shuffled (no concurrency)",
			n:    200,
			badMuts: func(_ []strfmt.UUID, out []Replica) []Replica {
				out[0], out[1] = out[1], out[0]
				return out
			},
			badChunk: 0,
		},
		{
			// envelope ids stay in request order while the carried objects are
			// swapped. The envelope is not what the repairer writes, so checking
			// only the envelope would accept this and file content under the
			// wrong id.
			name: "chunk keeps envelope ids but swaps the carried payloads",
			n:    600,
			badMuts: func(_ []strfmt.UUID, out []Replica) []Replica {
				out[0].Object, out[len(out)-1].Object = out[len(out)-1].Object, out[0].Object
				return out
			},
			badChunk: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ids := seqIDs(tt.n)
			var seen atomic.Int32
			f := &fullReadFetcher{}
			f.fn = func(chunk []strfmt.UUID) ([]Replica, error) {
				out := make([]Replica, len(chunk))
				for i, id := range chunk {
					out[i] = replicaFor(id)
				}
				// identify chunk by its first id's encoded index
				idx := -1
				for k := 0; k < tt.n; k += MaxFullReadIDsPerRequest {
					if chunk[0] == ids[k] {
						idx = k / MaxFullReadIDsPerRequest
						break
					}
				}
				if idx == tt.badChunk {
					seen.Add(1)
					return tt.badMuts(chunk, out), nil
				}
				return out, nil
			}
			rs, err := newFullReadClient(t, f).FullReads(context.Background(), "h", "C1", "S1", ids)
			require.Equal(t, int32(1), seen.Load(), "the corrupted chunk was never requested")
			if err == nil {
				// the ONLY acceptable no-error outcome is a fully correct result
				for i := range ids {
					require.Equal(t, ids[i], rs[i].ID, "envelope mispair at %d WITHOUT an error", i)
					require.NotNil(t, rs[i].Object, "nil object at %d WITHOUT an error", i)
					require.Equal(t, ids[i], rs[i].Object.ID(),
						"CONTENT MISATTRIBUTION at position %d and FullReads returned no error", i)
				}
				t.Fatalf("malformed response accepted without error (result was internally consistent, but the corruption was silently absorbed)")
			}
			require.Nil(t, rs, "a failed FullReads must not return a partial result")
		})
	}
}

// TestFullReadsPropagatesChunkFailure makes chunk k fail while all others
// succeed, for k at the start, middle and end. The error must surface and the
// result must be nil, never a partial slice that reads as complete.
func TestFullReadsPropagatesChunkFailure(t *testing.T) {
	const n = 2000 // 8 chunks at 256
	nChunks := (n + MaxFullReadIDsPerRequest - 1) / MaxFullReadIDsPerRequest
	for _, k := range []int{0, nChunks / 2, nChunks - 1} {
		t.Run(fmt.Sprintf("failing_chunk=%d_of_%d", k, nChunks), func(t *testing.T) {
			ids := seqIDs(n)
			sentinel := errors.New("qa-sentinel-chunk-failure")
			f := &fullReadFetcher{}
			f.fn = func(chunk []strfmt.UUID) ([]Replica, error) {
				idx := -1
				for c := 0; c < n; c += MaxFullReadIDsPerRequest {
					if chunk[0] == ids[c] {
						idx = c / MaxFullReadIDsPerRequest
						break
					}
				}
				if idx == k {
					return nil, sentinel
				}
				out := make([]Replica, len(chunk))
				for i, id := range chunk {
					out[i] = replicaFor(id)
				}
				return out, nil
			}
			rs, err := newFullReadClient(t, f).FullReads(context.Background(), "h", "C1", "S1", ids)
			require.Error(t, err, "a failed chunk must fail the whole read")
			require.ErrorIs(t, err, sentinel, "the underlying error must reach the caller")
			require.Nil(t, rs, "a failed FullReads must return nil, not a partial result")
		})
	}
}

// TestFullReadsLeavesNoZeroValueGaps pins that reassembly never leaves a hole.
// A gap surfaces as a zero-value Replica (nil Object, empty ID) that reads as a
// real entry, and the repairer indexes the result positionally.
func TestFullReadsLeavesNoZeroValueGaps(t *testing.T) {
	for _, n := range []int{257, 600, 1024, 4096} {
		t.Run(fmt.Sprintf("n=%d", n), func(t *testing.T) {
			ids := seqIDs(n)
			f := &fullReadFetcher{fn: func(chunk []strfmt.UUID) ([]Replica, error) {
				out := make([]Replica, len(chunk))
				for i, id := range chunk {
					out[i] = replicaFor(id)
				}
				return out, nil
			}}
			rs, err := newFullReadClient(t, f).FullReads(context.Background(), "h", "C1", "S1", ids)
			require.NoError(t, err)
			for i := range rs {
				require.NotEmpty(t, rs[i].ID, "ZERO-VALUE HOLE at index %d", i)
				require.NotNil(t, rs[i].Object, "ZERO-VALUE HOLE (nil object) at index %d", i)
			}
		})
	}
}

// TestFullReadsSharedResultSliceStress drives the shared result slice from many
// concurrent reads so -race can see an overlapping range write.
func TestFullReadsSharedResultSliceStress(t *testing.T) {
	ids := seqIDs(8192)
	var wg sync.WaitGroup
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			f := &fullReadFetcher{fn: func(chunk []strfmt.UUID) ([]Replica, error) {
				time.Sleep(time.Duration(rand.Intn(200)) * time.Microsecond)
				out := make([]Replica, len(chunk))
				for i, id := range chunk {
					out[i] = replicaFor(id)
				}
				return out, nil
			}}
			logger, _ := test.NewNullLogger()
			rs, err := NewFinderClient(f, logger).FullReads(context.Background(), "h", "C1", "S1", ids)
			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}
			for i := range ids {
				if rs[i].ID != ids[i] || rs[i].Object == nil || rs[i].Object.ID() != ids[i] {
					t.Errorf("misattribution at %d", i)
					return
				}
			}
		}()
	}
	wg.Wait()
}
