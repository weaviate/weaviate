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

package backup

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"runtime"
	"testing"
	"testing/iotest"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/backup"
)

func TestFetchBackupDescriptors(t *testing.T) {
	logger, _ := test.NewNullLogger()

	// key builds a well-formed descriptor path for a given backup ID.
	key := func(id string) string { return id + "/" + GlobalBackupFile }

	makeJSON := func(id string) []byte {
		b, _ := json.Marshal(backup.DistributedBackupDescriptor{ID: id})
		return b
	}

	tests := []struct {
		name    string
		keys    []string
		fetch   func(ctx context.Context, key string) ([]byte, error)
		wantIDs []string
		wantErr string
	}{
		{
			name:    "empty keys returns nil",
			keys:    nil,
			fetch:   nil,
			wantIDs: nil,
		},
		{
			name: "success single",
			keys: []string{key("a")},
			fetch: func(_ context.Context, k string) ([]byte, error) {
				return makeJSON("a"), nil
			},
			wantIDs: []string{"a"},
		},
		{
			name: "success multiple",
			keys: []string{key("x"), key("y"), key("z")},
			fetch: func(_ context.Context, k string) ([]byte, error) {
				// derive ID from path prefix before the slash
				id := k[:len(k)-len("/"+GlobalBackupFile)]
				return makeJSON(id), nil
			},
			wantIDs: []string{"x", "y", "z"},
		},
		{
			name:    "non-matching keys are skipped",
			keys:    []string{"not-a-backup-file", key("a") + ".bak"},
			fetch:   func(_ context.Context, _ string) ([]byte, error) { return makeJSON("a"), nil },
			wantIDs: nil,
		},
		{
			name: "not found errors are skipped; partial",
			keys: []string{key("missing"), key("found")},
			fetch: func(_ context.Context, k string) ([]byte, error) {
				if k == key("missing") {
					return nil, backup.NewErrNotFound(errors.New("not found"))
				}
				id := k[:len(k)-len("/"+GlobalBackupFile)]
				return makeJSON(id), nil
			},
			wantIDs: []string{"found"},
		},
		{
			name: "not found errors are skipped; all",
			keys: []string{key("missing")},
			fetch: func(_ context.Context, k string) ([]byte, error) {
				return nil, backup.NewErrNotFound(errors.New("not found"))
			},
			wantIDs: nil,
		},
		{
			name: "fetch error propagates",
			keys: []string{key("bad")},
			fetch: func(_ context.Context, _ string) ([]byte, error) {
				return nil, fmt.Errorf("storage unavailable")
			},
			wantErr: "storage unavailable",
		},
		{
			name: "invalid json returns unmarshal error",
			keys: []string{key("bad-json")},
			fetch: func(_ context.Context, k string) ([]byte, error) {
				return []byte("not-json"), nil
			},
			wantErr: `unmarshal descriptor`,
		},
		{
			name: "cancelled context propagates",
			keys: []string{key("a"), key("b"), key("c")},
			fetch: func(ctx context.Context, _ string) ([]byte, error) {
				return nil, ctx.Err()
			},
			wantErr: "context canceled",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			if tc.name == "cancelled context propagates" {
				cancel()
			} else {
				defer cancel()
			}

			got, err := FetchBackupDescriptors(ctx, logger, tc.keys, tc.fetch)

			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			if tc.wantIDs == nil {
				assert.Nil(t, got)
				return
			}
			gotIDs := make([]string, len(got))
			for i, d := range got {
				gotIDs[i] = d.ID
			}
			assert.ElementsMatch(t, tc.wantIDs, gotIDs)
		})
	}

	t.Run("fetch error wrapping includes key", func(t *testing.T) {
		k := key("backup1")
		_, err := FetchBackupDescriptors(context.Background(), logger, []string{k}, func(_ context.Context, _ string) ([]byte, error) {
			return nil, errors.New("boom")
		})
		assert.ErrorContains(t, err, k)
		assert.ErrorContains(t, err, "boom")
	})
}

func TestReadAllSized(t *testing.T) {
	payload := bytes.Repeat([]byte{'x'}, 1<<20)
	abovePresize := bytes.Repeat([]byte{'x'}, maxPresize+1<<20)
	errRead := errors.New("connection reset")

	tests := []struct {
		name     string
		r        io.Reader
		size     int64
		want     []byte
		maxAlloc uint64
		tightCap bool
		wantErr  error
	}{
		{name: "known size allocates once", r: bytes.NewReader(payload), size: int64(len(payload)), want: payload, maxAlloc: uint64(len(payload)) * 3 / 2, tightCap: true},
		{name: "unknown size reads everything", r: bytes.NewReader(payload), size: -1, want: payload, tightCap: true},
		{name: "size below the object length reads everything", r: bytes.NewReader(payload), size: 10, want: payload},
		{name: "size above the cap allocates only the cap", r: bytes.NewReader(payload), size: 4 * maxPresize, want: payload, maxAlloc: maxPresize * 3 / 2},
		{
			name:     "object above the cap grows to its size, not to double the cap",
			r:        bytes.NewReader(abovePresize),
			size:     int64(len(abovePresize)),
			want:     abovePresize,
			maxAlloc: uint64(maxPresize+len(abovePresize)) * 11 / 10,
			tightCap: true,
		},
		{
			name: "size overstated near MaxInt64 grows without overflowing",
			r:    bytes.NewReader(abovePresize),
			size: math.MaxInt64,
			want: abovePresize,
		},
		{
			name:    "read error is returned",
			r:       io.MultiReader(bytes.NewReader(payload[:100]), iotest.ErrReader(errRead)),
			size:    int64(len(payload)),
			wantErr: errRead,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var before, after runtime.MemStats
			runtime.ReadMemStats(&before)
			// The struct hides bytes.Reader's WriteTo, which a network body does not have.
			got, err := ReadAllSized(struct{ io.Reader }{tt.r}, tt.size)
			runtime.ReadMemStats(&after)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
			if tt.tightCap {
				assert.LessOrEqual(t, cap(got), len(got)*5/4, "returned capacity")
			}
			if tt.maxAlloc > 0 {
				assert.Less(t, after.TotalAlloc-before.TotalAlloc, tt.maxAlloc, "bytes allocated")
			}
		})
	}
}
