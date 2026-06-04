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
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"

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
