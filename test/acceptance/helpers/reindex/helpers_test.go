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

package reindexhelpers

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAwaitIndexOnNode(t *testing.T) {
	tests := []struct {
		name      string
		responses []string // "status/algorithm"; the last one repeats
		want      IndexWant
		wantPolls int
		wantErr   string
	}{
		{
			name:      "follower catches up after reporting indexing",
			responses: []string{"indexing/", "indexing/", "ready/"},
			want:      IndexWant{Status: "ready"},
			wantPolls: 3,
		},
		{
			name:      "status alone is not enough when an algorithm is wanted",
			responses: []string{"ready/wand", "ready/blockmax"},
			want:      IndexWant{Status: "ready", Algorithm: "blockmax"},
			wantPolls: 2,
		},
		{
			name:      "never catches up",
			responses: []string{"indexing/"},
			want:      IndexWant{Status: "ready"},
			wantErr:   `(last: status="indexing" algorithm="")`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var calls atomic.Int32
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				i := min(int(calls.Add(1))-1, len(tt.responses)-1)
				status, algorithm, _ := strings.Cut(tt.responses[i], "/")
				fmt.Fprintf(w, `{"collection":"C","properties":[{"name":"p","indexes":[{"type":"filterable","status":%q,"algorithm":%q}]}]}`,
					status, algorithm)
			}))
			defer srv.Close()
			uri := strings.TrimPrefix(srv.URL, "http://")

			polls, err := awaitIndexOnNode(uri, "C", "p", "filterable", tt.want, 300*time.Millisecond)
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), uri)
				assert.Contains(t, err.Error(), tt.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.wantPolls, polls)
		})
	}
}
