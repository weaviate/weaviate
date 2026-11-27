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

package postingedge

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// What the second phase must leave is checked, and a pass that leaves a
// deleted vector behind is told.
func TestDeleteEliminatedCatches(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	// harm breaks the index and returns what puts it back.
	tests := []struct {
		name  string
		wants string
		harm  func(f *deleteFixture, gone uint64) (undo func())
	}{
		{name: "nothing wrong"},
		{
			name:  "an edge to a deleted vector",
			wants: "still has an edge",
			harm: func(f *deleteFixture, gone uint64) func() {
				for p := range f.s.st.postings.lists {
					if l := &f.s.st.postings.lists[p]; len(l.edges) > 0 {
						was := l.edges[0]
						l.edges[0] = uint32(gone)
						return func() { l.edges[0] = was }
					}
				}
				return func() {}
			},
		},
		{
			name:  "a deleted vector in a posting",
			wants: "still holds",
			harm: func(f *deleteFixture, gone uint64) func() {
				for p := range f.s.st.postings.lists {
					if l := &f.s.st.postings.lists[p]; len(l.docIDs) > 0 {
						was := l.docIDs[0]
						l.docIDs[0] = gone
						return func() { l.docIDs[0] = was }
					}
				}
				return func() {}
			},
		},
	}
	// Every case harms the same index after the two phases, and puts back
	// what it harmed.
	f := newDeleteFixture(t, 200, 200, 16, false, 120, 20)
	gone := f.removeShare(t, 0.3, 13)
	_, err := f.d.repair(ctx, 1, 4)
	require.NoError(t, err)
	require.NoError(t, f.d.eliminate(ctx, 20, 0))
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.harm != nil {
				undo := tt.harm(f, gone[0])
				defer undo()
			}
			err := f.d.checkEliminated()
			if tt.wants == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, tt.wants)
		})
	}
	require.NoError(t, f.d.checkEliminated(), "every case put back what it harmed")
}
