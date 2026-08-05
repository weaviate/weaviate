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

package rest

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// stuckOnProber reports a wedged teardown on exactly one collection, and records
// what it was asked, so a lookup that ignores its argument cannot pass.
type stuckOnProber struct {
	stuck string
	asked []string
}

func (p *stuckOnProber) AnyCleanupInProgress() bool { return p.stuck != "" }

func (p *stuckOnProber) AnyCleanupInProgressForCollection(collection string) bool {
	p.asked = append(p.asked, collection)
	return collection == p.stuck
}

// The wait for a wedged worker is capped in minutes, so a collection-blind
// answer refuses restores of every OTHER collection for that whole time. This
// covers the mapping itself, not just that the argument is threaded through.
func TestAnyCleanupInProgressLookupIsScopedByCollection(t *testing.T) {
	tests := []struct {
		name        string
		collections []string
		want        bool
		why         string
	}{
		{
			name:        "the wedged collection",
			collections: []string{"Stuck"},
			want:        true,
			why:         "the collection whose teardown is wedged must still be refused",
		},
		{
			name:        "an unrelated collection",
			collections: []string{"Unrelated"},
			want:        false,
			why:         "another collection's wedged teardown must not refuse this restore",
		},
		{
			name:        "a list containing the wedged one",
			collections: []string{"Unrelated", "Stuck"},
			want:        true,
			why:         "a restore covering the wedged collection is refused whatever else it covers",
		},
		{
			name:        "no class list yet",
			collections: nil,
			want:        true,
			why:         "the meta-not-found arm has no classes, so it must fall back to the blind answer",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			prober := &stuckOnProber{stuck: "Stuck"}
			require.Equal(t, test.want, anyCleanupInProgressLookup(prober)(test.collections), test.why)
			if len(test.collections) > 0 {
				require.NotEmpty(t, prober.asked,
					"a scoped answer has to ask about the collections; an unasked prober means the argument was ignored")
			}
		})
	}
}
