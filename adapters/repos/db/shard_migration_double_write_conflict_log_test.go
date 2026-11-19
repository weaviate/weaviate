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
	"fmt"
	"strings"
	"testing"

	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
)

// Only newly conflicting properties are reported, on one bounded line: a
// teardown disarms one property at a time, so reporting the whole set each
// time is quadratic, and a line per property grows with the schema.
func TestOverlayConflictIsReportedOnTheTransition(t *testing.T) {
	props := make([]string, 50)
	forced := map[string]inverted.PropertyOverlay{}
	for i := range props {
		props[i] = fmt.Sprintf("prop_%02d", i)
		forced[props[i]] = inverted.PropertyOverlay{ForceSearchable: true}
	}

	tests := []struct {
		name      string
		then      func(s *Shard, disarmFirst, disarmSecond func(string))
		wantWarns int
	}{
		{
			name: "a disarm that only shrinks the conflict set reports nothing",
			then: func(_ *Shard, disarmFirst, _ func(string)) {
				disarmFirst(props[0])
			},
		},
		{
			name: "tearing a registration down property by property reports nothing",
			then: func(_ *Shard, _, disarmSecond func(string)) {
				for _, prop := range props {
					disarmSecond(prop)
				}
			},
		},
		{
			name: "a conflict that goes away and comes back is reported again",
			then: func(s *Shard, _, disarmSecond func(string)) {
				disarmSecond(props[0])
				s.registerDoubleWriteWithScope(props[:1], nil, noopMirrorCallbacks)
			},
			wantWarns: 1,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			logger, hook := logrustest.NewNullLogger()
			s := &Shard{index: &Index{logger: logger}}
			warns := func() int {
				n := 0
				for _, entry := range hook.AllEntries() {
					if strings.Contains(entry.Message, "different analyzer overlays") {
						n++
					}
				}
				return n
			}

			disarmFirst := s.registerDoubleWriteWithScope(props, forced, noopMirrorCallbacks)
			require.Zero(t, warns(), "fixture: one registration cannot conflict with itself")

			disarmSecond := s.registerDoubleWriteWithScope(props, nil, noopMirrorCallbacks)
			require.Equal(t, 1, warns(),
				"one line for the transition, whatever the property count")
			require.Equal(t, len(props), hook.LastEntry().Data["property_count"],
				"the line names how many properties conflict")
			reported, ok := hook.LastEntry().Data["props"].([]string)
			require.True(t, ok, "the line carries the property names it counted")
			require.Len(t, reported, maxReportedErrors+1,
				"the capped names plus the one entry that says how many are unaccounted for")

			hook.Reset()
			test.then(s, disarmFirst, disarmSecond)
			require.Equal(t, test.wantWarns, warns())
		})
	}
}

func noopMirrorCallbacks(map[string]struct{}) (onAddToPropertyValueIndex, onDeleteFromPropertyValueIndex) {
	noop := func(_ *Shard, _ uint64, _ *inverted.Property) error { return nil }
	return noop, noop
}
