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
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestLogThrottle(t *testing.T) {
	type call struct {
		key            string
		at             time.Duration
		wantOK         bool
		wantSuppressed int
	}
	tests := []struct {
		name  string
		calls []call
	}{
		{name: "first call logs", calls: []call{{key: "a", wantOK: true}}},
		{
			name: "repeats within the window are suppressed",
			calls: []call{
				{key: "a", wantOK: true},
				{key: "a", at: time.Second},
				{key: "a", at: 59 * time.Second},
			},
		},
		{
			name: "next window logs with the suppressed count",
			calls: []call{
				{key: "a", wantOK: true},
				{key: "a", at: time.Second},
				{key: "a", at: 2 * time.Second},
				{key: "a", at: time.Minute, wantOK: true, wantSuppressed: 2},
				{key: "a", at: 2 * time.Minute, wantOK: true},
			},
		},
		{
			name: "keys are independent",
			calls: []call{
				{key: "a", wantOK: true},
				{key: "b", at: time.Second, wantOK: true},
				{key: "a", at: 2 * time.Second},
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			th := NewLogThrottle(time.Minute)
			start := time.Now()
			for i, c := range tc.calls {
				ok, suppressed := th.allowAt(c.key, start.Add(c.at))
				assert.Equal(t, c.wantOK, ok, "call %d", i)
				assert.Equal(t, c.wantSuppressed, suppressed, "call %d", i)
			}
		})
	}
}

func TestLogThrottlePrunesIdleKeys(t *testing.T) {
	th := NewLogThrottle(time.Minute)
	start := time.Now()
	for i := range logThrottlePruneAt {
		th.allowAt(fmt.Sprint(i), start)
	}
	th.allowAt("fresh", start.Add(time.Minute))
	assert.Len(t, th.keys, 1)
}
