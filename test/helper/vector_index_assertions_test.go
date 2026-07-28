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

package helper

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
)

// failNowSignal unwinds recordingT.FailNow, which must not return to the
// require call that raised it.
type failNowSignal struct{}

// recordingT stands in for *testing.T so a failing assertion becomes an
// inspectable value instead of failing the test that provoked it.
type recordingT struct {
	failed bool
	log    strings.Builder
}

func (r *recordingT) Errorf(format string, args ...any) {
	r.failed = true
	fmt.Fprintf(&r.log, format+"\n", args...)
}

func (r *recordingT) FailNow() {
	r.failed = true
	panic(failNowSignal{})
}

// rec is a named return so the recover below still yields what was recorded
// before FailNow unwound.
func runDropAssertion(a dropAssertion, vectorNames ...string) (rec *recordingT) {
	rec = &recordingT{}
	defer func() {
		if p := recover(); p != nil {
			if _, ok := p.(failNowSignal); !ok {
				panic(p)
			}
		}
	}()
	a.run(rec, vectorNames...)
	return rec
}

func classGetter(vc map[string]models.VectorConfig) func() *models.Class {
	return func() *models.Class {
		return &models.Class{Class: "SomeClass", VectorConfig: vc}
	}
}

// TestDropAssertionRefusesToPassOnNothing pins the guard against an assertion
// that reports success without having observed anything: every named vector
// absent, or no names at all, used to run zero assertions and pass.
func TestDropAssertionRefusesToPassOnNothing(t *testing.T) {
	dropped := models.VectorConfig{VectorIndexType: "none"}
	live := models.VectorConfig{
		VectorIndexType:   "hnsw",
		VectorIndexConfig: map[string]any{"ef": 64},
	}

	tests := []struct {
		name          string
		getClass      func() *models.Class
		requireMarker bool
		vectorNames   []string
		wantFail      bool
		wantLog       string
	}{
		{
			name:        "no names given",
			getClass:    classGetter(map[string]models.VectorConfig{"vec1": dropped}),
			vectorNames: nil,
			wantFail:    true,
			wantLog:     "the assertion would be vacuous",
		},
		{
			name:        "class not readable",
			getClass:    func() *models.Class { return nil },
			vectorNames: []string{"vec1"},
			wantFail:    true,
			wantLog:     "class must be readable",
		},
		{
			name:          "every name absent, marker required",
			getClass:      classGetter(map[string]models.VectorConfig{"survivor": live}),
			requireMarker: true,
			vectorNames:   []string{"vec1", "vec2"},
			wantFail:      true,
			wantLog:       "still carry the 'none' marker",
		},
		{
			name:          "class empty of every vector, marker required",
			getClass:      classGetter(nil),
			requireMarker: true,
			vectorNames:   []string{"vec1"},
			wantFail:      true,
			wantLog:       "still carry the 'none' marker",
		},
		{
			name:          "one marked one removed, marker required",
			getClass:      classGetter(map[string]models.VectorConfig{"vec1": dropped}),
			requireMarker: true,
			vectorNames:   []string{"vec1", "vec2"},
			wantFail:      true,
			wantLog:       "still carry the 'none' marker",
		},
		{
			name:        "index still live",
			getClass:    classGetter(map[string]models.VectorConfig{"vec1": live}),
			vectorNames: []string{"vec1"},
			wantFail:    true,
			wantLog:     "should be 'none' for dropped vector",
		},
		{
			name:        "marker observed",
			getClass:    classGetter(map[string]models.VectorConfig{"vec1": dropped}),
			vectorNames: []string{"vec1"},
			wantFail:    false,
		},
		{
			name:          "marker observed, marker required",
			getClass:      classGetter(map[string]models.VectorConfig{"vec1": dropped}),
			requireMarker: true,
			vectorNames:   []string{"vec1"},
			wantFail:      false,
		},
		{
			// The finalizer legitimately wins on an empty class, so the lenient
			// helper must keep accepting it: this is the flake the helper exists
			// to absorb, not a vacuous pass.
			name:        "every name absent, marker not required",
			getClass:    classGetter(map[string]models.VectorConfig{"survivor": live}),
			vectorNames: []string{"vec1", "vec2"},
			wantFail:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := runDropAssertion(dropAssertion{
				getClass:      tt.getClass,
				requireMarker: tt.requireMarker,
				waitFor:       50 * time.Millisecond,
				tick:          10 * time.Millisecond,
			}, tt.vectorNames...)

			require.Equalf(t, tt.wantFail, rec.failed,
				"wantFail=%v, log:\n%s", tt.wantFail, rec.log.String())
			if tt.wantLog != "" {
				require.Contains(t, rec.log.String(), tt.wantLog)
			}
		})
	}
}
