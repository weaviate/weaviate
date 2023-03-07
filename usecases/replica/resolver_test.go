//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package replica

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestResolver(t *testing.T) {
	ss := map[string][]string{
		"S0": {},
		"S1": {"A", "B", "C"},
		"S2": {"D", "E"},
		"S3": {"A", "B", "C", "D", "E"},
		"S4": {"D", "E", "F"},
		"S5": {"A", "B", "C", "D", "E", "F"},
	}

	nr := newFakeNodeResolver([]string{"A", "B", "C"})
	r := resolver{
		nodeResolver: nr,
		schema:       newFakeShardingState(ss, nr),
	}
	t.Run("ShardingState", func(t *testing.T) {
		_, err := r.State("Sx", One)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "sharding state")
	})
	t.Run("ALL", func(t *testing.T) {
		got, err := r.State("S1", All)
		assert.Nil(t, err)
		want := rState{All, len(ss["S1"]), ss["S1"], nil}
		assert.Equal(t, want, got)
	})
	t.Run("Quorum", func(t *testing.T) {
		got, err := r.State("S3", Quorum)
		assert.Nil(t, err)
		want := rState{Quorum, len(ss["S1"]), ss["S1"], ss["S2"]}
		assert.Equal(t, want, got)
		_, err = got.ConsistencyLevel(All)
		assert.ErrorIs(t, err, errUnresolvedName)
		_, err = got.ConsistencyLevel(Quorum)
		assert.Nil(t, err)
		_, err = got.ConsistencyLevel(One)
		assert.Nil(t, err)
	})
	t.Run("NoQuorum", func(t *testing.T) {
		got, err := r.State("S5", Quorum)
		assert.ErrorIs(t, err, errUnresolvedName)
		want := rState{Quorum, 0, ss["S1"], ss["S4"]}
		assert.Equal(t, want, got)
		_, err = got.ConsistencyLevel(All)
		assert.ErrorIs(t, err, errUnresolvedName)
		_, err = got.ConsistencyLevel(Quorum)
		assert.ErrorIs(t, err, errUnresolvedName)
		_, err = got.ConsistencyLevel(One)
		assert.Nil(t, err)
	})
}
