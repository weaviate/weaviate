//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
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
	assertSameHosts := func(want, got rState, thisNode string) {
		t.Helper()
		if thisNode != "" {
			assert.Equal(t, thisNode, got.Hosts[0])
		}
		assert.ElementsMatch(t, want.Hosts, got.Hosts, "match Hosts")
		assert.Equal(t, want.NodeMap, got.NodeMap, "match NodeMap")
		assert.Equal(t, want.CLevel, got.CLevel, "match CLevel")
		assert.Equal(t, want.Level, got.Level, "match Level")
	}

	nr := newFakeNodeResolver([]string{"A", "B", "C"})
	r := resolver{
		nodeResolver: nr,
		Class:        "C",
		NodeName:     "A",
		Schema:       newFakeShardingState("A", ss, nr),
	}
	t.Run("ShardingState", func(t *testing.T) {
		_, err := r.State("Sx", One, "")
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "sharding state")
	})
	t.Run("ALL", func(t *testing.T) {
		r := resolver{
			nodeResolver: nr,
			Class:        "C",
			NodeName:     "B",
			Schema:       newFakeShardingState("B", ss, nr),
		}
		got, err := r.State("S1", All, "")
		assert.Nil(t, err)
		m := make(map[string]string, len(ss["S1"]))
		for _, k := range ss["S1"] {
			m[k] = nr.hosts[k]
		}
		want := rState{All, len(ss["S1"]), ss["S1"], m}
		assertSameHosts(want, got, "B")
	})

	t.Run("ALLWithDirectCandidate", func(t *testing.T) {
		got, err := r.State("S1", All, "B")
		assert.Nil(t, err)
		m := make(map[string]string, len(ss["S1"]))
		for _, k := range ss["S1"] {
			m[k] = nr.hosts[k]
		}
		want := rState{All, len(ss["S1"]), ss["S1"], m}
		assertSameHosts(want, got, "B")
	})
	t.Run("Quorum", func(t *testing.T) {
		got, err := r.State("S3", Quorum, "")
		assert.Nil(t, err)

		m := make(map[string]string, len(ss["S1"]))
		for _, k := range ss["S3"] {
			m[k] = nr.hosts[k]
		}
		want := rState{Quorum, len(ss["S1"]), ss["S1"], m} // ss["S2"]}
		assertSameHosts(want, got, "A")
		_, err = got.ConsistencyLevel(All)
		assert.ErrorIs(t, err, errUnresolvedName)
		_, err = got.ConsistencyLevel(Quorum)
		assert.Nil(t, err)
		_, err = got.ConsistencyLevel(One)
		assert.Nil(t, err)
	})
	t.Run("NoQuorum", func(t *testing.T) {
		got, err := r.State("S5", Quorum, "")
		assert.ErrorIs(t, err, errUnresolvedName)
		m := make(map[string]string, len(ss["S1"]))
		for _, k := range ss["S5"] {
			m[k] = nr.hosts[k]
		}
		want := rState{Quorum, 0, ss["S1"], m} // ss["S4"]}
		assertSameHosts(want, got, "A")

		_, err = got.ConsistencyLevel(All)
		assert.ErrorIs(t, err, errUnresolvedName)
		_, err = got.ConsistencyLevel(Quorum)
		assert.ErrorIs(t, err, errUnresolvedName)
		_, err = got.ConsistencyLevel(One)
		assert.Nil(t, err)
	})
}
