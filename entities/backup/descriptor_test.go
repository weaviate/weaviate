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
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExcludeClasses(t *testing.T) {
	tests := []struct {
		in  BackupDescriptor
		xs  []string
		out []string
	}{
		{in: BackupDescriptor{}, xs: []string{}, out: []string{}},
		{in: BackupDescriptor{Classes: []ClassDescriptor{{Name: "a"}}}, xs: []string{}, out: []string{"a"}},
		{in: BackupDescriptor{Classes: []ClassDescriptor{{Name: "a"}}}, xs: []string{"a"}, out: []string{}},
		{in: BackupDescriptor{Classes: []ClassDescriptor{{Name: "1"}, {Name: "2"}, {Name: "3"}, {Name: "4"}}}, xs: []string{"2", "3"}, out: []string{"1", "4"}},
		{in: BackupDescriptor{Classes: []ClassDescriptor{{Name: "1"}, {Name: "2"}, {Name: "3"}}}, xs: []string{"1", "3"}, out: []string{"2"}},

		// {in: []BackupDescriptor{"1", "2", "3", "4"}, xs: []string{"2", "3"}, out: []string{"1", "4"}},
		// {in: []BackupDescriptor{"1", "2", "3"}, xs: []string{"1", "3"}, out: []string{"2"}},
	}
	for _, tc := range tests {
		tc.in.Exclude(tc.xs)
		lst := tc.in.List()
		assert.Equal(t, tc.out, lst)
	}
}

func TestIncludeClasses(t *testing.T) {
	tests := []struct {
		in  BackupDescriptor
		xs  []string
		out []string
	}{
		{in: BackupDescriptor{}, xs: []string{}, out: []string{}},
		{in: BackupDescriptor{Classes: []ClassDescriptor{{Name: "a"}}}, xs: []string{}, out: []string{"a"}},
		{in: BackupDescriptor{Classes: []ClassDescriptor{{Name: "a"}}}, xs: []string{"a"}, out: []string{"a"}},
		{in: BackupDescriptor{Classes: []ClassDescriptor{{Name: "1"}, {Name: "2"}, {Name: "3"}, {Name: "4"}}}, xs: []string{"2", "3"}, out: []string{"2", "3"}},
		{in: BackupDescriptor{Classes: []ClassDescriptor{{Name: "1"}, {Name: "2"}, {Name: "3"}}}, xs: []string{"1", "3"}, out: []string{"1", "3"}},
	}
	for _, tc := range tests {
		tc.in.Include(tc.xs)
		lst := tc.in.List()
		assert.Equal(t, tc.out, lst)
	}
}

func TestAllExist(t *testing.T) {
	x := BackupDescriptor{Classes: []ClassDescriptor{{Name: "a"}}}
	if y := x.AllExist(nil); y != "" {
		t.Errorf("x.AllExists(nil) got=%v want=%v", y, "")
	}
	if y := x.AllExist([]string{"a"}); y != "" {
		t.Errorf("x.AllExists(['a']) got=%v want=%v", y, "")
	}
	if y := x.AllExist([]string{"b"}); y != "b" {
		t.Errorf("x.AllExists(['a']) got=%v want=%v", y, "b")
	}
}

func TestValidateBackup(t *testing.T) {
	timept := time.Now().UTC()
	bytes := []byte("hello")
	tests := []struct {
		desc      BackupDescriptor
		successV1 bool
		successV2 bool
	}{
		// first level check
		{desc: BackupDescriptor{}},
		{desc: BackupDescriptor{ID: "1"}},
		{desc: BackupDescriptor{ID: "1", Version: "1"}},
		{desc: BackupDescriptor{ID: "1", Version: "1", ServerVersion: "1"}},
		{
			desc:      BackupDescriptor{ID: "1", Version: "1", ServerVersion: "1", StartedAt: timept},
			successV1: true, successV2: true,
		},
		{desc: BackupDescriptor{ID: "1", Version: "1", ServerVersion: "1", StartedAt: timept, Error: "err"}},
		{desc: BackupDescriptor{
			ID: "1", Version: "1", ServerVersion: "1", StartedAt: timept,
			Classes: []ClassDescriptor{{}},
		}},
		{desc: BackupDescriptor{
			ID: "1", Version: "1", ServerVersion: "1", StartedAt: timept,
			Classes: []ClassDescriptor{{Name: "n"}},
		}},
		{desc: BackupDescriptor{
			ID: "1", Version: "1", ServerVersion: "1", StartedAt: timept,
			Classes: []ClassDescriptor{{Name: "n", Schema: bytes}},
		}},
		{desc: BackupDescriptor{
			ID: "1", Version: "1", ServerVersion: "1", StartedAt: timept,
			Classes: []ClassDescriptor{{Name: "n", Schema: bytes, ShardingState: bytes}},
		}, successV1: true, successV2: true},
		{desc: BackupDescriptor{
			ID: "1", Version: "1", ServerVersion: "1", StartedAt: timept,
			Classes: []ClassDescriptor{{
				Name: "n", Schema: bytes, ShardingState: bytes,
				Shards: []*ShardDescriptor{{Name: ""}},
			}},
		}},
		{desc: BackupDescriptor{
			ID: "1", Version: "1", ServerVersion: "1", StartedAt: timept,
			Classes: []ClassDescriptor{{
				Name: "n", Schema: bytes, ShardingState: bytes,
				Shards: []*ShardDescriptor{{Name: "n", Node: ""}},
			}},
		}},
		{desc: BackupDescriptor{
			ID: "1", Version: "1", ServerVersion: "1", StartedAt: timept,
			Classes: []ClassDescriptor{{
				Name: "n", Schema: bytes, ShardingState: bytes,
				Shards: []*ShardDescriptor{{Name: "n", Node: "n"}},
			}},
		}, successV2: true},
		{desc: BackupDescriptor{
			ID: "1", Version: "1", ServerVersion: "1", StartedAt: timept,
			Classes: []ClassDescriptor{{
				Name: "n", Schema: bytes, ShardingState: bytes,
				Shards: []*ShardDescriptor{{
					Name: "n", Node: "n",
					PropLengthTrackerPath: "n", DocIDCounterPath: "n", ShardVersionPath: "n",
				}},
			}},
		}, successV1: true, successV2: true},
		{desc: BackupDescriptor{
			ID: "1", Version: "1", ServerVersion: "1", StartedAt: timept,
			Classes: []ClassDescriptor{{
				Name: "n", Schema: bytes, ShardingState: bytes,
				Shards: []*ShardDescriptor{{
					Name: "n", Node: "n",
					PropLengthTrackerPath: "n", DocIDCounterPath: "n", ShardVersionPath: "n",
					Files: []string{"file"},
				}},
			}},
		}, successV2: true},
		{desc: BackupDescriptor{
			ID: "1", Version: "1", ServerVersion: "1", StartedAt: timept,
			Classes: []ClassDescriptor{{
				Name: "n", Schema: bytes, ShardingState: bytes,
				Shards: []*ShardDescriptor{{
					Name: "n", Node: "n",
					PropLengthTrackerPath: "n", DocIDCounterPath: "n", ShardVersionPath: "n",
					DocIDCounter: bytes, Files: []string{"file"},
				}},
			}},
		}, successV2: true},
		{desc: BackupDescriptor{
			ID: "1", Version: "1", ServerVersion: "1", StartedAt: timept,
			Classes: []ClassDescriptor{{
				Name: "n", Schema: bytes, ShardingState: bytes,
				Shards: []*ShardDescriptor{{
					Name: "n", Node: "n",
					PropLengthTrackerPath: "n", DocIDCounterPath: "n", ShardVersionPath: "n",
					DocIDCounter: bytes, Version: bytes, PropLengthTracker: bytes, Files: []string{""},
				}},
			}},
		}, successV2: true},
		{desc: BackupDescriptor{
			ID: "1", Version: "1", ServerVersion: "1", StartedAt: timept,
			Classes: []ClassDescriptor{{
				Name: "n", Schema: bytes, ShardingState: bytes,
				Shards: []*ShardDescriptor{{
					Name: "n", Node: "n",
					PropLengthTrackerPath: "n", DocIDCounterPath: "n", ShardVersionPath: "n",
					DocIDCounter: bytes, Version: bytes, PropLengthTracker: bytes, Files: []string{"file"},
				}},
			}},
		}, successV1: true, successV2: true},
	}
	for i, tc := range tests {
		err := tc.desc.Validate(false)
		if got := err == nil; got != tc.successV1 {
			t.Errorf("%d. validate(%+v): want=%v got=%v err=%v", i, tc.desc, tc.successV1, got, err)
		}
		err = tc.desc.Validate(true)
		if got := err == nil; got != tc.successV2 {
			t.Errorf("%d. validate(%+v): want=%v got=%v err=%v", i, tc.desc, tc.successV1, got, err)
		}
	}
}

func TestBackwardCompatibility(t *testing.T) {
	timept := time.Now().UTC()
	tests := []struct {
		desc    BackupDescriptor
		success bool
	}{
		// first level check
		{desc: BackupDescriptor{}},
		{desc: BackupDescriptor{ID: "1"}},
		{desc: BackupDescriptor{ID: "1", Version: "1"}},
		{desc: BackupDescriptor{ID: "1", Version: "1", ServerVersion: "1"}},
		{desc: BackupDescriptor{ID: "1", Version: "1", ServerVersion: "1", StartedAt: timept}},
		{desc: BackupDescriptor{ID: "1", Version: "1", ServerVersion: "1", StartedAt: timept, Error: "err"}},
		{desc: BackupDescriptor{
			ID: "1", Version: "1", ServerVersion: "1", StartedAt: timept,
			Classes: []ClassDescriptor{{
				Name:   "n",
				Shards: []*ShardDescriptor{{Name: "n", Node: ""}},
			}},
		}},
		{desc: BackupDescriptor{
			ID: "1", Version: "1", ServerVersion: "1", StartedAt: timept,
			Classes: []ClassDescriptor{{
				Name: "n",
				Shards: []*ShardDescriptor{{
					Name: "n", Node: "n",
				}},
			}},
		}, success: true},
	}
	for i, tc := range tests {
		desc := tc.desc.ToDistributed()
		err := desc.Validate()
		if got := err == nil; got != tc.success {
			t.Errorf("%d. validate(%+v): want=%v got=%v err=%v", i, tc.desc, tc.success, got, err)
		}
	}
}

func TestDistributedBackup(t *testing.T) {
	d := DistributedBackupDescriptor{
		Nodes: map[string]*NodeDescriptor{
			"N1": {Classes: []string{"1", "2"}},
			"N2": {Classes: []string{"3", "4"}},
		},
	}
	if n := d.Len(); n != 2 {
		t.Errorf("#nodes got:%v want:%v", n, 2)
	}
	if n := d.Count(); n != 4 {
		t.Errorf("#classes got:%v want:%v", n, 4)
	}
	d.Exclude([]string{"3", "4"})
	d.RemoveEmpty()
	if n := d.Len(); n != 1 {
		t.Errorf("#nodes got:%v want:%v", n, 2)
	}
	if n := d.Count(); n != 2 {
		t.Errorf("#classes got:%v want:%v", n, 4)
	}
}

func TestDistributedBackupExcludeClasses(t *testing.T) {
	tests := []struct {
		in  DistributedBackupDescriptor
		xs  []string
		out []string
	}{
		{
			in:  DistributedBackupDescriptor{},
			xs:  []string{},
			out: []string{},
		},
		{
			in: DistributedBackupDescriptor{
				Nodes: map[string]*NodeDescriptor{
					"N1": {Classes: []string{"a"}},
				},
			},
			xs:  []string{},
			out: []string{"a"},
		},
		{
			in: DistributedBackupDescriptor{
				Nodes: map[string]*NodeDescriptor{
					"N1": {Classes: []string{"a"}},
				},
			},
			xs:  []string{"a"},
			out: []string{},
		},
		{
			in: DistributedBackupDescriptor{
				Nodes: map[string]*NodeDescriptor{
					"N1": {Classes: []string{"1", "2"}},
					"N2": {Classes: []string{"3", "4"}},
				},
			},
			xs:  []string{"2", "3"},
			out: []string{"1", "4"},
		},

		{
			in: DistributedBackupDescriptor{
				Nodes: map[string]*NodeDescriptor{
					"N1": {Classes: []string{"1", "2"}},
					"N2": {Classes: []string{"3"}},
				},
			},
			xs:  []string{"1", "3"},
			out: []string{"2"},
		},
	}

	for _, tc := range tests {
		tc.in.Exclude(tc.xs)
		lst := tc.in.Classes()
		sort.Strings(lst)
		assert.Equal(t, tc.out, lst)
	}
}

func TestDistributedBackupIncludeClasses(t *testing.T) {
	tests := []struct {
		in  DistributedBackupDescriptor
		xs  []string
		out []string
	}{
		{
			in:  DistributedBackupDescriptor{},
			xs:  []string{},
			out: []string{},
		},
		{
			in: DistributedBackupDescriptor{
				Nodes: map[string]*NodeDescriptor{
					"N1": {Classes: []string{"a"}},
				},
			},
			xs:  []string{},
			out: []string{"a"},
		},
		{
			in: DistributedBackupDescriptor{
				Nodes: map[string]*NodeDescriptor{
					"N1": {Classes: []string{"a"}},
				},
			},
			xs:  []string{"a"},
			out: []string{"a"},
		},
		{
			in: DistributedBackupDescriptor{
				Nodes: map[string]*NodeDescriptor{
					"N1": {Classes: []string{"1", "2"}},
					"N2": {Classes: []string{"3", "4"}},
				},
			},
			xs:  []string{"2", "3"},
			out: []string{"2", "3"},
		},

		{
			in: DistributedBackupDescriptor{
				Nodes: map[string]*NodeDescriptor{
					"N1": {Classes: []string{"1", "2"}},
					"N2": {Classes: []string{"3"}},
				},
			},
			xs:  []string{"1", "3"},
			out: []string{"1", "3"},
		},
	}
	for _, tc := range tests {
		tc.in.Include(tc.xs)
		lst := tc.in.Classes()
		sort.Strings(lst)
		assert.Equal(t, tc.out, lst)
	}
}

func TestDistributedBackupAllExist(t *testing.T) {
	x := DistributedBackupDescriptor{Nodes: map[string]*NodeDescriptor{"N1": {Classes: []string{"a"}}}}
	if y := x.AllExist(nil); y != "" {
		t.Errorf("x.AllExists(nil) got=%v want=%v", y, "")
	}
	if y := x.AllExist([]string{"a"}); y != "" {
		t.Errorf("x.AllExists(['a']) got=%v want=%v", y, "")
	}
	if y := x.AllExist([]string{"b"}); y != "b" {
		t.Errorf("x.AllExists(['a']) got=%v want=%v", y, "b")
	}
}

func TestDistributedBackupValidate(t *testing.T) {
	timept := time.Now().UTC()
	tests := []struct {
		desc    DistributedBackupDescriptor
		success bool
	}{
		// first level check
		{desc: DistributedBackupDescriptor{}},
		{desc: DistributedBackupDescriptor{ID: "1"}},
		{desc: DistributedBackupDescriptor{ID: "1", Version: "1"}},
		{desc: DistributedBackupDescriptor{ID: "1", Version: "1", ServerVersion: "1"}},
		{desc: DistributedBackupDescriptor{ID: "1", Version: "1", ServerVersion: "1", StartedAt: timept, Error: "err"}},
		{desc: DistributedBackupDescriptor{ID: "1", Version: "1", ServerVersion: "1", StartedAt: timept}},
		{desc: DistributedBackupDescriptor{
			ID: "1", Version: "1", ServerVersion: "1", StartedAt: timept,
			Nodes: map[string]*NodeDescriptor{"N": {}},
		}, success: true},
	}
	for i, tc := range tests {
		err := tc.desc.Validate()
		if got := err == nil; got != tc.success {
			t.Errorf("%d. validate(%+v): want=%v got=%v err=%v", i, tc.desc, tc.success, got, err)
		}
	}
}

func TestTestDistributedBackupResetStatus(t *testing.T) {
	begin := time.Now().UTC().Add(-2)
	desc := DistributedBackupDescriptor{
		StartedAt:     begin,
		CompletedAt:   begin.Add(2),
		ID:            "1",
		Version:       "1",
		ServerVersion: "1",
		Nodes: map[string]*NodeDescriptor{
			"1": {},
			"2": {Status: Success},
			"3": {Error: "error"},
		},
		Error: "error",
	}

	desc.ResetStatus()
	if !desc.StartedAt.After(begin) {
		t.Fatalf("!desc.StartedAt.After(begin)")
	}
	want := DistributedBackupDescriptor{
		StartedAt:     desc.StartedAt,
		ID:            "1",
		Version:       "1",
		ServerVersion: "1",
		Nodes: map[string]*NodeDescriptor{
			"1": {Status: Started},
			"2": {Status: Started},
			"3": {Status: Started, Error: ""},
		},
		Status: Started,
	}
	assert.Equal(t, want, desc)
}

func TestShardDescriptorClear(t *testing.T) {
	s := ShardDescriptor{
		Name:                  "name",
		Node:                  "node",
		PropLengthTrackerPath: "a/b",
		PropLengthTracker:     []byte{1},
		DocIDCounterPath:      "a/c",
		DocIDCounter:          []byte{2},
		ShardVersionPath:      "a/d",
		Version:               []byte{3},
		Files:                 []string{"file"},
		Chunk:                 1,
	}

	want := ShardDescriptor{
		Name:  "name",
		Node:  "node",
		Files: []string{"file"},
		Chunk: 1,
	}
	s.ClearTemporary()
	assert.Equal(t, want, s)
}

func TestSharedBackupState(t *testing.T) {
	s := NewSharedBackupState(2)

	s.AddShard("node1", "class2", "shard1", []string{"node1", "node2", "node3"})
	s.AddShard("node2", "class2", "shard2", []string{"node1", "node2", "node3"})
	s.AddShard("node3", "class2", "shard3", []string{"node1", "node2", "node3"})
	s.AddShard("node1", "class2", "shard4", []string{"node1", "node2", "node3"})
	s.AddShard("node1", "class1", "shard1", []string{"node1", "node2", "node3"})

	tests := []struct {
		node           string
		expectedShards []string
	}{
		{node: "node1", expectedShards: []string{"shard2", "shard3"}},
		{node: "node2", expectedShards: []string{"shard1", "shard3", "shard4"}},
		{node: "node0", expectedShards: []string{"shard1", "shard2", "shard3", "shard4"}},
	}

	for _, tt := range tests {
		t.Run(tt.node, func(t *testing.T) {
			shardsToSkip := s.ShardsToSkipForNodeAndClass(tt.node, "class2")
			require.ElementsMatch(t, shardsToSkip, tt.expectedShards)
		})
	}
}

func TestSharedBackupLocations(t *testing.T) {
	s := make(SharedBackupLocations, 0)
	s = append(s, SharedBackupLocation{
		StoredOnNode:   "node1",
		Class:          "class1",
		Shard:          "shard1",
		Chunk:          1,
		BelongsToNodes: []string{"node1", "node2", "node3"},
	})
	s = append(s, SharedBackupLocation{
		StoredOnNode:   "node1",
		Class:          "class1",
		Shard:          "shard2",
		Chunk:          1,
		BelongsToNodes: []string{"node1", "node2", "node3"},
	})
	s = append(s, SharedBackupLocation{
		StoredOnNode:   "node1",
		Class:          "class1",
		Shard:          "shard3",
		Chunk:          1,
		BelongsToNodes: []string{"node1", "node2", "node3"},
	})
	s = append(s, SharedBackupLocation{
		StoredOnNode:   "node2",
		Class:          "class1",
		Shard:          "shard3",
		Chunk:          2,
		BelongsToNodes: []string{"node1", "node2", "node3"},
	})
	s = append(s, SharedBackupLocation{
		StoredOnNode:   "node2",
		Class:          "class1",
		Shard:          "shard4",
		Chunk:          3,
		BelongsToNodes: []string{"node1", "node2", "node3"},
	})
	chunksPerNode := s.SharedChunksPerNode()

	expectedChunksPerNode := map[string]map[int32][]string{
		"node1": {
			1: {"shard1", "shard2", "shard3"},
		},
		"node2": {
			2: {"shard3"},
			3: {"shard4"},
		},
	}
	require.Equal(t, expectedChunksPerNode, chunksPerNode)
}

func TestDescriptorToSharedLocation(t *testing.T) {
	desc := BackupDescriptor{
		ID: "backup1",
		Classes: []ClassDescriptor{
			{
				Name: "Class1",
				Chunks: map[int32][]string{
					1: {"shard1", "shard2"},
					2: {"shard3"},
				},
			},
			{
				Name: "Class2",
				Chunks: map[int32][]string{
					1: {"shardA"},
				},
			},
		},
	}

	state := SharedBackupState{
		ClassShardsToNodes: map[string]map[string][]string{
			"Class1": {
				"shard1": {"node1", "node2"},
				"shard2": {"node1"},
				"shard3": {"node1", "node3"},
			},
			"Class2": {
				"shardA": {"node1", "node2", "node3"},
			},
		},
	}

	location := desc.ToSharedBackupLocation("node1", state)

	expected := SharedBackupLocations{
		{
			Class:          "Class1",
			Shard:          "shard1",
			Chunk:          1,
			StoredOnNode:   "node1",
			BelongsToNodes: []string{"node1", "node2"},
		},
		{
			Class:          "Class1",
			Shard:          "shard2",
			Chunk:          1,
			StoredOnNode:   "node1",
			BelongsToNodes: []string{"node1"},
		},
		{
			Class:          "Class1",
			Shard:          "shard3",
			Chunk:          2,
			StoredOnNode:   "node1",
			BelongsToNodes: []string{"node1", "node3"},
		},
		{
			Class:          "Class2",
			Shard:          "shardA",
			Chunk:          1,
			StoredOnNode:   "node1",
			BelongsToNodes: []string{"node1", "node2", "node3"},
		},
	}

	require.Equal(t, expected, location)
}

func TestSharedBackupLocationsForNode(t *testing.T) {
	locations := SharedBackupLocations{
		{
			Class:          "Class1",
			Shard:          "shard1",
			Chunk:          1,
			StoredOnNode:   "node1",
			BelongsToNodes: []string{"node1", "node2"},
		},
		{
			Class:          "Class1",
			Shard:          "shard2",
			Chunk:          2,
			StoredOnNode:   "node2",
			BelongsToNodes: []string{"node1", "node2"},
		},
		{
			Class:          "Class1",
			Shard:          "shard3",
			Chunk:          3,
			StoredOnNode:   "node2",
			BelongsToNodes: []string{"node2", "node3"},
		},
		{
			Class:          "Class2",
			Shard:          "shardB",
			Chunk:          2,
			StoredOnNode:   "node1",
			BelongsToNodes: []string{"node3"},
		},
	}

	t.Run("node1 should get chunks stored elsewhere that belong to it", func(t *testing.T) {
		result := locations.ForNode("node1")

		expected := SharedBackupLocations{
			{
				Class:          "Class1",
				Shard:          "shard2",
				Chunk:          2,
				StoredOnNode:   "node2",
				BelongsToNodes: []string{"node1", "node2"},
			},
			{
				Class:          "Class2",
				Shard:          "shardA",
				Chunk:          1,
				StoredOnNode:   "node3",
				BelongsToNodes: []string{"node1", "node2", "node3"},
			},
		}

		require.Equal(t, expected, result)
	})

	t.Run("node4 should get nothing as it doesn't belong to any shards", func(t *testing.T) {
		result := locations.ForNode("node4")

		expected := SharedBackupLocations{}

		require.Equal(t, expected, result)
	})
}

func TestSharedBackupLocationsForClass(t *testing.T) {
	locations := SharedBackupLocations{
		{
			Class:          "Class1",
			Shard:          "shard1",
			Chunk:          1,
			StoredOnNode:   "node1",
			BelongsToNodes: []string{"node1", "node2"},
		},
		{
			Class:          "Class1",
			Shard:          "shard2",
			Chunk:          2,
			StoredOnNode:   "node2",
			BelongsToNodes: []string{"node1", "node2"},
		},
		{
			Class:          "Class3",
			Shard:          "shardX",
			Chunk:          1,
			StoredOnNode:   "node2",
			BelongsToNodes: []string{"node2"},
		},
	}

	t.Run("filter by Class1", func(t *testing.T) {
		result := locations.ForClass("Class1")

		expected := SharedBackupLocations{
			{
				Class:          "Class1",
				Shard:          "shard1",
				Chunk:          1,
				StoredOnNode:   "node1",
				BelongsToNodes: []string{"node1", "node2"},
			},
			{
				Class:          "Class1",
				Shard:          "shard2",
				Chunk:          2,
				StoredOnNode:   "node2",
				BelongsToNodes: []string{"node1", "node2"},
			},
		}

		require.Equal(t, expected, result)
	})

	t.Run("filter empty locations", func(t *testing.T) {
		emptyLocations := SharedBackupLocations{}
		result := emptyLocations.ForClass("Class1")

		expected := SharedBackupLocations{}

		require.Equal(t, expected, result)
	})
}
