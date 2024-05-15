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

package backup

import (
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
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
		{in: BackupDescriptor{Classes: []ClassDescriptor{{Name: "data-1"}, {Name: "data-2"}, {Name: "config"}}}, xs: []string{"data-*"}, out: []string{"config"}}, // Test case for wildcard exclusion
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

func TestWildcardFunctionality(t *testing.T) {
	// Test case scenarios for wildcard functionality
	tests := []struct {
		name            string
		classes         []string // Simulated list of class names
		wildcardPattern string   // Wildcard pattern to match class names
		expectedMatch   []string // Expected list of class names matching the pattern
		expectedNoMatch []string // Expected list of class names not matching the pattern
	}{
		{
			name:            "Exact match",
			classes:         []string{"ClassA", "ClassB", "ClassC"},
			wildcardPattern: "ClassB",
			expectedMatch:   []string{"ClassB"},
			expectedNoMatch: []string{"ClassA", "ClassC"},
		},
		{
			name:            "Wildcard match",
			classes:         []string{"ClassA", "ClassB", "ClassC"},
			wildcardPattern: "Class*",
			expectedMatch:   []string{"ClassA", "ClassB", "ClassC"},
			expectedNoMatch: []string{},
		},
		{
			name:            "Pattern match with character wildcard",
			classes:         []string{"ClassA", "ClassB", "ClassC"},
			wildcardPattern: "Class?",
			expectedMatch:   []string{"ClassA", "ClassB", "ClassC"},
			expectedNoMatch: []string{},
		},
		{
			name:            "Pattern match with range wildcard",
			classes:         []string{"Class1", "Class2", "Class3", "OtherClass"},
			wildcardPattern: "Class[1-3]",
			expectedMatch:   []string{"Class1", "Class2", "Class3"},
			expectedNoMatch: []string{"OtherClass"},
		},
	}

	// Iterate over test cases
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Simulate wildcard matching based on the pattern
			matchedClasses := []string{}
			for _, class := range tc.classes {
				// Check if class matches the wildcard pattern
				matched, err := filepath.Match(tc.wildcardPattern, class)
				if err != nil {
					t.Fatalf("Error matching wildcard pattern: %v", err)
				}
				if matched {
					matchedClasses = append(matchedClasses, class)
				}
			}

			// Assert that matched classes match the expected list
			sort.Strings(matchedClasses)
			sort.Strings(tc.expectedMatch)
			assert.Equal(t, tc.expectedMatch, matchedClasses, "Matched classes mismatch")

			// Assert that non-matched classes match the expected list
			notMatchedClasses := []string{}
			for _, class := range tc.classes {
				// Check if class does not match the wildcard pattern
				matched, err := filepath.Match(tc.wildcardPattern, class)
				if err != nil {
					t.Fatalf("Error matching wildcard pattern: %v", err)
				}
				if !matched {
					notMatchedClasses = append(notMatchedClasses, class)
				}
			}
			sort.Strings(notMatchedClasses)
			sort.Strings(tc.expectedNoMatch)
			assert.Equal(t, tc.expectedNoMatch, notMatchedClasses, "Non-matched classes mismatch")
		})
	}
}
