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

package sharding

import (
	"context"
	"fmt"
	"io"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/storobj"
)

func discardLogger() *logrus.Logger {
	l := logrus.New()
	l.Out = io.Discard
	l.Level = logrus.WarnLevel
	return l
}

// objFor builds a one-object result so a replica's answer is distinguishable
// from an empty answer.
func objFor(node string) ReplicasSearchResult {
	return ReplicasSearchResult{
		Objects: []*storobj.Object{{}},
		Scores:  []float32{1},
		Node:    node,
	}
}

// TestQueryAllReplicas_PartialUnion pins the observable behaviour of
// queryAllReplicas when only a subset of a shard's replicas answers.
//
// The production warn
//
//	full replicas search response does not match amount of queries sent: response=2 replicas=3
//
// is emitted from here; this table asserts what the caller actually gets back
// in each of those situations.
func TestQueryAllReplicas_PartialUnion(t *testing.T) {
	type testCase struct {
		name       string
		nodes      []string // replicas of the shard, per sharding state
		resolvable []string // subset of nodes the node resolver can resolve
		failing    map[string]bool
		empty      map[string]bool // answers successfully, but with 0 objects
		localNode  string
		ctx        func() context.Context

		wantErr       bool
		wantRespLen   int
		wantNodes     []string
		wantWarnMatch string // substring expected in a warn line, "" = none asserted
	}

	all := func(ns ...string) []string { return ns }

	cases := []testCase{
		{
			name:        "zero replicas in sharding state",
			nodes:       nil,
			resolvable:  nil,
			localNode:   "N9",
			wantErr:     true,
			wantRespLen: 0,
		},
		{
			name:        "all replicas answer, coordinator is not a replica",
			nodes:       all("N0", "N1", "N2"),
			resolvable:  all("N0", "N1", "N2"),
			localNode:   "N9",
			wantRespLen: 3,
			wantNodes:   all("N0", "N1", "N2"),
		},
		{
			name:        "all replicas answer, coordinator is a replica (skipped)",
			nodes:       all("N0", "N1", "N2"),
			resolvable:  all("N0", "N1", "N2"),
			localNode:   "N0",
			wantRespLen: 2,
			wantNodes:   all("N1", "N2"),
		},
		{
			// This is the observed production case: one replica is restarting
			// and answers 503 "Node not ready".
			name:          "one of three replicas fails (rolling restart)",
			nodes:         all("N0", "N1", "N2"),
			resolvable:    all("N0", "N1", "N2"),
			failing:       map[string]bool{"N1": true},
			localNode:     "N9",
			wantRespLen:   2,
			wantNodes:     all("N0", "N2"),
			wantWarnMatch: "does not match amount of queries sent",
		},
		{
			name:        "two of three replicas fail",
			nodes:       all("N0", "N1", "N2"),
			resolvable:  all("N0", "N1", "N2"),
			failing:     map[string]bool{"N1": true, "N2": true},
			localNode:   "N9",
			wantRespLen: 1,
			wantNodes:   all("N0"),
		},
		{
			name:        "all replicas fail",
			nodes:       all("N0", "N1", "N2"),
			resolvable:  all("N0", "N1", "N2"),
			failing:     map[string]bool{"N0": true, "N1": true, "N2": true},
			localNode:   "N9",
			wantErr:     true,
			wantRespLen: 0,
		},
		{
			name:        "replica answers with an empty result (not an error)",
			nodes:       all("N0", "N1", "N2"),
			resolvable:  all("N0", "N1", "N2"),
			empty:       map[string]bool{"N1": true},
			localNode:   "N9",
			wantRespLen: 3,
			wantNodes:   all("N0", "N1", "N2"),
		},
		{
			name:        "unresolvable replica hostname",
			nodes:       all("N0", "N1", "N2"),
			resolvable:  all("N0", "N2"),
			localNode:   "N9",
			wantRespLen: 2,
			wantNodes:   all("N0", "N2"),
		},
		{
			name:        "coordinator is the only replica",
			nodes:       all("N0"),
			resolvable:  all("N0"),
			localNode:   "N0",
			wantErr:     false,
			wantRespLen: 0,
		},
		{
			name:       "cancelled context",
			nodes:      all("N0", "N1", "N2"),
			resolvable: all("N0", "N1", "N2"),
			localNode:  "N9",
			ctx: func() context.Context {
				c, cancel := context.WithCancel(context.Background())
				cancel()
				return c
			},
			wantErr:     true,
			wantRespLen: 0,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rTable := map[string]string{}
			for _, n := range tc.resolvable {
				rTable[n] = "H" + n
			}
			resolver := fakeNodeResolver{rTable: rTable}
			schema := fakeSchema{nodes: tc.nodes}

			ri := RemoteIndex{
				class:        "C",
				stateGetter:  &schema,
				nodeResolver: &resolver,
			}

			do := func(node, host string) (ReplicasSearchResult, error) {
				if tc.failing[node] {
					return ReplicasSearchResult{}, fmt.Errorf("node %s: 503 Node not ready", node)
				}
				if tc.empty[node] {
					return ReplicasSearchResult{Node: node}, nil
				}
				return objFor(node), nil
			}

			logger := discardLogger()
			var warns []string
			logger.AddHook(&collectHook{out: &warns})

			ctx := context.Background()
			if tc.ctx != nil {
				ctx = tc.ctx()
			}

			resp, err := ri.queryAllReplicas(ctx, logger, "S", do, tc.localNode)

			if tc.wantErr {
				require.Error(t, err)
				require.Empty(t, resp)
				return
			}
			require.NoError(t, err)
			require.Len(t, resp, tc.wantRespLen)

			if tc.wantNodes != nil {
				got := map[string]bool{}
				for _, r := range resp {
					got[r.Node] = true
				}
				for _, n := range tc.wantNodes {
					require.True(t, got[n], "expected an answer from %s, got %v", n, got)
				}
			}

			if tc.wantWarnMatch != "" {
				found := false
				for _, w := range warns {
					if containsStr(w, tc.wantWarnMatch) {
						found = true
					}
				}
				require.True(t, found, "expected a warn containing %q, got %v", tc.wantWarnMatch, warns)
			}
		})
	}
}

// TestQueryAllReplicas_ErrAssignmentRace exercises the unsynchronised write to
// queryAll's named return value `err` from every fan-out goroutine
// (usecases/sharding/remote_index.go:470). Run with -race.
func TestQueryAllReplicas_ErrAssignmentRace(t *testing.T) {
	const n = 16
	rTable := map[string]string{}
	nodes := make([]string, 0, n)
	for i := 0; i < n; i++ {
		name := fmt.Sprintf("N%d", i)
		nodes = append(nodes, name)
		rTable[name] = "H" + name
	}
	resolver := fakeNodeResolver{rTable: rTable}
	schema := fakeSchema{nodes: nodes}
	ri := RemoteIndex{class: "C", stateGetter: &schema, nodeResolver: &resolver}

	do := func(node, host string) (ReplicasSearchResult, error) {
		// Half succeed, half fail, so `err` is written concurrently with both
		// nil and non-nil values.
		if node[len(node)-1]%2 == 0 {
			return ReplicasSearchResult{}, fmt.Errorf("node %s failed", node)
		}
		return objFor(node), nil
	}

	for i := 0; i < 20; i++ {
		_, err := ri.queryAllReplicas(context.Background(), discardLogger(), "S", do, "LOCAL")
		require.NoError(t, err)
	}
}

type collectHook struct{ out *[]string }

func (h *collectHook) Levels() []logrus.Level {
	return []logrus.Level{logrus.WarnLevel, logrus.ErrorLevel}
}

func (h *collectHook) Fire(e *logrus.Entry) error {
	*h.out = append(*h.out, e.Message)
	return nil
}

func containsStr(s, sub string) bool {
	return len(sub) == 0 || (len(s) >= len(sub) && indexOf(s, sub) >= 0)
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}
