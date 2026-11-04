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

package selfrecovery

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"maps"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/sirupsen/logrus"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/handlers/rest/clusterapi/grpc/generated/protocol"
	"github.com/weaviate/weaviate/adapters/repos/db"
	"github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/cluster/replication/copier"
	"github.com/weaviate/weaviate/usecases/namespaces"
	"github.com/weaviate/weaviate/usecases/schema/namespacing"
)

func TestRunOne_NamespaceRefusalLogsInfo(t *testing.T) {
	const (
		waitStem  = "self-recovery waits for its namespace to become active"
		retryStem = "self-recovery register/poll failed; will retry"
	)
	tests := []struct {
		name        string
		dequeued    api.NamespaceState // what the namespace reads when runOne starts
		refusal     error
		wantLevel   logrus.Level
		wantStem    string
		otherStem   string
		wantEvent   string
		wantGiveups float64
	}{
		{"suspended when dequeued", api.NamespaceStateSuspended, nil, logrus.InfoLevel, waitStem, retryStem, "self_recovery.parked", 0},
		{"suspended at registration", api.NamespaceStateActive, namespaces.ErrNamespaceSuspended, logrus.InfoLevel, waitStem, retryStem, "self_recovery.parked", 0},
		{"deleting", api.NamespaceStateDeleting, namespaces.ErrNamespaceDeleting, logrus.WarnLevel, retryStem, waitStem, "self_recovery.started", 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			raft := &stubRaft{}
			if tt.refusal != nil {
				raft.registerErr = fmt.Errorf("apply command: %w", tt.refusal)
			}
			o := newOrchestratorWithDataPeer(t, raft)
			o.namespaceStateForClass = func(string) (api.NamespaceState, error) { return tt.dequeued, nil }
			logger, hook := logrustest.NewNullLogger()
			o.logger = logger

			o.runOne(context.Background(), ShardRef{Collection: "C", Shard: "S"}, false)

			entries := hook.AllEntries()
			require.True(t, slices.ContainsFunc(entries, func(e *logrus.Entry) bool {
				return e.Level == tt.wantLevel && strings.HasPrefix(e.Message, tt.wantStem) && e.Data["event"] == tt.wantEvent
			}), "no %s line %q with event %q", tt.wantLevel, tt.wantStem, tt.wantEvent)
			require.False(t, slices.ContainsFunc(entries, func(e *logrus.Entry) bool {
				return strings.HasPrefix(e.Message, tt.otherStem)
			}), "logged %q", tt.otherStem)
			require.Equal(t, tt.wantGiveups, testutil.ToFloat64(o.metrics.GiveupTotal))
		})
	}
}

// namespaceStates answers each namespace's state. Its read method is the reader the orchestrator polls, and counts its calls.
type namespaceStates struct {
	mu     sync.Mutex
	states map[string]api.NamespaceState
	errs   map[string]error
	reads  map[string]int
}

func newNamespaceStates() *namespaceStates {
	return &namespaceStates{states: map[string]api.NamespaceState{}, errs: map[string]error{}, reads: map[string]int{}}
}

func (s *namespaceStates) set(ns string, state api.NamespaceState) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.states[ns] = state
	delete(s.errs, ns)
}

// fail makes read return err for ns, whose state then answers active.
func (s *namespaceStates) fail(ns string, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.states, ns)
	s.errs[ns] = err
}

// state answers active for a namespace never set.
func (s *namespaceStates) state(ns string) api.NamespaceState {
	s.mu.Lock()
	defer s.mu.Unlock()
	if state, ok := s.states[ns]; ok {
		return state
	}
	return api.NamespaceStateActive
}

func (s *namespaceStates) read(className string) (api.NamespaceState, error) {
	ns := namespacing.NamespaceFromQualified(className)
	s.mu.Lock()
	s.reads[ns]++
	err := s.errs[ns]
	s.mu.Unlock()
	if err != nil {
		return "", err
	}
	return s.state(ns), nil
}

func (s *namespaceStates) readCount(ns string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.reads[ns]
}

// namespaceRefusingRaft refuses a registration while its namespace reads suspended or resuming, as RAFT does.
type namespaceRefusingRaft struct {
	*stubRaft
	states   *namespaceStates
	mu       sync.Mutex
	attempts map[string]int // RegisterSelfRecovery calls by ShardRef.key, refused or not
}

func (r *namespaceRefusingRaft) RegisterSelfRecovery(ctx context.Context, sourceNode, collection, shard, targetNode string) (strfmt.UUID, error) {
	r.mu.Lock()
	r.attempts[ShardRef{Collection: collection, Shard: shard}.key()]++
	r.mu.Unlock()
	switch r.states.state(namespacing.NamespaceFromQualified(collection)) {
	case api.NamespaceStateSuspended:
		return "", fmt.Errorf("apply command: %w", namespaces.ErrNamespaceSuspended)
	case api.NamespaceStateResuming:
		return "", fmt.Errorf("apply command: %w", namespaces.ErrNamespaceResuming)
	default:
		return r.stubRaft.RegisterSelfRecovery(ctx, sourceNode, collection, shard, targetNode)
	}
}

type parkFixture struct {
	o      *Orchestrator
	raft   *namespaceRefusingRaft
	states *namespaceStates

	mu             sync.Mutex
	probes         map[string]int // ProbeShardData calls by ShardRef.key
	suspendOnProbe string         // a namespace the next probe suspends, as if the suspend landed mid-probe
}

// newParkFixture builds a one-worker orchestrator whose peer has data and whose registered ops sit in opState.
func newParkFixture(t *testing.T, opState api.ShardReplicationState) *parkFixture {
	t.Helper()
	inner := &stubRaft{detailsByUUID: map[strfmt.UUID]api.ReplicationDetailsResponse{}}
	inner.registerHook = func(uuid strfmt.UUID, _, _, _, _ string) {
		inner.mu.Lock()
		defer inner.mu.Unlock()
		inner.detailsByUUID[uuid] = api.ReplicationDetailsResponse{Status: api.ReplicationDetailsState{State: string(opState)}}
	}
	states := newNamespaceStates()
	raft := &namespaceRefusingRaft{stubRaft: inner, states: states, attempts: map[string]int{}}
	o := newOrchestratorWithDataPeer(t, raft)
	o.namespaceStateForClass = states.read
	f := &parkFixture{o: o, raft: raft, states: states, probes: map[string]int{}}
	o.clientFactory = func(context.Context, string) (copier.FileReplicationServiceClient, error) {
		return &stubFileReplicationClient{probeShardData: f.probe}, nil
	}
	return f
}

// probe counts the call, suspends suspendOnProbe once, and answers that the peer hosts data.
func (f *parkFixture) probe(_ context.Context, in *protocol.ProbeShardDataRequest) (*protocol.ProbeShardDataResponse, error) {
	f.mu.Lock()
	f.probes[ShardRef{Collection: in.IndexName, Shard: in.ShardName}.key()]++
	ns := f.suspendOnProbe
	f.suspendOnProbe = ""
	f.mu.Unlock()
	if ns != "" {
		f.states.set(ns, api.NamespaceStateSuspended)
	}
	return answerHostedData()
}

func (f *parkFixture) probeCount(ref ShardRef) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.probes[ref.key()]
}

func (f *parkFixture) submit(t *testing.T, ref ShardRef, startedWithoutRaftState bool) {
	t.Helper()
	require.True(t, f.o.Submit(context.Background(), ref, startedWithoutRaftState))
}

func (f *parkFixture) attempts(ref ShardRef) int {
	f.raft.mu.Lock()
	defer f.raft.mu.Unlock()
	return f.raft.attempts[ref.key()]
}

func (f *parkFixture) parked() map[string]submission {
	f.o.queueMu.Lock()
	defer f.o.queueMu.Unlock()
	return maps.Clone(f.o.parked)
}

func (f *parkFixture) idle() bool {
	f.o.queueMu.Lock()
	defer f.o.queueMu.Unlock()
	return f.o.active == 0 && f.o.pending.Len() == 0
}

// waitParked waits until ref made n registration attempts, sits parked, and the worker is idle.
func (f *parkFixture) waitParked(t *testing.T, ref ShardRef, n int) {
	t.Helper()
	require.Eventually(t, func() bool {
		_, ok := f.parked()[ref.key()]
		return f.attempts(ref) == n && ok && f.idle()
	}, 5*time.Second, time.Millisecond, "%s not parked after %d attempts", ref.key(), n)
}

// waitPolls waits until the poll read ns's state n more times.
func (f *parkFixture) waitPolls(t *testing.T, ns string, n int) {
	t.Helper()
	start := f.states.readCount(ns)
	require.Eventually(t, func() bool { return f.states.readCount(ns) >= start+n }, 5*time.Second, time.Millisecond)
}

func (f *parkFixture) waitSucceeded(t *testing.T, n float64) {
	t.Helper()
	require.Eventually(t, func() bool {
		return testutil.ToFloat64(f.o.metrics.CompletedTotal.WithLabelValues("success")) == n
	}, 5*time.Second, time.Millisecond)
}

func (f *parkFixture) started() float64 {
	return testutil.ToFloat64(f.o.metrics.StartedTotal.WithLabelValues("peer1"))
}

func TestOrchestrator_ParksUntilNamespaceActive(t *testing.T) {
	var (
		alphaS1 = ShardRef{Collection: "alpha:Books", Shard: "S1"}
		alphaS2 = ShardRef{Collection: "alpha:Books", Shard: "S2"}
		betaS1  = ShardRef{Collection: "beta:Books", Shard: "S1"}
		gammaS1 = ShardRef{Collection: "gamma:Books", Shard: "S1"}
		waiting = []api.NamespaceState{api.NamespaceStateSuspended, api.NamespaceStateResuming}
	)

	// A shard probed while its namespace waits would be created empty there if no peer held its data.
	t.Run("a shard dequeued while its namespace waits parks without probing and registers once it is active", func(t *testing.T) {
		for _, state := range waiting {
			t.Run(string(state), func(t *testing.T) {
				f := newParkFixture(t, api.READY)
				f.states.set("alpha", state)
				f.submit(t, alphaS1, false)
				f.submit(t, betaS1, false)

				f.waitParked(t, alphaS1, 0)
				f.waitSucceeded(t, 1)
				require.Equal(t, 1, f.attempts(betaS1))
				f.waitPolls(t, "alpha", 2)
				require.Zero(t, f.attempts(alphaS1), "the poll released a shard whose namespace reads %s", state)
				require.Zero(t, f.probeCount(alphaS1), "a parked shard probed its peers")
				require.Zero(t, testutil.ToFloat64(f.o.metrics.GiveupTotal))

				f.states.set("alpha", api.NamespaceStateActive)
				f.waitSucceeded(t, 2)
				require.Equal(t, 1, f.attempts(alphaS1))
				require.Empty(t, f.parked())
				require.Never(t, func() bool { return f.attempts(alphaS1) > 1 }, 2*f.o.parkPollInterval, time.Millisecond)
			})
		}
	})

	t.Run("a registration refused after the dequeue parks, counts no start, and counts one once it registers", func(t *testing.T) {
		f := newParkFixture(t, api.REGISTERED)
		f.suspendOnProbe = "alpha"
		f.submit(t, alphaS1, false)

		f.waitParked(t, alphaS1, 1)
		require.Zero(t, f.started())

		f.states.set("alpha", api.NamespaceStateActive)
		require.Eventually(t, func() bool { return f.started() == 1 }, 5*time.Second, time.Millisecond)
	})

	t.Run("a namespace unknown locally releases its parked shard", func(t *testing.T) {
		f := newParkFixture(t, api.READY)
		f.states.set("alpha", api.NamespaceStateSuspended)
		f.submit(t, alphaS1, false)
		f.waitParked(t, alphaS1, 0)

		f.states.fail("alpha", db.ErrNamespaceUnknownLocally)
		require.Eventually(t, func() bool { return f.attempts(alphaS1) == 1 }, 5*time.Second, time.Millisecond)
	})

	t.Run("each parked shard is released by its own namespace", func(t *testing.T) {
		f := newParkFixture(t, api.READY)
		f.states.set("alpha", api.NamespaceStateSuspended)
		f.states.set("gamma", api.NamespaceStateSuspended)
		f.submit(t, alphaS1, false)
		f.submit(t, gammaS1, false)
		f.waitParked(t, alphaS1, 0)
		f.waitParked(t, gammaS1, 0)

		f.states.set("gamma", api.NamespaceStateDeleting)
		require.Eventually(t, func() bool { return f.attempts(gammaS1) == 1 }, 5*time.Second, time.Millisecond)
		f.waitPolls(t, "alpha", 2)
		require.Zero(t, f.attempts(alphaS1))
	})

	t.Run("a debug restart parks its shard again beside the collection's other parked shard", func(t *testing.T) {
		f := newParkFixture(t, api.READY)
		logger, hook := logrustest.NewNullLogger()
		f.o.logger = logger
		f.states.set("alpha", api.NamespaceStateSuspended)
		f.submit(t, alphaS1, false)
		f.submit(t, alphaS2, false)
		f.waitParked(t, alphaS1, 0)
		f.waitParked(t, alphaS2, 0)

		require.NoError(t, f.o.Restart(context.Background(), alphaS1))
		f.waitParked(t, alphaS1, 0)
		require.Equal(t, 2, len(slices.DeleteFunc(hook.AllEntries(), func(e *logrus.Entry) bool {
			return e.Data["event"] != "self_recovery.parked" || e.Data["shard"] != alphaS1.Shard
		})), "the restart did not run the shard again")
		parked := f.parked()
		require.Len(t, parked, 2)
		require.Contains(t, parked, alphaS2.key())

		f.states.set("alpha", api.NamespaceStateActive)
		f.waitSucceeded(t, 2)
		require.Equal(t, 1, f.attempts(alphaS1))
		require.Equal(t, 1, f.attempts(alphaS2))
	})

	// No wipe marker is configured, so only the parked entry carries the label.
	t.Run("a debug restart keeps a parked wiped shard's label", func(t *testing.T) {
		f := newParkFixture(t, api.READY)
		f.states.set("alpha", api.NamespaceStateSuspended)
		f.submit(t, alphaS1, true)
		f.waitParked(t, alphaS1, 0)

		require.NoError(t, f.o.Restart(context.Background(), alphaS1))
		f.waitParked(t, alphaS1, 0)
		parked := f.parked()
		require.Len(t, parked, 1)
		require.True(t, parked[alphaS1.key()].startedWithoutRaftState)
	})

	// A restart drops the parked map, and the boot after it submits the shard as intact.
	// Only the wipe marker carries the shard's label across it.
	t.Run("a node restarted while a wiped shard is parked still counts the shard as wiped", func(t *testing.T) {
		root := t.TempDir()
		states := newNamespaceStates()
		states.set("alpha", api.NamespaceStateSuspended)
		// startNode builds this node's orchestrator. The shard has no other replica, so a release falls back to empty.
		startNode := func() *Orchestrator {
			o := New(Config{
				Raft: &stubRaft{}, Schema: stubSchema{replicas: []string{"self"}}, PathResolver: stubPathResolver{root: root},
				NodeSelector: &stubNodeSelector{}, NodeName: "self", RootDataPath: root, Logger: quietLogger(),
				NamespaceStateForClass: states.read,
			})
			o.parkPollInterval = 20 * time.Millisecond
			t.Cleanup(func() { require.NoError(t, o.Close(context.Background())) })
			return o
		}
		waitParkedIn := func(o *Orchestrator) {
			require.Eventually(t, func() bool {
				o.queueMu.Lock()
				defer o.queueMu.Unlock()
				_, ok := o.parked[alphaS1.key()]
				return ok && o.active == 0 && o.pending.Len() == 0
			}, 5*time.Second, time.Millisecond)
		}

		wiped := startNode()
		require.True(t, wiped.Submit(context.Background(), alphaS1, true))
		waitParkedIn(wiped)
		require.FileExists(t, wiped.wipeMarker, "a parked shard keeps the wipe round open")
		require.NoError(t, wiped.Close(context.Background()))

		restarted := startNode()
		require.True(t, restarted.Submit(context.Background(), alphaS1, false))
		waitParkedIn(restarted)

		states.set("alpha", api.NamespaceStateActive)
		require.Eventually(t, func() bool {
			return testutil.ToFloat64(restarted.metrics.NoDataDuringBootstrapTotal) == 1
		}, 5*time.Second, time.Millisecond)
		require.Zero(t, testutil.ToFloat64(restarted.metrics.NoDataEmptyTotal))
		require.Eventually(t, func() bool { _, err := os.Stat(restarted.wipeMarker); return errors.Is(err, fs.ErrNotExist) },
			5*time.Second, time.Millisecond, "the drained queue keeps the wipe round open")
	})

	t.Run("a plain register error gives up and parks nothing", func(t *testing.T) {
		f := newParkFixture(t, api.READY)
		f.raft.registerErr = errors.New("leader unavailable")
		f.submit(t, alphaS1, false)

		require.Eventually(t, func() bool {
			return testutil.ToFloat64(f.o.metrics.GiveupTotal) == 1 && f.idle()
		}, 5*time.Second, time.Millisecond)
		require.Empty(t, f.parked())
		require.Equal(t, 10.0, f.started(), "a plain register error counts a start per attempt")
	})
}
