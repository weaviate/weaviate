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
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/backup"
)

func TestProjectDesignations(t *testing.T) {
	plan := &DedupePlan{
		Designations: map[string]map[string]string{
			"C1": {"s1": "n1", "s2": "n2"},
			"C2": {"t1": "n3"},
		},
		Replicas: map[string]map[string][]string{
			"C1": {"s1": {"n1", "n2"}, "s2": {"n2", "n3"}},
			"C2": {"t1": {"n3", "n1"}},
		},
	}

	assert.Equal(t, map[string]map[string]string{
		"C1": {"s1": "n1"},
		"C2": {"t1": "n3"},
	}, projectDesignations(plan, "n1"))
	assert.Equal(t, map[string]map[string]string{
		"C1": {"s1": "n1", "s2": "n2"},
	}, projectDesignations(plan, "n2"))
	assert.Nil(t, projectDesignations(plan, "n9"))
	assert.Nil(t, projectDesignations(nil, "n1"))
}

func TestCoordinatedBackupDedupe(t *testing.T) {
	t.Parallel()
	var (
		backendName  = "s3"
		any          = mock.Anything
		backupID     = "dedupe-1"
		ctx          = context.Background()
		nodes        = []string{"N1", "N2"}
		classes      = []string{"Class-A"}
		sReq         = &StatusRequest{OpCreate, backupID, backendName, "", "", "", ""}
		sresp        = &StatusResponse{Status: backup.Success, ID: backupID, Method: OpCreate}
		nodeResolver = newFakeNodeResolver(nodes)
	)

	newDedupeReq := func() Request {
		req := newReq(classes, backendName, backupID)
		req.DedupeReplicas = true
		return req
	}

	convergedPlan := func(designee string) *DedupePlan {
		return &DedupePlan{
			Designations:    map[string]map[string]string{"Class-A": {"s1": designee}},
			Replicas:        map[string]map[string][]string{"Class-A": {"s1": {"N1", "N2"}}},
			Cutoffs:         map[string]int64{"Class-A": time.Now().UnixMilli()},
			CandidateShards: 1,
		}
	}

	requirePlannedOnce := func(t *testing.T, p *fakeDedupePlanner, preferred map[string]map[string]string) {
		t.Helper()
		calls := p.recordedCalls()
		require.Len(t, calls, 1)
		assert.Equal(t, classes, calls[0].classes)
		assert.Equal(t, map[string]struct{}{"N1": {}, "N2": {}}, calls[0].participants)
		assert.Zero(t, calls[0].budget)
		assert.Equal(t, preferred, calls[0].preferred)
		require.NotNil(t, calls[0].cancelled)
		assert.False(t, calls[0].cancelled())
	}

	t.Run("missing ack from old participant aborts", func(t *testing.T) {
		t.Parallel()
		fc := newFakeCoordinator(nodeResolver)
		fc.selector.On("Shards", ctx, classes[0]).Return(nodes, nil)
		fc.client.On("CanCommit", any, nodes[0], any).Return(&CanCommitResponse{
			Method: OpCreate, ID: backupID, Timeout: maxBooking(false), DedupeHonored: true,
		}, nil).Maybe()
		fc.client.On("CanCommit", any, nodes[1], any).Return(&CanCommitResponse{
			Method: OpCreate, ID: backupID, Timeout: 1,
		}, nil)
		fc.client.On("Abort", any, nodes[0], any).Return(nil).Maybe()
		fc.client.On("Abort", any, nodes[1], any).Return(nil).Maybe()
		fc.backend.On("HomeDir", any, any, backupID).Return("bucket/" + backupID)

		coordinator := *fc.coordinator()
		coordinator.dedupePlanner = &fakeDedupePlanner{plan: &DedupePlan{}}
		coordinator.dedupePollInterval = time.Millisecond

		req := newDedupeReq()
		store := coordStore{objectStore{fc.backend, req.ID, "", "", ""}}
		err := coordinator.Backup(ctx, store, &req)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "does not support dedupeReplicas")
		assert.ErrorAs(t, err, &backup.ErrUnprocessable{}, "mixed-version refusal is client-actionable and must map to 422")
		assert.Empty(t, fc.backend.glMeta.ID, "aborted mixed-version create must leave the backend prefix empty")
	})

	runCommittedDedupeBackup := func(t *testing.T, nodeMeta backup.BackupDescriptor, canCommitMatcher interface{}, getObject func(fc *fakeCoordinator), plan *DedupePlan, mutate ...func(*Request)) (*fakeCoordinator, *fakeDedupePlanner, *coordinator) {
		t.Helper()
		fc := newFakeCoordinator(nodeResolver)
		fc.selector.On("Shards", ctx, classes[0]).Return(nodes, nil)

		if plan == nil {
			plan = convergedPlan("N1")
		}
		f := &fakeDedupePlanner{plan: plan}

		ack := &CanCommitResponse{Method: OpCreate, ID: backupID, Timeout: maxBooking(false), DedupeHonored: true}
		fc.client.On("CanCommit", any, nodes[0], canCommitMatcher).Return(ack, nil)
		fc.client.On("CanCommit", any, nodes[1], canCommitMatcher).Return(ack, nil)
		fc.client.On("Commit", any, nodes[0], matchStatusReq(sReq)).Return(nil)
		fc.client.On("Commit", any, nodes[1], matchStatusReq(sReq)).Return(nil)
		fc.client.On("Status", any, nodes[0], matchStatusReq(sReq)).Return(sresp, nil)
		fc.client.On("Status", any, nodes[1], matchStatusReq(sReq)).Return(sresp, nil)
		fc.backend.On("HomeDir", any, any, backupID).Return("bucket/" + backupID)
		fc.backend.On("PutObject", any, backupID, GlobalBackupFile, any).Return(nil).Twice()

		coordinator := *fc.coordinator()
		coordinator.dedupePlanner = f
		coordinator.dedupePollInterval = 5 * time.Millisecond

		mockBackendProvider := NewMockBackupBackendProvider(t)
		coordinator.backends = mockBackendProvider
		mockBackendProvider.EXPECT().BackupBackend(backendName, mock.Anything).Return(fc.backend, nil)
		if getObject == nil {
			getObject = func(fc *fakeCoordinator) {
				fc.backend.On("GetObject", any, any, any, any, any).Return(marshalMeta(nodeMeta), nil)
			}
		}
		getObject(fc)

		req := newDedupeReq()
		for _, m := range mutate {
			m(&req)
		}
		store := coordStore{objectStore{fc.backend, req.ID, "", "", ""}}
		require.NoError(t, coordinator.Backup(ctx, store, &req))
		<-fc.backend.doneChan
		return fc, f, &coordinator
	}

	countGetObjectCalls := func(t *testing.T, fc *fakeCoordinator, c *coordinator) int {
		t.Helper()
		require.Eventually(t, func() bool { return c.lastOp.get().ID == "" }, 5*time.Second, 10*time.Millisecond)
		n := 0
		for i := range fc.backend.Calls {
			if fc.backend.Calls[i].Method == "GetObject" {
				n++
			}
		}
		return n
	}

	t.Run("success stamps v3 and ships projected designations", func(t *testing.T) {
		t.Parallel()
		wantDesignations := map[string]map[string]string{"Class-A": {"s1": "N1"}}
		match := mock.MatchedBy(func(r *Request) bool {
			return r.Method == OpCreate && r.ID == backupID && r.DedupeReplicas &&
				assert.ObjectsAreEqual(wantDesignations, r.ShardDesignations)
		})
		nodeMeta := backup.BackupDescriptor{Status: backup.Success, Classes: []backup.ClassDescriptor{
			{Name: "Class-A", Shards: []*backup.ShardDescriptor{{Name: "s1", Node: "N1"}}},
		}}
		fc, f, c := runCommittedDedupeBackup(t, nodeMeta, match, nil, nil)

		got := fc.backend.glMeta
		assert.Equal(t, backup.Success, got.Status)
		assert.Equal(t, VersionDedupeReplicas, got.Version)
		assert.True(t, got.DedupeReplicas)
		assert.Equal(t, 1, got.DedupeDesignatedShards)
		assert.Equal(t, 0, got.DedupeFallbackShards)
		assert.Equal(t, wantDesignations, got.DedupeDesignations)
		require.Len(t, got.DedupeCutoffsMs, 1)
		assert.Positive(t, got.DedupeCutoffsMs["Class-A"])
		requirePlannedOnce(t, f, nil)
		assert.Equal(t, len(nodes), countGetObjectCalls(t, fc, c), "coverage verify must reuse the descriptors commit already read")
	})

	t.Run("zero designations stamp the legacy version", func(t *testing.T) {
		t.Parallel()
		match := mock.MatchedBy(func(r *Request) bool {
			return r.Method == OpCreate && r.ID == backupID && r.DedupeReplicas &&
				!r.DedupeEffective && len(r.ShardDesignations) == 0
		})
		nodeMeta := backup.BackupDescriptor{Status: backup.Success, Classes: []backup.ClassDescriptor{
			{Name: "Class-A", Shards: []*backup.ShardDescriptor{{Name: "s1", Node: "N1"}, {Name: "s1", Node: "N2"}}},
		}}
		fc, f, c := runCommittedDedupeBackup(t, nodeMeta, match, nil, &DedupePlan{})

		requirePlannedOnce(t, f, nil)
		require.Eventually(t, func() bool { return c.lastOp.get().ID == "" }, 5*time.Second, 10*time.Millisecond)
		got := fc.backend.glMeta
		assert.Equal(t, backup.Success, got.Status)
		assert.Equal(t, Version, got.Version)
		assert.False(t, got.DedupeReplicas)
		assert.Zero(t, got.DedupeDesignatedShards)
		assert.Zero(t, got.DedupeFallbackShards)
		assert.Nil(t, got.DedupeCutoffsMs)
		assert.Nil(t, got.DedupeDesignations)
		assert.Zero(t, got.DedupeSkippedBytes)
	})

	t.Run("deduped base pins the version stamp with zero designations", func(t *testing.T) {
		t.Parallel()
		match := mock.MatchedBy(func(r *Request) bool {
			return r.Method == OpCreate && r.ID == backupID && !r.DedupeEffective
		})
		nodeMeta := backup.BackupDescriptor{Status: backup.Success, Classes: []backup.ClassDescriptor{
			{Name: "Class-A", Shards: []*backup.ShardDescriptor{{Name: "s1", Node: "N1"}, {Name: "s1", Node: "N2"}}},
		}}
		fc, _, c := runCommittedDedupeBackup(t, nodeMeta, match, nil, &DedupePlan{},
			func(r *Request) { r.BaseChainDeduped = true })

		require.Eventually(t, func() bool { return c.lastOp.get().ID == "" }, 5*time.Second, 10*time.Millisecond)
		got := fc.backend.glMeta
		assert.Equal(t, backup.Success, got.Status)
		assert.Equal(t, VersionDedupeReplicas, got.Version)
		assert.False(t, got.DedupeReplicas)
		assert.Zero(t, got.DedupeDesignatedShards)
	})

	t.Run("base designations stick end to end", func(t *testing.T) {
		t.Parallel()
		wantDesignations := map[string]map[string]string{"Class-A": {"s1": "N2"}}
		match := mock.MatchedBy(func(r *Request) bool {
			return r.Method == OpCreate && r.ID == backupID && r.DedupeReplicas &&
				assert.ObjectsAreEqual(wantDesignations, r.ShardDesignations)
		})
		nodeMeta := backup.BackupDescriptor{Status: backup.Success, Classes: []backup.ClassDescriptor{
			{Name: "Class-A", Shards: []*backup.ShardDescriptor{{Name: "s1", Node: "N2"}}},
		}}
		fc, f, c := runCommittedDedupeBackup(t, nodeMeta, match, nil, convergedPlan("N2"),
			func(r *Request) { r.BaseDedupeDesignations = wantDesignations })

		requirePlannedOnce(t, f, wantDesignations)
		require.Eventually(t, func() bool { return c.lastOp.get().ID == "" }, 5*time.Second, 10*time.Millisecond)
		got := fc.backend.glMeta
		assert.Equal(t, backup.Success, got.Status)
		assert.Equal(t, wantDesignations, got.DedupeDesignations)
	})

	t.Run("coverage verify re-reads only nodes commit could not", func(t *testing.T) {
		t.Parallel()
		nodeMeta := backup.BackupDescriptor{Status: backup.Success, Classes: []backup.ClassDescriptor{
			{Name: "Class-A", Shards: []*backup.ShardDescriptor{{Name: "s1", Node: "N1"}}},
		}}
		getObject := func(fc *fakeCoordinator) {
			fc.backend.On("GetObject", any, backupID+"/N1", any, any, any).Return(nil, ErrAny).Once()
			fc.backend.On("GetObject", any, backupID, any, any, any).Return(nil, ErrAny)
			fc.backend.On("GetObject", any, backupID+"/N1", any, any, any).Return(marshalMeta(nodeMeta), nil)
			fc.backend.On("GetObject", any, backupID+"/N2", any, any, any).Return(marshalMeta(nodeMeta), nil)
		}
		fc, _, c := runCommittedDedupeBackup(t, nodeMeta, any, getObject, nil)

		assert.Equal(t, backup.Success, fc.backend.glMeta.Status)
		assert.Equal(t, 4, countGetObjectCalls(t, fc, c), "one failed commit read, its legacy-detect probe, one commit read, one verify fallback re-read")
	})

	t.Run("poll during coverage verify never reports success", func(t *testing.T) {
		t.Parallel()
		fc := newFakeCoordinator(nodeResolver)
		fc.selector.On("Shards", ctx, classes[0]).Return(nodes, nil)

		ack := &CanCommitResponse{Method: OpCreate, ID: backupID, Timeout: maxBooking(false), DedupeHonored: true}
		fc.client.On("CanCommit", any, nodes[0], any).Return(ack, nil)
		fc.client.On("CanCommit", any, nodes[1], any).Return(ack, nil)
		fc.client.On("Commit", any, nodes[0], matchStatusReq(sReq)).Return(nil)
		fc.client.On("Commit", any, nodes[1], matchStatusReq(sReq)).Return(nil)
		fc.client.On("Status", any, nodes[0], matchStatusReq(sReq)).Return(sresp, nil)
		fc.client.On("Status", any, nodes[1], matchStatusReq(sReq)).Return(sresp, nil)
		fc.backend.On("HomeDir", any, any, backupID).Return("bucket/" + backupID)
		fc.backend.On("PutObject", any, backupID, GlobalBackupFile, any).Return(nil).Twice()

		enteredVerify := make(chan struct{})
		releaseVerify := make(chan struct{})
		okMeta := backup.BackupDescriptor{Status: backup.Success, Classes: []backup.ClassDescriptor{
			{Name: "Class-A", Shards: []*backup.ShardDescriptor{{Name: "s1", Node: "N1"}}},
		}}
		fc.backend.On("GetObject", any, backupID+"/N1", any, any, any).Return(nil, ErrAny).Once()
		fc.backend.On("GetObject", any, backupID, any, any, any).Return(nil, ErrAny)
		fc.backend.On("GetObject", any, backupID+"/N1", any, any, any).
			Run(func(mock.Arguments) { close(enteredVerify); <-releaseVerify }).
			Return(marshalMeta(backup.BackupDescriptor{Status: backup.Success}), nil).Once()
		fc.backend.On("GetObject", any, backupID+"/N2", any, any, any).Return(marshalMeta(okMeta), nil)

		coordinator := *fc.coordinator()
		coordinator.dedupePlanner = &fakeDedupePlanner{plan: convergedPlan("N1")}
		coordinator.dedupePollInterval = 5 * time.Millisecond

		mockBackendProvider := NewMockBackupBackendProvider(t)
		coordinator.backends = mockBackendProvider
		mockBackendProvider.EXPECT().BackupBackend(backendName, mock.Anything).Return(fc.backend, nil)

		req := newDedupeReq()
		store := coordStore{objectStore{fc.backend, req.ID, "", "", ""}}
		require.NoError(t, coordinator.Backup(ctx, store, &req))
		<-enteredVerify

		st, err := coordinator.OnStatus(ctx, store, sReq)
		require.NoError(t, err)
		assert.Equal(t, backup.Started, st.Status)

		close(releaseVerify)
		<-fc.backend.doneChan
		assert.Equal(t, backup.Failed, fc.backend.glMeta.Status)
		assert.Contains(t, fc.backend.glMeta.Error, "designated shard")
		require.Eventually(t, func() bool { return coordinator.lastOp.get().ID == "" }, 5*time.Second, 10*time.Millisecond)
		reason, ok := coordinator.lastOp.rememberedFailure(backupID)
		require.True(t, ok)
		assert.Contains(t, reason, "designated shard")
	})

	t.Run("designated shard missing from archive fails the backup", func(t *testing.T) {
		t.Parallel()
		fc, _, c := runCommittedDedupeBackup(t, backup.BackupDescriptor{Status: backup.Success}, any, nil, nil)

		got := fc.backend.glMeta
		assert.Equal(t, backup.Failed, got.Status)
		assert.Contains(t, got.Error, "designated shard")
		assert.Zero(t, got.DedupeSkippedBytes, "failed backup must not attribute sizes")
		require.Eventually(t, func() bool { return c.lastOp.get().ID == "" }, 5*time.Second, 10*time.Millisecond)
		reason, ok := c.lastOp.rememberedFailure(backupID)
		require.True(t, ok, "coverage failure must be published to the slot, not only the stored descriptor")
		assert.Contains(t, reason, "designated shard")
	})

	t.Run("success attributes logical sizes to skipping replicas", func(t *testing.T) {
		t.Parallel()
		state := []byte(`{"physical":{"s1":{"belongsToNodes":["N1","N2"]}}}`)
		n1Meta := backup.BackupDescriptor{Status: backup.Success, PreCompressionSizeBytes: 1000, Classes: []backup.ClassDescriptor{
			{Name: "Class-A", ShardingState: state, PreCompressionSizeBytes: 1000, Shards: []*backup.ShardDescriptor{{Name: "s1", Node: "N1", PreCompressionSizeBytes: 1000}}},
		}}
		n2Meta := backup.BackupDescriptor{Status: backup.Success, Classes: []backup.ClassDescriptor{{Name: "Class-A", ShardingState: state}}}
		getObject := func(fc *fakeCoordinator) {
			fc.backend.On("GetObject", any, backupID+"/N1", any, any, any).Return(marshalMeta(n1Meta), nil)
			fc.backend.On("GetObject", any, backupID+"/N2", any, any, any).Return(marshalMeta(n2Meta), nil)
		}
		fc, _, c := runCommittedDedupeBackup(t, backup.BackupDescriptor{}, any, getObject, nil)

		got := fc.backend.glMeta
		assert.Equal(t, backup.Success, got.Status)
		assert.Equal(t, int64(2000), got.PreCompressionSizeBytes)
		assert.Equal(t, int64(1000), got.DedupeSkippedBytes)
		assert.Equal(t, int64(1000), got.Nodes["N1"].PreCompressionSizeBytes)
		assert.Equal(t, int64(1000), got.Nodes["N2"].PreCompressionSizeBytes)
		assert.Equal(t, len(nodes), countGetObjectCalls(t, fc, c), "attribution must reuse the descriptors commit already read")
	})

	t.Run("attribution survives a transient commit meta read failure", func(t *testing.T) {
		t.Parallel()
		state := []byte(`{"physical":{"s1":{"belongsToNodes":["N1","N2"]}}}`)
		n1Meta := backup.BackupDescriptor{Status: backup.Success, PreCompressionSizeBytes: 1000, Classes: []backup.ClassDescriptor{
			{Name: "Class-A", ShardingState: state, PreCompressionSizeBytes: 1000, Shards: []*backup.ShardDescriptor{{Name: "s1", Node: "N1", PreCompressionSizeBytes: 1000}}},
		}}
		n2Meta := backup.BackupDescriptor{Status: backup.Success, Classes: []backup.ClassDescriptor{{Name: "Class-A", ShardingState: state}}}
		getObject := func(fc *fakeCoordinator) {
			fc.backend.On("GetObject", any, backupID+"/N1", any, any, any).Return(nil, ErrAny).Once()
			fc.backend.On("GetObject", any, backupID, any, any, any).Return(nil, ErrAny)
			fc.backend.On("GetObject", any, backupID+"/N1", any, any, any).Return(marshalMeta(n1Meta), nil)
			fc.backend.On("GetObject", any, backupID+"/N2", any, any, any).Return(marshalMeta(n2Meta), nil)
		}
		fc, _, c := runCommittedDedupeBackup(t, backup.BackupDescriptor{}, any, getObject, nil)

		got := fc.backend.glMeta
		assert.Equal(t, backup.Success, got.Status)
		assert.Zero(t, got.Nodes["N1"].PreCompressionSizeBytes)
		assert.Equal(t, int64(1000), got.Nodes["N2"].PreCompressionSizeBytes)
		assert.Equal(t, int64(1000), got.PreCompressionSizeBytes)
		assert.Equal(t, int64(1000), got.DedupeSkippedBytes)
		assert.Equal(t, 4, countGetObjectCalls(t, fc, c), "one failed commit read, its legacy-detect probe, one commit read, one verify fallback re-read")
	})

	t.Run("no planner backs up in the legacy format", func(t *testing.T) {
		t.Parallel()
		fc := newFakeCoordinator(nodeResolver)
		fc.selector.On("Shards", ctx, classes[0]).Return(nodes, nil)
		match := mock.MatchedBy(func(r *Request) bool {
			return r.Method == OpCreate && r.ID == backupID && !r.DedupeEffective && len(r.ShardDesignations) == 0
		})
		ack := &CanCommitResponse{Method: OpCreate, ID: backupID, Timeout: maxBooking(false), DedupeHonored: true}
		fc.client.On("CanCommit", any, nodes[0], match).Return(ack, nil)
		fc.client.On("CanCommit", any, nodes[1], match).Return(ack, nil)
		fc.client.On("Commit", any, nodes[0], matchStatusReq(sReq)).Return(nil)
		fc.client.On("Commit", any, nodes[1], matchStatusReq(sReq)).Return(nil)
		fc.client.On("Status", any, nodes[0], matchStatusReq(sReq)).Return(sresp, nil)
		fc.client.On("Status", any, nodes[1], matchStatusReq(sReq)).Return(sresp, nil)
		fc.backend.On("HomeDir", any, any, backupID).Return("bucket/" + backupID)
		fc.backend.On("PutObject", any, backupID, GlobalBackupFile, any).Return(nil).Twice()
		fc.backend.On("GetObject", any, any, any, any, any).Return(marshalMeta(backup.BackupDescriptor{Status: backup.Success}), nil)

		coordinator := *fc.coordinator()
		require.Nil(t, coordinator.dedupePlanner)
		mockBackendProvider := NewMockBackupBackendProvider(t)
		coordinator.backends = mockBackendProvider
		mockBackendProvider.EXPECT().BackupBackend(backendName, mock.Anything).Return(fc.backend, nil).Maybe()
		req := newDedupeReq()
		store := coordStore{objectStore{fc.backend, req.ID, "", "", ""}}
		require.NoError(t, coordinator.Backup(ctx, store, &req))
		<-fc.backend.doneChan

		require.Eventually(t, func() bool { return coordinator.lastOp.get().ID == "" }, 5*time.Second, 10*time.Millisecond)
		got := fc.backend.glMeta
		assert.Equal(t, backup.Success, got.Status)
		assert.Equal(t, Version, got.Version)
		assert.False(t, got.DedupeReplicas)
		assert.Zero(t, got.DedupeDesignatedShards)
		assert.Zero(t, got.DedupeFallbackShards)
		assert.Nil(t, got.DedupeDesignations)
		assert.Nil(t, got.DedupeCutoffsMs)
	})

	t.Run("flag off keeps wire payload legacy", func(t *testing.T) {
		t.Parallel()
		raw, err := json.Marshal(&Request{Method: OpCreate, ID: backupID, Classes: classes})
		require.NoError(t, err)
		for _, key := range []string{"dedupeReplicas", "dedupeEffective", "shardDesignations", "dedupeConvergenceTimeoutSeconds"} {
			assert.NotContains(t, string(raw), key)
		}
		raw, err = json.Marshal(&CanCommitResponse{Method: OpCreate, ID: backupID, Timeout: 1})
		require.NoError(t, err)
		assert.NotContains(t, string(raw), "dedupe_honored")

		var legacy Request
		require.NoError(t, json.Unmarshal([]byte(`{"Method":"create","ID":"x"}`), &legacy))
		assert.False(t, legacy.DedupeReplicas)
		assert.False(t, legacy.DedupeEffective)
		assert.Nil(t, legacy.ShardDesignations)
	})
}

func TestAttributeDedupedShardSizesWarnsOncePerBackup(t *testing.T) {
	log, hook := test.NewNullLogger()
	designations := make(map[string]map[string]string, 30)
	for i := range 30 {
		designations[fmt.Sprintf("Class-%02d", i)] = map[string]string{"s1": "N1"}
	}
	desc := &backup.DistributedBackupDescriptor{Leader: "N1", DedupeDesignations: designations}

	attributeDedupedShardSizes(log, desc, map[string]*backup.BackupDescriptor{})

	var warns []string
	for _, e := range hook.AllEntries() {
		if e.Level <= logrus.WarnLevel {
			warns = append(warns, e.Message)
		}
	}
	require.Len(t, warns, 1)
	assert.Contains(t, warns[0], "skipped 30 classes")
	assert.Contains(t, warns[0], "+20 more]")
}

func TestCappedNameList(t *testing.T) {
	tests := []struct {
		names []string
		total int
		want  string
	}{
		{want: "[]"},
		{names: []string{"a", "b"}, total: 2, want: "[a b]"},
		{names: []string{"a", "b"}, total: 12, want: "[a b +10 more]"},
	}
	for _, tc := range tests {
		t.Run(tc.want, func(t *testing.T) {
			assert.Equal(t, tc.want, cappedNameList(tc.names, tc.total))
		})
	}
}

func TestAttributeDedupedShardSizes(t *testing.T) {
	log, _ := test.NewNullLogger()
	state := func(shards map[string][]string) []byte {
		physical := make(map[string]map[string][]string, len(shards))
		for name, replicas := range shards {
			physical[name] = map[string][]string{"belongsToNodes": replicas}
		}
		raw, err := json.Marshal(map[string]interface{}{"physical": physical})
		require.NoError(t, err)
		return raw
	}
	shard := func(name string, size int64) *backup.ShardDescriptor {
		return &backup.ShardDescriptor{Name: name, PreCompressionSizeBytes: size}
	}
	class := func(name string, shardingState []byte, shards ...*backup.ShardDescriptor) backup.ClassDescriptor {
		return backup.ClassDescriptor{Name: name, ShardingState: shardingState, Shards: shards}
	}
	meta := func(classes ...backup.ClassDescriptor) *backup.BackupDescriptor {
		return &backup.BackupDescriptor{Status: backup.Success, Classes: classes}
	}
	desc := func(designations map[string]map[string]string, nodeSizes map[string]int64) *backup.DistributedBackupDescriptor {
		nodes := make(map[string]*backup.NodeDescriptor, len(nodeSizes))
		var total int64
		for n, size := range nodeSizes {
			nodes[n] = &backup.NodeDescriptor{PreCompressionSizeBytes: size}
			total += size
		}
		return &backup.DistributedBackupDescriptor{Leader: "N1", Nodes: nodes, DedupeDesignations: designations, PreCompressionSizeBytes: total}
	}
	rf2 := state(map[string][]string{"s1": {"N1", "N2"}})

	tests := []struct {
		name        string
		desc        *backup.DistributedBackupDescriptor
		metas       map[string]*backup.BackupDescriptor
		wantNodes   map[string]int64
		wantSkipped int64
	}{
		{
			name: "rf2 single shard",
			desc: desc(map[string]map[string]string{"Class-A": {"s1": "N1"}}, map[string]int64{"N1": 1000, "N2": 0}),
			metas: map[string]*backup.BackupDescriptor{
				"N1": meta(class("Class-A", rf2, shard("s1", 1000))),
				"N2": meta(class("Class-A", rf2)),
			},
			wantNodes:   map[string]int64{"N1": 1000, "N2": 1000},
			wantSkipped: 1000,
		},
		{
			name: "rf3 two skipping replicas",
			desc: desc(map[string]map[string]string{"Class-A": {"s1": "N1"}}, map[string]int64{"N1": 700, "N2": 0, "N3": 0}),
			metas: map[string]*backup.BackupDescriptor{
				"N1": meta(class("Class-A", state(map[string][]string{"s1": {"N1", "N2", "N3"}}), shard("s1", 700))),
				"N2": meta(class("Class-A", nil)),
				"N3": meta(class("Class-A", nil)),
			},
			wantNodes:   map[string]int64{"N1": 700, "N2": 700, "N3": 700},
			wantSkipped: 1400,
		},
		{
			name: "multiple classes and shards with spread archivers",
			desc: desc(map[string]map[string]string{"Class-A": {"s1": "N1", "s2": "N2"}, "Class-B": {"t1": "N2"}}, map[string]int64{"N1": 100, "N2": 250}),
			metas: map[string]*backup.BackupDescriptor{
				"N1": meta(class("Class-A", state(map[string][]string{"s1": {"N1", "N2"}, "s2": {"N1", "N2"}}), shard("s1", 100)), class("Class-B", state(map[string][]string{"t1": {"N1", "N2"}}))),
				"N2": meta(class("Class-A", nil, shard("s2", 200)), class("Class-B", nil, shard("t1", 50))),
			},
			wantNodes:   map[string]int64{"N1": 350, "N2": 350},
			wantSkipped: 350,
		},
		{
			name: "fallback shard untouched",
			desc: desc(map[string]map[string]string{"Class-A": {"s1": "N1"}}, map[string]int64{"N1": 180, "N2": 90}),
			metas: map[string]*backup.BackupDescriptor{
				"N1": meta(class("Class-A", state(map[string][]string{"s1": {"N1", "N2"}, "s2": {"N1", "N2"}}), shard("s1", 100), shard("s2", 80))),
				"N2": meta(class("Class-A", nil, shard("s2", 90))),
			},
			wantNodes:   map[string]int64{"N1": 180, "N2": 190},
			wantSkipped: 100,
		},
		{
			name:        "class with empty designations",
			desc:        desc(map[string]map[string]string{"Class-A": {}}, map[string]int64{"N1": 100, "N2": 100}),
			metas:       map[string]*backup.BackupDescriptor{"N1": meta(class("Class-A", rf2, shard("s1", 100)))},
			wantNodes:   map[string]int64{"N1": 100, "N2": 100},
			wantSkipped: 0,
		},
		{
			name: "replica not a participant",
			desc: desc(map[string]map[string]string{"Class-A": {"s1": "N1"}}, map[string]int64{"N1": 100}),
			metas: map[string]*backup.BackupDescriptor{
				"N1": meta(class("Class-A", state(map[string][]string{"s1": {"N1", "NX"}}), shard("s1", 100))),
			},
			wantNodes:   map[string]int64{"N1": 100},
			wantSkipped: 0,
		},
		{
			name: "replica archived the shard anyway",
			desc: desc(map[string]map[string]string{"Class-A": {"s1": "N1"}}, map[string]int64{"N1": 1000, "N2": 950}),
			metas: map[string]*backup.BackupDescriptor{
				"N1": meta(class("Class-A", rf2, shard("s1", 1000))),
				"N2": meta(class("Class-A", nil, shard("s1", 950))),
			},
			wantNodes:   map[string]int64{"N1": 1000, "N2": 950},
			wantSkipped: 0,
		},
		{
			name: "archiver meta missing",
			desc: desc(map[string]map[string]string{"Class-A": {"s1": "N1"}}, map[string]int64{"N1": 0, "N2": 0}),
			metas: map[string]*backup.BackupDescriptor{
				"N2": meta(class("Class-A", rf2)),
			},
			wantNodes:   map[string]int64{"N1": 0, "N2": 0},
			wantSkipped: 0,
		},
		{
			name: "archiver meta lacks the shard",
			desc: desc(map[string]map[string]string{"Class-A": {"s1": "N1"}}, map[string]int64{"N1": 0, "N2": 0}),
			metas: map[string]*backup.BackupDescriptor{
				"N1": meta(class("Class-A", rf2)),
				"N2": meta(class("Class-A", nil)),
			},
			wantNodes:   map[string]int64{"N1": 0, "N2": 0},
			wantSkipped: 0,
		},
		{
			name: "pre-field archiver reports zero size",
			desc: desc(map[string]map[string]string{"Class-A": {"s1": "N1", "s2": "N1"}}, map[string]int64{"N1": 400, "N2": 0}),
			metas: map[string]*backup.BackupDescriptor{
				"N1": meta(class("Class-A", state(map[string][]string{"s1": {"N1", "N2"}, "s2": {"N1", "N2"}}), shard("s1", 0), shard("s2", 400))),
				"N2": meta(class("Class-A", nil)),
			},
			wantNodes:   map[string]int64{"N1": 400, "N2": 400},
			wantSkipped: 400,
		},
		{
			name: "corrupt leader state falls back to the archiver",
			desc: desc(map[string]map[string]string{"Class-A": {"s1": "N2"}}, map[string]int64{"N1": 0, "N2": 300}),
			metas: map[string]*backup.BackupDescriptor{
				"N1": meta(class("Class-A", []byte("{"))),
				"N2": meta(class("Class-A", rf2, shard("s1", 300))),
			},
			wantNodes:   map[string]int64{"N1": 300, "N2": 300},
			wantSkipped: 300,
		},
		{
			name: "unresolvable state skips only that class",
			desc: desc(map[string]map[string]string{"Class-A": {"s1": "N1"}, "Class-B": {"t1": "N1"}}, map[string]int64{"N1": 150, "N2": 0}),
			metas: map[string]*backup.BackupDescriptor{
				"N1": meta(class("Class-A", []byte("{"), shard("s1", 100)), class("Class-B", state(map[string][]string{"t1": {"N1", "N2"}}), shard("t1", 50))),
				"N2": meta(class("Class-A", []byte("{")), class("Class-B", nil)),
			},
			wantNodes:   map[string]int64{"N1": 150, "N2": 50},
			wantSkipped: 50,
		},
		{
			name: "empty and duplicate replica entries",
			desc: desc(map[string]map[string]string{"Class-A": {"s1": "N1"}}, map[string]int64{"N1": 100, "N2": 0}),
			metas: map[string]*backup.BackupDescriptor{
				"N1": meta(class("Class-A", state(map[string][]string{"s1": {"", "N2", "N2", "N1"}}), shard("s1", 100))),
				"N2": meta(class("Class-A", nil)),
			},
			wantNodes:   map[string]int64{"N1": 100, "N2": 100},
			wantSkipped: 100,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := attributeDedupedShardSizes(log, tc.desc, tc.metas)
			assert.Equal(t, tc.wantSkipped, got)
			assert.Equal(t, tc.wantSkipped, tc.desc.DedupeSkippedBytes)
			var sum int64
			for node, want := range tc.wantNodes {
				require.Contains(t, tc.desc.Nodes, node)
				assert.Equal(t, want, tc.desc.Nodes[node].PreCompressionSizeBytes, node)
			}
			for _, nd := range tc.desc.Nodes {
				sum += nd.PreCompressionSizeBytes
			}
			assert.Equal(t, sum, tc.desc.PreCompressionSizeBytes)
		})
	}

	t.Run("mapped replica lands on its original node entry", func(t *testing.T) {
		d := desc(map[string]map[string]string{"Class-A": {"s1": "N1"}}, map[string]int64{"N1": 100, "N2": 0})
		d.NodeMapping = map[string]string{"N2": "N2x"}
		metas := map[string]*backup.BackupDescriptor{
			"N1":  meta(class("Class-A", state(map[string][]string{"s1": {"N1", "N2x"}}), shard("s1", 100))),
			"N2x": meta(class("Class-A", nil)),
		}
		require.Equal(t, int64(100), attributeDedupedShardSizes(log, d, metas))
		assert.Equal(t, int64(100), d.Nodes["N2"].PreCompressionSizeBytes)
	})

	t.Run("second call is a no-op", func(t *testing.T) {
		d := desc(map[string]map[string]string{"Class-A": {"s1": "N1"}}, map[string]int64{"N1": 1000, "N2": 0})
		metas := map[string]*backup.BackupDescriptor{
			"N1": meta(class("Class-A", rf2, shard("s1", 1000))),
			"N2": meta(class("Class-A", nil)),
		}
		require.Equal(t, int64(1000), attributeDedupedShardSizes(log, d, metas))
		require.Zero(t, attributeDedupedShardSizes(log, d, metas))
		assert.Equal(t, int64(1000), d.Nodes["N2"].PreCompressionSizeBytes)
		assert.Equal(t, int64(2000), d.PreCompressionSizeBytes)
		assert.Equal(t, int64(1000), d.DedupeSkippedBytes)
	})
}
