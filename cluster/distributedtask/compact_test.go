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

package distributedtask

import (
	"encoding/json"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	cmd "github.com/weaviate/weaviate/cluster/proto/api"
)

type compactTestHeader struct {
	Name    string   `json:"collection"`
	Tenants []string `json:"tenants,omitempty"`
}

func (h compactTestHeader) Collection() string             { return h.Name }
func (h compactTestHeader) VisiblePayload() map[string]any { return nil }

type compactTestCodec struct{}

func (compactTestCodec) CheckConflict(TaskHeader, []*Task) error { return nil }

func (compactTestCodec) DecodeHeader(payload []byte) (TaskHeader, error) {
	var h compactTestHeader
	err := json.Unmarshal(payload, &h)
	return h, err
}

func (compactTestCodec) DecodeCollection(payload []byte) (string, error) {
	var h compactTestHeader
	err := json.Unmarshal(payload, &h)
	return h.Name, err
}

var compactAt = time.UnixMilli(1_700_000_000_000)

func addCompactTestTask(t *testing.T, m *Manager, ns, id, payload string, barrier bool, specs ...*cmd.UnitSpec) {
	t.Helper()
	require.NoError(t, m.AddTask(toCmd(t, &cmd.AddDistributedTaskRequest{
		Namespace: ns, Id: id, Payload: []byte(payload), SubmittedAtUnixMillis: compactAt.UnixMilli(),
		UnitSpecs: specs, NeedsPreparationBarrier: barrier,
	}), 1))
}

func finishCompactTestUnit(t *testing.T, m *Manager, ns, id, node, unit, errMsg string) {
	t.Helper()
	require.NoError(t, m.RecordUnitCompletion(toCmd(t, &cmd.RecordDistributedTaskUnitCompletionRequest{
		Namespace: ns, Id: id, Version: 1, NodeId: node, UnitId: unit, Error: errMsg,
		FinishedAtUnixMillis: compactAt.UnixMilli(),
	})))
}

func ackCompactTestGroup(t *testing.T, m *Manager, id, group, node string, success bool) {
	t.Helper()
	require.NoError(t, m.RecordPreparationCompleteAck(toCmd(t, &cmd.RecordDistributedTaskPreparationCompleteAckRequest{
		Namespace: "reindex", Id: id, Version: 1, GroupId: &group, NodeId: node,
		Success: success, Error: errIfFailed(success), AckedAtUnixMillis: compactAt.UnixMilli(),
	})))
}

func spec(id, group, node string) *cmd.UnitSpec {
	return &cmd.UnitSpec{Id: id, GroupId: group, Node: node}
}

func allocatedBytes(f func()) uint64 {
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	f()
	runtime.ReadMemStats(&after)
	return after.TotalAlloc - before.TotalAlloc
}
