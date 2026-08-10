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

package rest

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db"
	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/entities/models"
)

// TestPropertyGateMessagesAreByteIdenticalAcrossLayers pins that the
// pre-check and apply gate produce byte-identical refusals across every
// status/payload-shape combination, not just matching substrings.
func TestPropertyGateMessagesAreByteIdenticalAcrossLayers(t *testing.T) {
	statuses := []distributedtask.TaskStatus{
		distributedtask.TaskStatusStarted,
		distributedtask.TaskStatusPreparing,
		distributedtask.TaskStatusSwapping,
		distributedtask.TaskStatus("VALIDATING"),
	}

	marshal := func(t *testing.T, p db.ReindexTaskPayload) []byte {
		t.Helper()
		payload, err := json.Marshal(p)
		require.NoError(t, err)
		return payload
	}

	payloads := []struct {
		name string
		// className is what both layers are called with — the qualified
		// form on a namespace-enabled cluster, matching the payload.
		className string
		build     func(*testing.T) []byte
		// wantInReason, when set, must appear in the shared refusal.
		wantInReason []string
		// notInReason, when set, must not.
		notInReason []string
	}{
		{
			name: "the property by name",
			build: func(t *testing.T) []byte {
				return marshal(t, db.ReindexTaskPayload{
					MigrationType: db.ReindexTypeChangeTokenization,
					Collection:    "C",
					Properties:    []string{"name"},
				})
			},
		},
		{
			name: "the property among several",
			build: func(t *testing.T) []byte {
				return marshal(t, db.ReindexTaskPayload{
					MigrationType: db.ReindexTypeChangeTokenization,
					Collection:    "C",
					Properties:    []string{"other", "name"},
				})
			},
		},
		{
			// Empty Properties is "all properties" on both sides, but each
			// layer spells that out with different code — most likely row
			// to drift.
			name: "no properties means all of them",
			build: func(t *testing.T) []byte {
				return marshal(t, db.ReindexTaskPayload{
					MigrationType: db.ReindexTypeChangeTokenization,
					Collection:    "C",
				})
			},
		},
		{
			name: "a payload written by an older binary",
			build: func(t *testing.T) []byte {
				return marshal(t, db.ReindexTaskPayload{Collection: "C", Properties: []string{"name"}})
			},
		},
		{
			name:  "a payload that will not parse",
			build: func(t *testing.T) []byte { return []byte("{not json") },
		},
		{
			// Both layers keep the qualified class in the cancel URL: a
			// global operator has to type the prefix, and the REST error
			// path strips it again for the namespace-confined caller.
			name:      "a namespace-qualified collection",
			className: "customer1:C",
			build: func(t *testing.T) []byte {
				return marshal(t, db.ReindexTaskPayload{
					MigrationType: db.ReindexTypeChangeTokenization,
					Collection:    "customer1:C",
					Properties:    []string{"name"},
				})
			},
			wantInReason: []string{`PUT /v1/schema/customer1:C/indexes/name {"searchable":{"cancel":true}}`},
			notInReason:  []string{"/v1/schema/C/"},
		},
	}

	for _, p := range payloads {
		for _, status := range statuses {
			t.Run(p.name+"/"+string(status), func(t *testing.T) {
				className := p.className
				if className == "" {
					className = "C"
				}
				task := &distributedtask.Task{
					Namespace:      db.ReindexNamespace,
					TaskDescriptor: distributedtask.TaskDescriptor{ID: "T_remedy"},
					Status:         status,
					Payload:        p.build(t),
				}

				h := &schemaHandlers{
					reindexTaskLister: fakeReindexTaskLister{tasks: map[string][]*distributedtask.Task{
						db.ReindexNamespace: {task},
					}},
				}
				preCheck := h.checkReindexConflictForPropertyMutation(context.Background(), className, "name")

				applyGate := (&db.ReindexProvider{}).CheckPropertyUpdate(
					className, "name", []*distributedtask.Task{task})
				require.Error(t, applyGate)

				require.Equal(t, applyGate.Error(), preCheck)
				// An unrecognized status claims nothing about cancel, so it
				// renders no call to check the class name in.
				if status.IsCoordinationPhase() || status == distributedtask.TaskStatusStarted {
					for _, want := range p.wantInReason {
						require.Contains(t, preCheck, want)
					}
				}
				for _, unwanted := range p.notInReason {
					require.NotContains(t, preCheck, unwanted)
				}
			})
		}
	}
}

// TestTheCancelHelpersReadTheSharedIndexTypeMapping pins that both REST
// helpers delegate to db.ReindexTargetIndexes rather than forking their own
// copy of the mapping. (The mapping's own arms are pinned in the db package.)
func TestTheCancelHelpersReadTheSharedIndexTypeMapping(t *testing.T) {
	migrationTypes := []db.ReindexMigrationType{
		db.ReindexTypeChangeTokenization, // two index types
		db.ReindexTypeEnableRangeable,    // one
		db.ReindexMigrationType("invent-index"),
	}

	for _, mt := range migrationTypes {
		t.Run(string(mt), func(t *testing.T) {
			targets := db.ReindexTargetIndexes(mt)

			indexTypes, known := indexTypesFromMigrationType(mt)
			require.Equal(t, targets, indexTypes)
			require.Equal(t, len(targets) > 0, known)

			for _, indexType := range []string{"searchable", "filterable", "rangeable"} {
				matches, known := migrationTypeTargetsIndex(mt, indexType)
				require.Equal(t, len(targets) > 0, known)
				require.Equal(t, slices.Contains(targets, indexType), matches)
			}
		})
	}
}

// TestTheRenderedCallReachesBothKindsOfCaller pins the reason the gate keeps
// the namespace prefix in the URL it renders. The two callers need opposite
// things from the same message, and only one of them can be served by the
// renderer: the confined caller is served by the REST error path, which
// removes their own prefix from the whole message.
//
// Stripping in the renderer instead would be a no-op for the confined caller
// (their prefix comes off either way) and would hand the global operator a
// short name, which QualifyClass leaves short and which then resolves to no
// collection at all.
func TestTheRenderedCallReachesBothKindsOfCaller(t *testing.T) {
	payload := db.ReindexTaskPayload{
		MigrationType: db.ReindexTypeChangeTokenization,
		Collection:    "customer1:C",
		Properties:    []string{"name"},
	}
	raw, err := json.Marshal(payload)
	require.NoError(t, err)
	task := &distributedtask.Task{
		Namespace:      db.ReindexNamespace,
		TaskDescriptor: distributedtask.TaskDescriptor{ID: "T_remedy"},
		Status:         distributedtask.TaskStatusStarted,
		Payload:        raw,
	}
	h := &schemaHandlers{
		reindexTaskLister: fakeReindexTaskLister{tasks: map[string][]*distributedtask.Task{
			db.ReindexNamespace: {task},
		}},
	}
	reason := h.checkReindexConflictForPropertyMutation(context.Background(), "customer1:C", "name")
	require.NotEmpty(t, reason)

	cases := []struct {
		name      string
		principal *models.Principal
		want      string
	}{
		{
			name:      "namespace-confined caller",
			principal: &models.Principal{Namespace: "customer1"},
			want:      `PUT /v1/schema/C/indexes/name {"searchable":{"cancel":true}}`,
		},
		{
			name:      "global operator",
			principal: &models.Principal{Namespace: "customer1", IsGlobalOperator: true},
			want:      `PUT /v1/schema/customer1:C/indexes/name {"searchable":{"cancel":true}}`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			payload := errPayloadFromSingleErr(tc.principal, fmt.Errorf("%s", reason))
			require.NotNil(t, payload)
			require.Len(t, payload.Error, 1)
			require.Contains(t, payload.Error[0].Message, tc.want)
		})
	}
}
