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
	"strings"
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
			wantInReason: []string{`PUT /v1/schema/C/indexes/name {"searchable":{"cancel":true}}`},
		},
		{
			// The asked-for property is named, not the first one in the task.
			name: "the property among several",
			build: func(t *testing.T) []byte {
				return marshal(t, db.ReindexTaskPayload{
					MigrationType: db.ReindexTypeChangeTokenization,
					Collection:    "C",
					Properties:    []string{"other", "name"},
				})
			},
			wantInReason: []string{`PUT /v1/schema/C/indexes/name {"searchable":{"cancel":true}}`},
			notInReason:  []string{"/indexes/other"},
		},
		{
			// Empty Properties means "all properties" via different code on
			// each side — the row most likely to drift.
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
					MigrationType:      db.ReindexTypeChangeTokenization,
					Collection:         "customer1:C",
					Properties:         []string{"name"},
					TargetTokenization: "word",
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

// TestTheRenderedCallReachesBothKindsOfCaller pins that the rendered URL
// keeps its namespace prefix: the REST error path strips a confined
// caller's own prefix, but a global operator needs it to reach the class.
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

// TestEveryDeclaredTypeRendersAnAcceptedRepairCall runs every rendered
// repair/cancel call through the checks the real handler would run on it:
// group exclusivity first, then the per-type precondition for the verb in the
// body. The property state each row carries is the one the remedy is printed
// in — the migration terminalized, so the schema bit its flip would have set
// has not moved. Exclusivity alone is not enough: a body can be
// well-formed and still be rejected by its own validator.
func TestEveryDeclaredTypeRendersAnAcceptedRepairCall(t *testing.T) {
	const targetTok = "word"
	// The tokenization every retokenize row is migrating away from.
	const oldTok = "whitespace"

	enabled, disabled := true, false
	textProp := func(name string, searchable, filterable *bool, tok string) *models.Property {
		return &models.Property{
			Name: name, DataType: []string{"text"},
			IndexSearchable: searchable, IndexFilterable: filterable,
			Tokenization: tok,
		}
	}
	numericProp := func(name string, rangeable *bool) *models.Property {
		return &models.Property{Name: name, DataType: []string{"int"}, IndexRangeFilters: rangeable}
	}

	// The full declared set. accept runs the same validator updateIndex runs
	// for the verb this type's repair body carries.
	cases := []struct {
		migrationType db.ReindexMigrationType
		prop          *models.Property
		usingBlockMax bool
		// wantNoRepair marks a type that deliberately renders no repair call.
		wantNoRepair string
		accept       func(*models.Class, *models.Property, *models.IndexUpdateRequest) error
	}{
		{
			migrationType: db.ReindexTypeChangeAlgorithm,
			prop:          textProp("name", &enabled, nil, targetTok),
			accept: func(c *models.Class, p *models.Property, b *models.IndexUpdateRequest) error {
				return validateChangeAlgorithmProperty(c, p, b.Searchable.Algorithm)
			},
		},
		{
			migrationType: db.ReindexTypeRebuildSearchable,
			prop:          textProp("name", &enabled, nil, targetTok),
			usingBlockMax: true,
			accept: func(c *models.Class, p *models.Property, _ *models.IndexUpdateRequest) error {
				return validateRebuildSearchableProperty(c, p)
			},
		},
		{
			migrationType: db.ReindexTypeRepairFilterable,
			prop:          textProp("name", nil, &enabled, targetTok),
			accept: func(_ *models.Class, p *models.Property, _ *models.IndexUpdateRequest) error {
				return validateRebuildFilterableProperty(p)
			},
		},
		{
			migrationType: db.ReindexTypeEnableRangeable,
			prop:          numericProp("name", &disabled),
			// The flag flips per shard as the migration runs, so no single
			// body is accepted in every terminal state.
			wantNoRepair: "enable-rangeable has no repairable terminal state",
		},
		{
			migrationType: db.ReindexTypeRepairRangeable,
			prop:          numericProp("name", &enabled),
			accept: func(_ *models.Class, p *models.Property, _ *models.IndexUpdateRequest) error {
				return validateRebuildRangeableProperty(p)
			},
		},
		{
			migrationType: db.ReindexTypeEnableFilterable,
			prop:          textProp("name", nil, &disabled, targetTok),
			accept: func(_ *models.Class, p *models.Property, _ *models.IndexUpdateRequest) error {
				return validateEnableFilterableProperty(p)
			},
		},
		{
			migrationType: db.ReindexTypeEnableSearchable,
			prop:          textProp("name", &disabled, &disabled, ""),
			accept: func(_ *models.Class, p *models.Property, b *models.IndexUpdateRequest) error {
				return validateEnableSearchableProperty(p, b.Searchable.Tokenization)
			},
		},
		{
			migrationType: db.ReindexTypeChangeTokenization,
			prop:          textProp("name", &enabled, nil, oldTok),
			accept: func(_ *models.Class, p *models.Property, b *models.IndexUpdateRequest) error {
				return validateSearchableTokenizationChange(p, b.Searchable.Tokenization)
			},
		},
		{
			migrationType: db.ReindexTypeChangeTokenizationFilterable,
			prop:          textProp("name", nil, &enabled, oldTok),
			accept: func(_ *models.Class, p *models.Property, b *models.IndexUpdateRequest) error {
				return validateFilterableTokenizationChange(p, b.Filterable.Tokenization)
			},
		},
	}

	bodyOf := func(t *testing.T, call string) *models.IndexUpdateRequest {
		t.Helper()
		require.True(t, strings.HasPrefix(call, "PUT /v1/schema/C/indexes/name {"),
			"unexpected call shape: %s", call)
		var body models.IndexUpdateRequest
		require.NoError(t, json.Unmarshal(
			[]byte(call[strings.Index(call, "{"):]), &body))
		return &body
	}

	for _, tc := range cases {
		t.Run(string(tc.migrationType), func(t *testing.T) {
			payload := db.ReindexTaskPayload{
				Collection:         "C",
				MigrationType:      tc.migrationType,
				Properties:         []string{"name"},
				TargetTokenization: targetTok,
			}
			class := &models.Class{
				Class:      "C",
				Properties: []*models.Property{tc.prop},
				InvertedIndexConfig: &models.InvertedIndexConfig{
					UsingBlockMaxWAND: tc.usingBlockMax,
				},
			}

			repair := db.ReindexRepairCall(payload, "name")
			if tc.wantNoRepair != "" {
				require.Empty(t, repair, tc.wantNoRepair)
			} else {
				require.NotEmpty(t, repair, "every other declared type needs a repair call")
				body := bodyOf(t, repair)
				require.NoError(t, validateBodyExclusivity(body))
				require.NoError(t, tc.accept(class, tc.prop, body),
					"the rendered repair call is rejected by the handler that receives it")
			}

			// Cancel is dispatched before every per-type precondition, so
			// exclusivity is the whole check it has to pass.
			cancel := db.ReindexCancelCall(payload, "name")
			require.NotEmpty(t, cancel, "every declared type needs a cancel call")
			require.NoError(t, validateBodyExclusivity(bodyOf(t, cancel)))
		})
	}
}

// TestBothLayersNameTheSameTaskWhenSeveralConflict pins the pre-check's sort
// to match the apply gate's (both by task ID), so the two name the same
// conflicting task for the same request.
func TestBothLayersNameTheSameTaskWhenSeveralConflict(t *testing.T) {
	payload, err := json.Marshal(db.ReindexTaskPayload{
		MigrationType:      db.ReindexTypeChangeTokenization,
		Collection:         "C",
		Properties:         []string{"name"},
		TargetTokenization: "word",
	})
	require.NoError(t, err)

	task := func(id string) *distributedtask.Task {
		return &distributedtask.Task{
			Namespace:      db.ReindexNamespace,
			TaskDescriptor: distributedtask.TaskDescriptor{ID: id},
			Status:         distributedtask.TaskStatusStarted,
			Payload:        payload,
		}
	}
	// All three conflict on C.name; only the ordering differs. Three, not
	// two, so that reversing the insertion order does not happen to sort it.
	sorted := []*distributedtask.Task{task("T_a"), task("T_b"), task("T_c")}
	unsorted := []*distributedtask.Task{task("T_b"), task("T_a"), task("T_c")}

	h := &schemaHandlers{
		reindexTaskLister: fakeReindexTaskLister{tasks: map[string][]*distributedtask.Task{
			db.ReindexNamespace: unsorted,
		}},
	}
	preCheck := h.checkReindexConflictForPropertyMutation(context.Background(), "C", "name")

	applyGate := (&db.ReindexProvider{}).CheckPropertyUpdate("C", "name", sorted)
	require.Error(t, applyGate)

	require.Equal(t, applyGate.Error(), preCheck)
	require.Contains(t, preCheck, `"T_a"`)
	require.NotContains(t, preCheck, `"T_b"`)
	require.NotContains(t, preCheck, `"T_c"`)
}
