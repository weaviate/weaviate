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

package grpc_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/conditional"
	protocol "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
)

// TestGRPCConditionalBatchOutcome verifies that:
//  1. ConditionalWriteRequest and ConditionalWriteReply are present in the
//     generated proto package and carry all fields defined in synthesis §6.5.
//  2. The ConditionalWriteOutcome enum values in the generated proto package
//     match the shared domain constants in entities/conditional/outcomes.go.
//  3. The BatchObject message accepts a ConditionalWriteRequest field.
//  4. The BatchObjectsReply message carries per-object ObjectResult entries
//     with ConditionalWriteReply embedded.
func TestGRPCConditionalBatchOutcome(t *testing.T) {
	t.Run("ConditionalWriteRequest fields present", func(t *testing.T) {
		req := &protocol.ConditionalWriteRequest{
			ConditionType: &protocol.ConditionalWriteRequest_InsertIfNotExists{
				InsertIfNotExists: &protocol.InsertIfNotExists{},
			},
			ReturnCurrent: true,
		}
		require.NotNil(t, req.GetInsertIfNotExists())
		assert.True(t, req.ReturnCurrent)
	})

	t.Run("ConditionalWriteRequest VersionMatch", func(t *testing.T) {
		expectedVersion := int64(42)
		req := &protocol.ConditionalWriteRequest{
			ConditionType: &protocol.ConditionalWriteRequest_VersionMatch{
				VersionMatch: &protocol.VersionMatch{ExpectedVersion: expectedVersion},
			},
		}
		require.NotNil(t, req.GetVersionMatch())
		assert.Equal(t, expectedVersion, req.GetVersionMatch().ExpectedVersion)
	})

	t.Run("ConditionalWriteRequest FieldPredicate", func(t *testing.T) {
		req := &protocol.ConditionalWriteRequest{
			ConditionType: &protocol.ConditionalWriteRequest_FieldPredicate{
				FieldPredicate: &protocol.FieldPredicate{
					Field:    "status",
					Operator: protocol.FieldOperator_FIELD_OPERATOR_EQ,
					Value:    &protocol.FieldPredicate_StringValue{StringValue: "active"},
				},
			},
		}
		fp := req.GetFieldPredicate()
		require.NotNil(t, fp)
		assert.Equal(t, "status", fp.Field)
		assert.Equal(t, protocol.FieldOperator_FIELD_OPERATOR_EQ, fp.Operator)
		assert.Equal(t, "active", fp.GetStringValue())
	})

	t.Run("ConditionalWriteReply all required fields", func(t *testing.T) {
		newVer := int64(5)
		curVer := int64(4)
		blob := []byte("serialized-object")
		errMsg := "object already exists"

		reply := &protocol.ConditionalWriteReply{
			Outcome:           protocol.ConditionalWriteOutcome_CONDITIONAL_WRITE_OUTCOME_SKIPPED,
			NewVersion:        &newVer,
			CurrentVersion:    &curVer,
			CurrentObjectBlob: blob,
			ErrorMessage:      &errMsg,
		}

		assert.Equal(t, protocol.ConditionalWriteOutcome_CONDITIONAL_WRITE_OUTCOME_SKIPPED, reply.Outcome)
		require.NotNil(t, reply.NewVersion)
		assert.Equal(t, newVer, *reply.NewVersion)
		require.NotNil(t, reply.CurrentVersion)
		assert.Equal(t, curVer, *reply.CurrentVersion)
		assert.Equal(t, blob, reply.CurrentObjectBlob)
		require.NotNil(t, reply.ErrorMessage)
		assert.Equal(t, errMsg, *reply.ErrorMessage)
	})

	t.Run("BatchObject carries ConditionalWriteRequest", func(t *testing.T) {
		obj := &protocol.BatchObject{
			Uuid:       "00000000-0000-0000-0000-000000000001",
			Collection: "TestCollection",
			Conditional: &protocol.ConditionalWriteRequest{
				ConditionType: &protocol.ConditionalWriteRequest_InsertIfNotExists{
					InsertIfNotExists: &protocol.InsertIfNotExists{},
				},
			},
		}
		require.NotNil(t, obj.Conditional)
		require.NotNil(t, obj.Conditional.GetInsertIfNotExists())
	})

	t.Run("BatchObjectsReply carries per-object ObjectResult", func(t *testing.T) {
		reply := &protocol.BatchObjectsReply{
			Took: 0.001,
			ObjectResults: []*protocol.BatchObjectsReply_ObjectResult{
				{
					Uuid: "00000000-0000-0000-0000-000000000001",
					ConditionalResult: &protocol.ConditionalWriteReply{
						Outcome: protocol.ConditionalWriteOutcome_CONDITIONAL_WRITE_OUTCOME_INSERTED,
					},
				},
				{
					Uuid: "00000000-0000-0000-0000-000000000002",
					ConditionalResult: &protocol.ConditionalWriteReply{
						Outcome: protocol.ConditionalWriteOutcome_CONDITIONAL_WRITE_OUTCOME_SKIPPED,
					},
				},
			},
		}
		require.Len(t, reply.ObjectResults, 2)
		assert.Equal(t, protocol.ConditionalWriteOutcome_CONDITIONAL_WRITE_OUTCOME_INSERTED,
			reply.ObjectResults[0].ConditionalResult.Outcome)
		assert.Equal(t, protocol.ConditionalWriteOutcome_CONDITIONAL_WRITE_OUTCOME_SKIPPED,
			reply.ObjectResults[1].ConditionalResult.Outcome)
	})

	t.Run("outcome enum values match domain constants", func(t *testing.T) {
		// Verify the gRPC enum ordinal values are identical to the domain constants.
		// This is the cross-layer contract that prevents silent mismatches when
		// mappers cast between the two.
		cases := []struct {
			proto  protocol.ConditionalWriteOutcome
			domain conditional.Outcome
		}{
			{protocol.ConditionalWriteOutcome_CONDITIONAL_WRITE_OUTCOME_UNSPECIFIED, conditional.OutcomeUnspecified},
			{protocol.ConditionalWriteOutcome_CONDITIONAL_WRITE_OUTCOME_INSERTED, conditional.OutcomeInserted},
			{protocol.ConditionalWriteOutcome_CONDITIONAL_WRITE_OUTCOME_SKIPPED, conditional.OutcomeSkipped},
			{protocol.ConditionalWriteOutcome_CONDITIONAL_WRITE_OUTCOME_UPDATED, conditional.OutcomeUpdated},
			{protocol.ConditionalWriteOutcome_CONDITIONAL_WRITE_OUTCOME_CONDITION_FAILED, conditional.OutcomeConditionFailed},
			{protocol.ConditionalWriteOutcome_CONDITIONAL_WRITE_OUTCOME_VERSION_MISMATCH, conditional.OutcomeVersionMismatch},
			{protocol.ConditionalWriteOutcome_CONDITIONAL_WRITE_OUTCOME_NOT_FOUND, conditional.OutcomeNotFound},
		}
		for _, tc := range cases {
			assert.Equal(t, int32(tc.proto), int32(tc.domain),
				"gRPC and domain outcome ordinals must match: proto=%v domain=%v",
				tc.proto, tc.domain)
		}
	})

	t.Run("FieldOperator enum covers all synthesis operators", func(t *testing.T) {
		ops := []protocol.FieldOperator{
			protocol.FieldOperator_FIELD_OPERATOR_EQ,
			protocol.FieldOperator_FIELD_OPERATOR_NE,
			protocol.FieldOperator_FIELD_OPERATOR_LT,
			protocol.FieldOperator_FIELD_OPERATOR_LTE,
			protocol.FieldOperator_FIELD_OPERATOR_GT,
			protocol.FieldOperator_FIELD_OPERATOR_GTE,
			protocol.FieldOperator_FIELD_OPERATOR_EXISTS,
			protocol.FieldOperator_FIELD_OPERATOR_NOT_EXISTS,
		}
		// All 8 non-zero operators must be defined (synthesis §6.5 spec).
		assert.Len(t, ops, 8)
		for _, op := range ops {
			assert.NotEqual(t, protocol.FieldOperator_FIELD_OPERATOR_UNSPECIFIED, op)
		}
	})
}
