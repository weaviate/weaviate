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

package grpc

import (
	"testing"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/weaviate/weaviate/usecases/restrictions"
)

// TestRestrictionViolationToGrpcError is the realistic coverage path:
// no gRPC user flow reaches the mapper yet (schema-create is REST-only;
// gRPC BatchObjects auto-schema uses DEFAULT_VECTOR_INDEX, already
// pinned to the allow-list at boot).
func TestRestrictionViolationToGrpcError(t *testing.T) {
	v := &restrictions.ViolationError{
		Restriction:     restrictions.RestrictionVectorIndexType,
		Value:           "flat",
		Allowed:         []string{"hfresh", "hnsw"},
		RenderedMessage: "flat is not allowed for vector_index_type",
	}
	err := restrictionViolationToGrpcError(v)
	if err == nil {
		t.Fatal("expected non-nil error")
	}

	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("expected gRPC status error, got %T", err)
	}
	if st.Code() != codes.FailedPrecondition {
		t.Errorf("code = %v, want FailedPrecondition", st.Code())
	}
	if st.Message() != "flat is not allowed for vector_index_type" {
		t.Errorf("message = %q, want rendered violation message", st.Message())
	}

	var foundErrorInfo *errdetails.ErrorInfo
	for _, d := range st.Details() {
		if ei, ok := d.(*errdetails.ErrorInfo); ok {
			foundErrorInfo = ei
			break
		}
	}
	if foundErrorInfo == nil {
		t.Fatal("expected an ErrorInfo detail attached to the status")
	}
	if foundErrorInfo.Reason != restrictions.ErrorCode {
		t.Errorf("Reason = %q, want %q", foundErrorInfo.Reason, restrictions.ErrorCode)
	}
	if foundErrorInfo.Domain != "weaviate.restrictions" {
		t.Errorf("Domain = %q, want weaviate.restrictions", foundErrorInfo.Domain)
	}
	if foundErrorInfo.Metadata["restriction"] != string(restrictions.RestrictionVectorIndexType) {
		t.Errorf("metadata[restriction] = %q, want %q",
			foundErrorInfo.Metadata["restriction"], restrictions.RestrictionVectorIndexType)
	}
	if foundErrorInfo.Metadata["value"] != "flat" {
		t.Errorf("metadata[value] = %q, want flat", foundErrorInfo.Metadata["value"])
	}
	// allowed is rendered as a comma-joined string; order matches the
	// mapper's input (no sort) so the test pins to that exact form.
	if foundErrorInfo.Metadata["allowed"] != "hfresh,hnsw" {
		t.Errorf("metadata[allowed] = %q, want hfresh,hnsw", foundErrorInfo.Metadata["allowed"])
	}
	if foundErrorInfo.Metadata["message"] != "flat is not allowed for vector_index_type" {
		t.Errorf("metadata[message] = %q, want rendered message", foundErrorInfo.Metadata["message"])
	}
}
