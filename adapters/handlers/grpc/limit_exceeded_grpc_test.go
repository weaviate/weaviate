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

	"github.com/weaviate/weaviate/usecases/usagelimits"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestLimitExceededToGrpcError(t *testing.T) {
	le := &usagelimits.LimitExceededError{
		Limit:           usagelimits.LimitObjects,
		Value:           10000,
		RenderedMessage: "hit limit of 10000 objects",
	}
	err := limitExceededToGrpcError(le)
	if err == nil {
		t.Fatal("expected non-nil error")
	}

	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("expected gRPC status error, got %T", err)
	}
	if st.Code() != codes.ResourceExhausted {
		t.Errorf("code = %v, want ResourceExhausted", st.Code())
	}
	if st.Message() != "hit limit of 10000 objects" {
		t.Errorf("message = %q, want %q", st.Message(), "hit limit of 10000 objects")
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
	if foundErrorInfo.Reason != usagelimits.ErrorCode {
		t.Errorf("Reason = %q, want %q", foundErrorInfo.Reason, usagelimits.ErrorCode)
	}
	if foundErrorInfo.Domain != "weaviate.usagelimits" {
		t.Errorf("Domain = %q, want weaviate.usagelimits", foundErrorInfo.Domain)
	}
	if foundErrorInfo.Metadata["limit"] != string(usagelimits.LimitObjects) {
		t.Errorf("metadata[limit] = %q, want %q", foundErrorInfo.Metadata["limit"], usagelimits.LimitObjects)
	}
	if foundErrorInfo.Metadata["value"] != "10000" {
		t.Errorf("metadata[value] = %q, want 10000", foundErrorInfo.Metadata["value"])
	}
	if foundErrorInfo.Metadata["message"] != "hit limit of 10000 objects" {
		t.Errorf("metadata[message] = %q, want rendered message", foundErrorInfo.Metadata["message"])
	}
}
