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

package monitoring

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestErrorToGrpcCode(t *testing.T) {
	assert.Equal(t, codes.OK, errorToGrpcCode(nil))

	// An error carrying a gRPC status must surface that status' own code. This
	// exercises the non-nil-status branch (`if st != nil { return st.Code() }`).
	assert.Equal(t, codes.NotFound, errorToGrpcCode(status.Error(codes.NotFound, "missing")))

	// A plain error has no gRPC status -> Unknown.
	assert.Equal(t, codes.Unknown, errorToGrpcCode(errors.New("boom")))
}

func TestSplitFullMethodName(t *testing.T) {
	svc, method := splitFullMethodName("/weaviate.v1.Weaviate/Search")
	assert.Equal(t, "weaviate.v1.Weaviate", svc)
	assert.Equal(t, "Search", method)

	// No method separator -> unknown/unknown.
	svc, method = splitFullMethodName("/onlyservice")
	assert.Equal(t, "unknown", svc)
	assert.Equal(t, "unknown", method)

	// A separator at index 0 (after trimming the one leading slash) must still
	// count as a split: the `i >= 0` check accepts it, whereas a `i > 0` mutant
	// would wrongly fall through to unknown/unknown.
	svc, method = splitFullMethodName("//Search")
	assert.Equal(t, "", svc)
	assert.Equal(t, "Search", method)
}
