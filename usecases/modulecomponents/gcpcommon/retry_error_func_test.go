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

package gcpcommon

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/api/googleapi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestRetryErrorFunc(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "nil error is not retried", err: nil},
		{name: "plain error is not retried", err: errors.New("boom")},
		{name: "http 401 is retried", err: &googleapi.Error{Code: 401}, want: true},
		{name: "http 403 is not retried", err: &googleapi.Error{Code: 403}},
		{name: "http 503 is retried", err: &googleapi.Error{Code: 503}, want: true},
		{name: "http2 connection lost is retried", err: errors.New("http2: client connection lost"), want: true},
		{
			name: "grpc unavailable is retried",
			err:  status.Error(codes.Unavailable, "backend down"),
			want: true,
		},
		{
			name: "wrapped grpc unavailable is retried",
			err:  fmt.Errorf("write object: %w", status.Error(codes.Unavailable, "backend down")),
			want: true,
		},
		{
			name: "grpc permission denied is not retried",
			err:  status.Error(codes.PermissionDenied, "no access"),
		},
		// grpc-go reports a failure to obtain credentials as Unauthenticated,
		// so retrying it would loop forever on a permanently bad credential:
		// the retry policy is RetryAlways and restore runs without a deadline.
		{
			name: "grpc unauthenticated is not retried",
			err:  status.Error(codes.Unauthenticated, "token expired"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, RetryErrorFunc(tt.err))
		})
	}
}
