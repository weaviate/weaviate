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

package telemetry

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

func TestIdentifyClientFromHeader(t *testing.T) {
	tests := []struct {
		name         string
		headerValue  string
		expectedType ClientType
		expectedVer  string
	}{
		{
			name:         "python client",
			headerValue:  "weaviate-client-python/4.10.0",
			expectedType: ClientTypePython,
			expectedVer:  "4.10.0",
		},
		{
			name:         "java client",
			headerValue:  "weaviate-client-java/5.2.1",
			expectedType: ClientTypeJava,
			expectedVer:  "5.2.1",
		},
		{
			name:         "go client",
			headerValue:  "weaviate-client-go/2.5.0",
			expectedType: ClientTypeGo,
			expectedVer:  "2.5.0",
		},
		{
			name:         "typescript client",
			headerValue:  "weaviate-client-typescript/3.1.0",
			expectedType: ClientTypeTypeScript,
			expectedVer:  "3.1.0",
		},
		{
			name:         "csharp client",
			headerValue:  "weaviate-client-csharp/1.0.0",
			expectedType: ClientTypeCSharp,
			expectedVer:  "1.0.0",
		},
		{
			name:         "no version",
			headerValue:  "weaviate-client-python",
			expectedType: ClientTypePython,
			expectedVer:  "",
		},
		{
			name:         "empty header",
			headerValue:  "",
			expectedType: ClientTypeUnknown,
			expectedVer:  "",
		},
		{
			name:         "invalid prefix",
			headerValue:  "some-other-client/1.0.0",
			expectedType: ClientTypeUnknown,
			expectedVer:  "",
		},
		{
			name:         "unknown sdk",
			headerValue:  "weaviate-client-ruby/1.0.0",
			expectedType: ClientTypeUnknown,
			expectedVer:  "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			info := IdentifyClientFromHeader(tt.headerValue)
			assert.Equal(t, tt.expectedType, info.Type)
			assert.Equal(t, tt.expectedVer, info.Version)
		})
	}
}

func TestSanitizeClientHeader(t *testing.T) {
	t.Run("normal header passes through", func(t *testing.T) {
		assert.Equal(t, "weaviate-client-python/4.10.0", SanitizeClientHeader("weaviate-client-python/4.10.0"))
	})

	t.Run("empty string", func(t *testing.T) {
		assert.Equal(t, "", SanitizeClientHeader(""))
	})

	t.Run("caps length at 128", func(t *testing.T) {
		long := strings.Repeat("a", 200)
		assert.Equal(t, 128, len(SanitizeClientHeader(long)))
	})
}

func TestClientTrackingUnaryInterceptor(t *testing.T) {
	logger, _ := test.NewNullLogger()
	tracker := NewClientTracker(logger)
	defer tracker.Stop()

	interceptor := ClientTrackingUnaryInterceptor(tracker)

	handler := func(ctx context.Context, req any) (any, error) {
		return nil, nil
	}

	// Send a gRPC request with x-weaviate-client metadata
	md := metadata.Pairs("x-weaviate-client", "weaviate-client-python/4.10.0")
	ctx := metadata.NewIncomingContext(context.Background(), md)

	_, err := interceptor(ctx, nil, &grpc.UnaryServerInfo{}, handler)
	require.NoError(t, err)

	// Allow the tracker goroutine to process the event
	assert.Eventually(t, func() bool {
		counts := tracker.Get()
		return counts[ClientTypePython]["4.10.0"] == 1
	}, time.Second, 10*time.Millisecond)
}

func TestClientTrackingUnaryInterceptor_NoHeader(t *testing.T) {
	logger, _ := test.NewNullLogger()
	tracker := NewClientTracker(logger)
	defer tracker.Stop()

	interceptor := ClientTrackingUnaryInterceptor(tracker)

	handler := func(ctx context.Context, req any) (any, error) {
		return nil, nil
	}

	// Send a gRPC request without x-weaviate-client metadata
	_, err := interceptor(context.Background(), nil, &grpc.UnaryServerInfo{}, handler)
	require.NoError(t, err)

	counts := tracker.Get()
	assert.Empty(t, counts)
}
