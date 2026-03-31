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
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

func TestMakeClientIdentifierInterceptor(t *testing.T) {
	interceptor := makeClientIdentifierInterceptor()

	tests := []struct {
		name        string
		headerValue string
	}{
		{
			name:        "python client",
			headerValue: "weaviate-client-python/4.10.0",
		},
		{
			name:        "go client",
			headerValue: "weaviate-client-go/2.5.0",
		},
		{
			name:        "no header",
			headerValue: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			if tt.headerValue != "" {
				md := metadata.Pairs("x-weaviate-client", tt.headerValue)
				ctx = metadata.NewIncomingContext(ctx, md)
			}

			var capturedCtx context.Context
			handler := func(ctx context.Context, req any) (any, error) {
				capturedCtx = ctx
				return nil, nil
			}

			_, err := interceptor(ctx, nil, &grpc.UnaryServerInfo{}, handler)
			assert.NoError(t, err)

			if tt.headerValue != "" {
				assert.Equal(t, tt.headerValue, capturedCtx.Value("clientIdentifier"))
			} else {
				assert.Nil(t, capturedCtx.Value("clientIdentifier"))
			}
		})
	}
}
