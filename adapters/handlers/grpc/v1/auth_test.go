//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package v1

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"google.golang.org/grpc/metadata"
)

func TestAuth(t *testing.T) {
	tests := []struct {
		name        string
		buildCtx    func() context.Context
		shouldErr   bool
		expectedOut *models.Principal
		allowAnon   bool
	}{
		{
			name: "nothing provided, anon allowed",
			buildCtx: func() context.Context {
				return context.Background()
			},
			allowAnon: true,
			shouldErr: false,
		},
		{
			name: "nothing provided, anon forbidden",
			buildCtx: func() context.Context {
				return context.Background()
			},
			allowAnon: false,
			shouldErr: true,
		},
		{
			name: "with md, but nothing usable, anon allowed",
			buildCtx: func() context.Context {
				md := metadata.Pairs("unrelated", "unrelated")
				return metadata.NewIncomingContext(context.Background(), md)
			},
			allowAnon: true,
			shouldErr: false,
		},
		{
			name: "with md, but nothing usable, anon forbidden",
			buildCtx: func() context.Context {
				md := metadata.Pairs("unrelated", "unrelated")
				return metadata.NewIncomingContext(context.Background(), md)
			},
			allowAnon: false,
			shouldErr: true,
		},
		{
			name: "with md, but nothing usable, anon allowed",
			buildCtx: func() context.Context {
				md := metadata.Pairs("authorization", "wrong-format")
				return metadata.NewIncomingContext(context.Background(), md)
			},
			allowAnon: true,
			shouldErr: false,
		},
		{
			name: "with md, but nothing usable, anon forbidden",
			buildCtx: func() context.Context {
				md := metadata.Pairs("authorization", "wrong-format")
				return metadata.NewIncomingContext(context.Background(), md)
			},
			allowAnon: false,
			shouldErr: true,
		},
		{
			name: "with md, and a token",
			buildCtx: func() context.Context {
				md := metadata.Pairs("authorization", "Bearer Foo")
				return metadata.NewIncomingContext(context.Background(), md)
			},
			shouldErr:   false,
			expectedOut: &models.Principal{Username: "Foo"},
		},
		{
			name: "with a token that makes extraction error",
			buildCtx: func() context.Context {
				md := metadata.Pairs("authorization", "Bearer err")
				return metadata.NewIncomingContext(context.Background(), md)
			},
			shouldErr: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			s := &Service{
				allowAnonymousAccess: test.allowAnon,
				authComposer: func(token string, scopes []string) (*models.Principal, error) {
					if token == "" {
						return nil, fmt.Errorf("not allowed")
					}
					if token == "err" {
						return nil, fmt.Errorf("other error")
					}
					return &models.Principal{Username: token}, nil
				},
			}

			p, err := s.principalFromContext(test.buildCtx())
			if test.shouldErr {
				require.NotNil(t, err)
			} else {
				require.Nil(t, err)
				assert.Equal(t, test.expectedOut, p)
			}
		})
	}
}
