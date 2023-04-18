package grpc

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
	}{
		{
			name: "nothing provided",
			buildCtx: func() context.Context {
				return context.Background()
			},
			shouldErr:   false,
			expectedOut: &models.Principal{Username: "anon"},
		},
		{
			name: "with md, but nothing usable",
			buildCtx: func() context.Context {
				md := metadata.Pairs("unrelated", "unrelated")
				return metadata.NewIncomingContext(context.Background(), md)
			},
			shouldErr:   false,
			expectedOut: &models.Principal{Username: "anon"},
		},
		{
			name: "with md, but nothing usable",
			buildCtx: func() context.Context {
				md := metadata.Pairs("authorization", "wrong-format")
				return metadata.NewIncomingContext(context.Background(), md)
			},
			shouldErr:   false,
			expectedOut: &models.Principal{Username: "anon"},
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
			s := &Server{
				authComposer: func(token string, scopes []string) (*models.Principal, error) {
					if token == "" {
						return &models.Principal{Username: "anon"}, nil
					}
					if token == "err" {
						return nil, fmt.Errorf("err")
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
