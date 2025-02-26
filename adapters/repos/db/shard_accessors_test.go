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

package db

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestShared_GetVectorIndexAndQueue(t *testing.T) {
	for _, tt := range []struct {
		name      string
		setupFunc func() *Shard

		wantLegacyExists bool
		wantNamedExists  bool
	}{
		{
			name: "nothing is initialized",
			setupFunc: func() *Shard {
				return &Shard{
					queue:  nil,
					queues: nil,
				}
			},
			wantLegacyExists: false,
			wantNamedExists:  false,
		},
		{
			name: "only legacy initialized",
			setupFunc: func() *Shard {
				return &Shard{
					queue:  &VectorIndexQueue{},
					queues: nil,
				}
			},
			wantLegacyExists: true,
			wantNamedExists:  false,
		},
		{
			name: "only named initialized",
			setupFunc: func() *Shard {
				return &Shard{
					queue: nil,
					queues: map[string]*VectorIndexQueue{
						"named": {},
						"foo":   {},
					},
				}
			},
			wantLegacyExists: false,
			wantNamedExists:  true,
		},
		{
			name: "mixed initialized",
			setupFunc: func() *Shard {
				return &Shard{
					queue: &VectorIndexQueue{},
					queues: map[string]*VectorIndexQueue{
						"named": {},
						"foo":   {},
					},
				}
			},
			wantLegacyExists: true,
			wantNamedExists:  true,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			s := tt.setupFunc()

			namedQueue, ok := s.GetVectorIndexQueue("named")
			require.Equal(t, tt.wantNamedExists, ok)

			namedIndex, ok := s.GetVectorIndex("named")
			require.Equal(t, tt.wantNamedExists, ok)

			if tt.wantNamedExists {
				require.NotNil(t, namedQueue)
				require.NotNil(t, namedIndex)
			}

			legacyQueue, ok := s.GetVectorIndexQueue("")
			require.Equal(t, tt.wantLegacyExists, ok)

			legacyIndex, ok := s.GetVectorIndex("")
			require.Equal(t, tt.wantLegacyExists, ok)

			if tt.wantLegacyExists {
				require.NotNil(t, legacyQueue)
				require.NotNil(t, legacyIndex)
			}
		})
	}
}
