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

package api

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNamespace_PlacementNodes(t *testing.T) {
	tests := []struct {
		name      string
		available []string
		factor    int
		want      []string
	}{
		{name: "home-node order, cut to the factor", available: []string{"n3", "n2", "n1"}, factor: 2, want: []string{"n1", "n2"}},
		{name: "a foreign entry is dropped and the list is not padded", available: []string{"n3", "n4"}, factor: 2, want: []string{"n3"}},
		{name: "no home node available", available: []string{"n4"}, factor: 2},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ns := Namespace{Name: "alpha", HomeNodes: []string{"n1", "n2", "n3"}}
			assert.Equal(t, tt.want, ns.PlacementNodes(tt.available, tt.factor))
		})
	}
}
