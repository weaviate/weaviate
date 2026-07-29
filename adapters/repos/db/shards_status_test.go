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

package db

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAggregateShardStatuses(t *testing.T) {
	tests := []struct {
		name     string
		statuses []string
		expected string
	}{
		{
			name:     "no replicas",
			statuses: nil,
			expected: "",
		},
		{
			name:     "single ready replica",
			statuses: []string{"READY"},
			expected: "READY",
		},
		{
			name:     "all replicas ready",
			statuses: []string{"READY", "READY", "READY"},
			expected: "READY",
		},
		{
			name:     "one replica still indexing",
			statuses: []string{"READY", "INDEXING", "READY"},
			expected: "INDEXING",
		},
		{
			name:     "all replicas indexing",
			statuses: []string{"INDEXING", "INDEXING"},
			expected: "INDEXING",
		},
		{
			name:     "one replica loading",
			statuses: []string{"READY", "LOADING"},
			expected: "LOADING",
		},
		{
			name:     "indexing dominates loading",
			statuses: []string{"LOADING", "INDEXING", "READY"},
			expected: "INDEXING",
		},
		{
			name:     "one replica lazy loading",
			statuses: []string{"LAZY_LOADING", "READY"},
			expected: "LAZY_LOADING",
		},
		{
			name:     "one replica shut down",
			statuses: []string{"READY", "SHUTDOWN"},
			expected: "SHUTDOWN",
		},
		{
			name:     "one replica read-only",
			statuses: []string{"READONLY", "READY"},
			expected: "READONLY",
		},
		{
			name:     "unknown non-ready status is passed through",
			statuses: []string{"READY", "SOMETHING_NEW"},
			expected: "SOMETHING_NEW",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, aggregateShardStatuses(tt.statuses))
		})
	}
}
