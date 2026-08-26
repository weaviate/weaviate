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

func TestFilterDesignatedShards(t *testing.T) {
	replicas := map[string][]string{
		"s1": {"n1", "n2", "n3"},
		"s2": {"n1", "n2", "n3"},
		"s3": {"n1", "n2", "n3"},
		"s4": {"n1", "n2"},
	}
	all := []string{"s1", "s2", "s3", "s4"}

	tests := []struct {
		name       string
		designated map[string]string
		nodeName   string
		want       []string
	}{
		{name: "nil map keeps everything", designated: nil, nodeName: "n1", want: all},
		{name: "empty map keeps everything", designated: map[string]string{}, nodeName: "n1", want: all},
		{
			name:       "designated elsewhere skipped, designated here and unlisted kept",
			designated: map[string]string{"s1": "n2", "s2": "n1"},
			nodeName:   "n1",
			want:       []string{"s2", "s3", "s4"},
		},
		{
			name:       "designated node no longer a replica keeps the shard",
			designated: map[string]string{"s4": "n3"},
			nodeName:   "n1",
			want:       all,
		},
		{
			name:       "all designated elsewhere",
			designated: map[string]string{"s1": "n3", "s2": "n3", "s3": "n3", "s4": "n2"},
			nodeName:   "n1",
			want:       []string{},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := filterDesignatedShards(all, tc.designated, replicas, tc.nodeName)
			assert.Equal(t, tc.want, got)
		})
	}
}
