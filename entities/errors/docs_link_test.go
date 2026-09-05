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

package errors

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestWithClusterID(t *testing.T) {
	t.Cleanup(func() { SetClusterIDSource(nil) })

	const page = DocsBaseURL + "/improve-your-cluster"
	tests := []struct {
		name   string
		source func() string
		want   string
	}{
		{name: "no source", source: nil, want: page},
		{name: "id not committed yet", source: func() string { return "" }, want: page},
		{
			name:   "id known",
			source: func() string { return "0198c0de-dead-beef-8000-000000000001" },
			want:   page + "?clusterid=0198c0de-dead-beef-8000-000000000001",
		},
		{
			name:   "id is query-escaped, never trusted",
			source: func() string { return "a b&c" },
			want:   page + "?clusterid=a+b%26c",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			SetClusterIDSource(tt.source)
			assert.Equal(t, tt.want, WithClusterID(page))
		})
	}
}
