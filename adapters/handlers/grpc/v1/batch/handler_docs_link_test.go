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

package batch

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/models"
)

func TestErrorMessage(t *testing.T) {
	ns := &models.Principal{Username: "u", Namespace: "https"}

	tests := []struct {
		name      string
		principal *models.Principal
		err       error
		want      string
	}{
		{
			// The namespace is stripped before the link is appended, so a
			// namespace named after the link's scheme leaves the link intact.
			name:      "documented error has the namespace stripped and the link appended",
			principal: ns,
			err:       fmt.Errorf("put object: https:Articles: cannot init shard: %w", enterrors.ErrNotEnoughMappings),
			want:      "put object: Articles: cannot init shard: not enough memory mappings (see https://docs.weaviate.io/e/core-mem001)",
		},
		{
			name:      "undocumented error is passed through",
			principal: ns,
			err:       fmt.Errorf("invalid object: something else"),
			want:      "invalid object: something else",
		},
		{
			name: "nil error renders as fmt's <nil> placeholder",
			want: "<nil>",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, errorMessage(tt.principal, tt.err))
		})
	}
}
