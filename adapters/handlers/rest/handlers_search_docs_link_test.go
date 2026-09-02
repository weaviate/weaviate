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

package rest

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	restsearch "github.com/weaviate/weaviate/adapters/handlers/rest/search"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/schema/namespacing"
)

func TestSearchErrPayloadDocsLink(t *testing.T) {
	tests := []struct {
		name      string
		principal *models.Principal
		err       error
		want      string
	}{
		{
			name: "undocumented error is passed through",
			err:  fmt.Errorf("explorer: get class: something else"),
			want: "explorer: get class: something else",
		},
		{
			name: "documented error gets the page appended",
			err:  fmt.Errorf("explorer: get class: cannot init shard: %w", enterrors.ErrNotEnoughMappings),
			want: "explorer: get class: cannot init shard: not enough memory mappings (see https://docs.weaviate.io/e/core-mem001)",
		},
		{
			name:      "namespace named after the link's scheme leaves the link intact",
			principal: &models.Principal{Username: "u", Namespace: "https"},
			err:       fmt.Errorf("explorer: get class: https:Articles: cannot init shard: %w", enterrors.ErrNotEnoughMappings),
			want:      "explorer: get class: Articles: cannot init shard: not enough memory mappings (see https://docs.weaviate.io/e/core-mem001)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// the search package strips the namespace before handing the error over
			apiErr := &restsearch.APIError{Status: http.StatusInternalServerError, Err: namespacing.StripErrForPrincipal(tt.principal, tt.err)}
			payload := searchErrPayload(apiErr)
			require.Len(t, payload.Error, 1)
			assert.Equal(t, tt.want, payload.Error[0].Message)
		})
	}
}
