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

package searchparams

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
)

// TestGetPropertyByName_DotNotation pins that a property is matched by the
// first dot-notation segment, so a nested reference resolves to its root
// property.
func TestGetPropertyByName_DotNotation(t *testing.T) {
	class := &models.Class{
		Class: "Test",
		Properties: []*models.Property{
			{Name: "plain"},
			{Name: "nested"},
		},
	}

	tests := []struct {
		propName string
		want     string
		wantErr  bool
	}{
		{propName: "plain", want: "plain"},
		{propName: "nested", want: "nested"},
		{propName: "nested.inner", want: "nested"},
		{propName: "nested.inner.deep", want: "nested"},
		{propName: "missing", wantErr: true},
		{propName: "missing.inner", wantErr: true},
		{propName: "", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.propName, func(t *testing.T) {
			got, err := GetPropertyByName(class, tt.propName)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got.Name)
		})
	}
}
