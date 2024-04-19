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

package validation

import (
	"context"
	"strings"
	"testing"

	"github.com/go-openapi/strfmt"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/config"
)

const BEACON = "weaviate://localhost/"

var (
	UuidUpper = "4E5CD755-4F43-44C5-B23C-0C7D6F6C21E6"
	UuidLower = strings.ToLower(UuidUpper)
)

func TestValidationReferencesInObject(t *testing.T) {
	validator := New(fakeExists, &config.WeaviateConfig{}, nil)

	class := &models.Class{
		Class: "From",
		Properties: []*models.Property{
			{Name: "ref", DataType: []string{"To"}},
		},
	}

	obj := &models.Object{
		Class: "From",
		Properties: map[string]interface{}{
			"ref": []interface{}{
				map[string]interface{}{"beacon": BEACON + "To/" + UuidUpper},
			},
		},
	}

	err := validator.properties(context.Background(), class, obj, nil)
	require.Nil(t, err)
	require.Equal(t, obj.Properties.(map[string]interface{})["ref"].(models.MultipleRef)[0].Beacon.String(), BEACON+"To/"+UuidLower)
}

func TestValidationReference(t *testing.T) {
	validator := New(fakeExists, &config.WeaviateConfig{}, nil)

	cref := &models.SingleRef{Beacon: strfmt.URI(BEACON + "To/" + UuidUpper)}
	ref, err := validator.ValidateSingleRef(cref)
	require.Nil(t, err)
	require.Equal(t, ref.TargetID.String(), UuidLower)
}
