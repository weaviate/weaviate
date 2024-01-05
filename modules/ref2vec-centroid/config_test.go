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

package modcentroid

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/modules/ref2vec-centroid/config"
)

func TestConfigDefaults(t *testing.T) {
	def := New().ClassConfigDefaults()
	cfg := config.New(fakeClassConfig(def))

	assert.Equal(t, config.MethodDefault, cfg.CalculationMethod())
}

func TestConfigValidator(t *testing.T) {
	class := &models.Class{Class: "CentroidClass"}

	tests := []struct {
		name        string
		class       *models.Class
		classConfig moduletools.ClassConfig
		expectedErr error
	}{
		{
			name:  "valid config",
			class: class,
			classConfig: fakeClassConfig{
				"referenceProperties": []interface{}{"someRef"},
			},
		},
		{
			name:        "invalid config - required fields omitted",
			class:       class,
			classConfig: fakeClassConfig{},
			expectedErr: fmt.Errorf("validate %q: invalid config: must have at least "+
				"one value in the \"referenceProperties\" field",
				class.Class),
		},
		{
			name:  "invalid config - wrong type for referenceProperties",
			class: class,
			classConfig: fakeClassConfig{
				"referenceProperties": "someRef",
			},
			expectedErr: fmt.Errorf("validate %q: invalid config: expected array for "+
				"field \"referenceProperties\", got string",
				class.Class),
		},
		{
			name:  "invalid config - empty referenceProperties slice",
			class: class,
			classConfig: fakeClassConfig{
				"referenceProperties": []interface{}{},
			},
			expectedErr: fmt.Errorf("validate %q: invalid config: must have at least "+
				"one value in the \"referenceProperties\" field",
				class.Class),
		},
		{
			name:  "invalid config - non-string value in referenceProperties array",
			class: class,
			classConfig: fakeClassConfig{
				"referenceProperties": []interface{}{"someRef", 123},
			},
			expectedErr: fmt.Errorf("validate %q: invalid config: expected \"referenceProperties\" "+
				"to contain strings, found int: [someRef 123]",
				class.Class),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			mod := New()
			err := mod.ValidateClass(context.Background(), test.class, test.classConfig)
			if test.expectedErr != nil {
				assert.EqualError(t, err, test.expectedErr.Error())
			} else {
				assert.Nil(t, err)
			}
		})
	}
}
