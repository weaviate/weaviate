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

package modtransformers

import (
	"context"
	"testing"

	"github.com/sirupsen/logrus"
	ltest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
)

func TestConfigDefaults(t *testing.T) {
	t.Run("for properties", func(t *testing.T) {
		def := New().ClassConfigDefaults()

		assert.Equal(t, true, def["vectorizeClassName"])
		assert.Equal(t, "masked_mean", def["poolingStrategy"])
	})

	t.Run("for the class", func(t *testing.T) {
		dt := schema.DataTypeText
		def := New().PropertyConfigDefaults(&dt)
		assert.Equal(t, false, def["vectorizePropertyName"])
		assert.Equal(t, false, def["skip"])
	})
}

func TestConfigValidator(t *testing.T) {
	t.Run("all usable props no-indexed", func(t *testing.T) {
		t.Run("all schema vectorization turned off", func(t *testing.T) {
			class := &models.Class{
				Vectorizer: "text2vec-contextionary",
				Class:      "ValidName",
				Properties: []*models.Property{
					{
						DataType: []string{"text"},
						Name:     "description",
					},
					{
						DataType:     schema.DataTypeText.PropString(),
						Tokenization: models.PropertyTokenizationWhitespace,
						Name:         "name",
					},
					{
						DataType: []string{"int"},
						Name:     "amount",
					},
				},
			}

			logger, _ := ltest.NewNullLogger()
			v := NewConfigValidator(logger)
			err := v.Do(context.Background(), class, nil, &fakeIndexChecker{
				vectorizePropertyName: false,
				vectorizeClassName:    false,
				propertyIndexed:       false,
			})
			assert.NotNil(t, err)
		})
	})
}

func TestConfigValidator_RiskOfDuplicateVectors(t *testing.T) {
	type test struct {
		name          string
		in            *models.Class
		expectWarning bool
		indexChecker  *fakeIndexChecker
	}

	tests := []test{
		{
			name: "usable properties",
			in: &models.Class{
				Class: "ValidName",
				Properties: []*models.Property{
					{
						DataType: []string{string(schema.DataTypeText)},
						Name:     "textProp",
					},
				},
			},
			expectWarning: false,
			indexChecker: &fakeIndexChecker{
				vectorizePropertyName: false,
				vectorizeClassName:    true,
				propertyIndexed:       true,
			},
		},
		{
			name: "no properties",
			in: &models.Class{
				Class: "ValidName",
			},
			expectWarning: true,
			indexChecker: &fakeIndexChecker{
				vectorizePropertyName: false,
				vectorizeClassName:    true,
				propertyIndexed:       false,
			},
		},
		{
			name: "usable properties, but they are no-indexed",
			in: &models.Class{
				Class: "ValidName",
				Properties: []*models.Property{
					{
						DataType: []string{string(schema.DataTypeText)},
						Name:     "textProp",
					},
				},
			},
			expectWarning: true,
			indexChecker: &fakeIndexChecker{
				vectorizePropertyName: false,
				vectorizeClassName:    true,
				propertyIndexed:       false,
			},
		},
		{
			name: "only unusable properties",
			in: &models.Class{
				Class: "ValidName",
				Properties: []*models.Property{
					{
						DataType: []string{string(schema.DataTypeInt)},
						Name:     "intProp",
					},
				},
			},
			expectWarning: true,
			indexChecker: &fakeIndexChecker{
				vectorizePropertyName: false,
				vectorizeClassName:    true,
				propertyIndexed:       false,
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			logger, hook := ltest.NewNullLogger()
			v := NewConfigValidator(logger)
			err := v.Do(context.Background(), test.in, nil, test.indexChecker)
			require.Nil(t, err)

			entry := hook.LastEntry()
			if test.expectWarning {
				require.NotNil(t, entry)
				assert.Equal(t, logrus.WarnLevel, entry.Level)
			} else {
				assert.Nil(t, entry)
			}
		})
	}
}

type fakeIndexChecker struct {
	vectorizeClassName    bool
	vectorizePropertyName bool
	propertyIndexed       bool
}

func (f *fakeIndexChecker) VectorizeClassName() bool {
	return f.vectorizeClassName
}

func (f *fakeIndexChecker) VectorizePropertyName(propName string) bool {
	return f.vectorizePropertyName
}

func (f *fakeIndexChecker) PropertyIndexed(propName string) bool {
	return f.propertyIndexed
}
