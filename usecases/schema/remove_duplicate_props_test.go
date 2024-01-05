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

package schema

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
)

func TestStartupWithDuplicateProps(t *testing.T) {
	sch := State{
		ObjectSchema: &models.Schema{
			Classes: []*models.Class{
				{
					Class:           "MyClass",
					VectorIndexType: "hnsw",
					Properties: []*models.Property{
						{
							Name:     "prop_1",
							DataType: schema.DataTypeInt.PropString(),
						},
						{
							Name:     "prop_2",
							DataType: []string{"Ref"},
						},
						{
							Name:     "prop_2",
							DataType: []string{"Ref"},
						},
						{
							Name:     "prop_3",
							DataType: []string{"Ref"},
						},
						{
							Name:     "prop_2",
							DataType: []string{"Ref"},
						},
						{
							Name:     "prOP_2",
							DataType: []string{"Ref"},
						},
						{
							Name:     "prop_4",
							DataType: schema.DataTypeBoolean.PropString(),
						},
					},
				},
			},
		},
	}
	m, err := newManagerWithClusterAndTx(t, &fakeClusterState{
		hosts: []string{"node1"},
	},
		&fakeTxClient{}, &sch)
	require.Nil(t, err)

	actual, err := m.GetClass(context.Background(), nil, "MyClass")
	require.Nil(t, err)

	vTrue := true
	vFalse := false
	expected := &models.Class{
		Class:           "MyClass",
		VectorIndexType: "hnsw",
		Properties: []*models.Property{
			{
				Name:            "prop_1",
				DataType:        schema.DataTypeInt.PropString(),
				IndexFilterable: &vTrue,
				IndexSearchable: &vFalse,
			},
			{
				Name:            "prop_2",
				DataType:        []string{"Ref"},
				IndexFilterable: &vTrue,
				IndexSearchable: &vFalse,
			},
			{
				Name:            "prop_3",
				DataType:        []string{"Ref"},
				IndexFilterable: &vTrue,
				IndexSearchable: &vFalse,
			},
			{
				Name:            "prop_4",
				DataType:        schema.DataTypeBoolean.PropString(),
				IndexFilterable: &vTrue,
				IndexSearchable: &vFalse,
			},
		},
	}

	assert.Equal(t, expected.Properties, actual.Properties)
}
