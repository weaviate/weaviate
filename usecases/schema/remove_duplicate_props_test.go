//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
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
							DataType: []string{"int"},
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
							Name:     "prop_4",
							DataType: []string{"boolean"},
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

	expected := &models.Class{
		Class:           "MyClass",
		VectorIndexType: "hnsw",
		Properties: []*models.Property{
			{
				Name:     "prop_1",
				DataType: []string{"int"},
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
				Name:     "prop_4",
				DataType: []string{"boolean"},
			},
		},
	}

	assert.Equal(t, expected.Properties, actual.Properties)
}
