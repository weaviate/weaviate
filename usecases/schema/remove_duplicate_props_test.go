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
