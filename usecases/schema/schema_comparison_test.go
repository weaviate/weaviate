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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/sharding"
)

func Test_SchemaComparison_Identical(t *testing.T) {
	left := &State{
		ShardingState: map[string]*sharding.State{
			"Foo": {
				IndexID: "Foo",
			},
		},

		ObjectSchema: &models.Schema{
			Classes: []*models.Class{
				{
					Class: "Foo",
					Properties: []*models.Property{
						{
							DataType: []string{"int"},
							Name:     "prop_1",
						},
					},
				},
			},
		},
	}

	right := &State{
		ShardingState: map[string]*sharding.State{
			"Foo": {
				IndexID: "Foo",
			},
		},

		ObjectSchema: &models.Schema{
			Classes: []*models.Class{
				{
					Class: "Foo",
					Properties: []*models.Property{
						{
							DataType: []string{"int"},
							Name:     "prop_1",
						},
					},
				},
			},
		},
	}

	assert.Len(t, Diff("left schema", left, "right schema", right), 0)
}

func Test_SchemaComparison_MismatchInClasses(t *testing.T) {
	left := &State{
		ShardingState: map[string]*sharding.State{
			"Foo": {
				IndexID: "Foo",
			},
		},

		ObjectSchema: &models.Schema{
			Classes: []*models.Class{
				{
					Class: "Foo",
					Properties: []*models.Property{
						{
							DataType: []string{"int"},
							Name:     "prop_1",
						},
					},
				},
			},
		},
	}

	right := &State{
		ShardingState: map[string]*sharding.State{
			"Foo": {
				IndexID: "Foo",
			},
		},

		ObjectSchema: &models.Schema{
			Classes: []*models.Class{},
		},
	}

	msgs := Diff("left schema", left, "right schema", right)
	assert.Greater(t, len(msgs), 0)
	assert.Contains(t, msgs, "class Foo exists in left schema, but not in right schema")
}

func Test_SchemaComparison_VariousMismatches(t *testing.T) {
	left := &State{
		ShardingState: map[string]*sharding.State{
			"Foo": {
				IndexID: "Foo",
			},
			"Foo2": {
				Physical: map[string]sharding.Physical{
					"abcd": {
						OwnsVirtual: []string{"v1"},
					},
				},
			},
			"Foo4": {
				Physical: map[string]sharding.Physical{
					"abcd": {},
				},
			},
		},

		ObjectSchema: &models.Schema{
			Classes: []*models.Class{
				{
					Class: "Foo",
					Properties: []*models.Property{
						{
							DataType: []string{"int"},
							Name:     "prop_1",
						},
						{
							DataType: []string{"text"},
							Name:     "prop_2",
						},
						{
							DataType:     schema.DataTypeText.PropString(),
							Tokenization: models.PropertyTokenizationWhitespace,
							Name:         "prop_4",
						},
					},
					Description: "foo",
					InvertedIndexConfig: &models.InvertedIndexConfig{
						IndexPropertyLength: true,
					},
					ModuleConfig: "bar",
					ReplicationConfig: &models.ReplicationConfig{
						Factor: 7,
					},
					ShardingConfig: map[string]interface{}{
						"desiredCount": 7,
					},
					VectorIndexConfig: map[string]interface{}{
						"ef": 1000,
					},
					VectorIndexType: "age-n-ass-double-u",
				},
				{Class: "Foo2"},
				{Class: "Foo3"},
				{Class: "Foo4"},
			},
		},
	}

	right := &State{
		ShardingState: map[string]*sharding.State{
			"Foo": {
				IndexID: "Foo",
			},
			"Foo2": {
				Physical: map[string]sharding.Physical{
					"xyz": {
						BelongsToNodes: []string{"n1"},
					},
				},
			},
			"Foo3": {
				Physical: map[string]sharding.Physical{
					"abcd": {},
				},
			},
		},

		ObjectSchema: &models.Schema{
			Classes: []*models.Class{
				{
					Class: "Foo",
					Properties: []*models.Property{
						{
							DataType: []string{"int"},
							Name:     "prop_1",
						},
						{
							DataType: []string{"bool"},
							Name:     "prop_3",
						},
						{
							DataType: []string{"text"},
							Name:     "prop_4",
						},
					},
					InvertedIndexConfig: &models.InvertedIndexConfig{
						IndexTimestamps: true,
					},
					ReplicationConfig: &models.ReplicationConfig{
						Factor: 8,
					},
					Vectorizer: "gpt-9",
				},
				{Class: "Foo2"},
				{Class: "Foo3"},
				{Class: "Foo4"},
			},
		},
	}

	actual := Diff("L", left, "R", right)

	expected := []string{
		"class Foo: property prop_2 exists in L, but not in R",
		"class Foo: property prop_3 exists in R, but not in L",
		"class Foo: property prop_4: mismatch: " +
			"L has {\"dataType\":[\"text\"],\"name\":\"prop_4\",\"tokenization\":\"whitespace\"}, but " +
			"R has {\"dataType\":[\"text\"],\"name\":\"prop_4\"}",
		"class Foo: description mismatch: " +
			"L has \"foo\", but R has \"\"",
		"class Foo: inverted index config mismatch: " +
			"L has {\"indexPropertyLength\":true}, " +
			"but R has {\"indexTimestamps\":true}",
		"class Foo: module config mismatch: " +
			"L has \"bar\", but R has null",
		"class Foo: replication config mismatch: " +
			"L has {\"factor\":7}, but R has {\"factor\":8}",
		"class Foo: sharding config mismatch: " +
			"L has {\"desiredCount\":7}, but R has null",
		"class Foo: vector index config mismatch: " +
			"L has {\"ef\":1000}, but R has null",
		"class Foo: vector index type mismatch: " +
			"L has \"age-n-ass-double-u\", but R has \"\"",
		"class Foo: vectorizer mismatch: " +
			"L has \"\", but R has \"gpt-9\"",
		"class Foo3: missing sharding state in L",
		"class Foo4: missing sharding state in R",
		"class Foo2: sharding state mismatch: " +
			"L has {\"indexID\":\"\",\"config\":{\"virtualPerPhysical\":0,\"desiredCount\":0,\"actualCount\":0,\"desiredVirtualCount\":0,\"actualVirtualCount\":0,\"key\":\"\",\"strategy\":\"\",\"function\":\"\"},\"physical\":{\"abcd\":{\"name\":\"\",\"ownsVirtual\":[\"v1\"],\"ownsPercentage\":0}},\"virtual\":null,\"partitioningEnabled\":false}, " +
			"but R has {\"indexID\":\"\",\"config\":{\"virtualPerPhysical\":0,\"desiredCount\":0,\"actualCount\":0,\"desiredVirtualCount\":0,\"actualVirtualCount\":0,\"key\":\"\",\"strategy\":\"\",\"function\":\"\"},\"physical\":{\"xyz\":{\"name\":\"\",\"ownsPercentage\":0,\"belongsToNodes\":[\"n1\"]}},\"virtual\":null,\"partitioningEnabled\":false}",
	}

	for _, exp := range expected {
		assert.Contains(t, actual, exp)
	}
}
