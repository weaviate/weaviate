//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package multishard

import (
	"github.com/go-openapi/strfmt"
	"github.com/semi-technologies/weaviate/entities/models"
)

const (
	MultiShardID1 strfmt.UUID = "aa44bbee-ca5f-4db7-a412-5fc6a23c534a"
	MultiShardID2 strfmt.UUID = "aa44bbee-ca5f-4db7-a412-5fc6a23c534b"
	MultiShardID3 strfmt.UUID = "aa44bbee-ca5f-4db7-a412-5fc6a23c534c"
)

func ClassContextionaryVectorizer() *models.Class {
	return class("text2vec-contextionary")
}

func ClassTransformersVectorizer() *models.Class {
	return class("text2vec-transformers")
}

func class(vectorizer string) *models.Class {
	return &models.Class{
		Class: "MultiShard",
		ModuleConfig: map[string]interface{}{
			vectorizer: map[string]interface{}{
				"vectorizeClassName": false,
			},
		},
		Properties: []*models.Property{
			{
				Name:     "name",
				DataType: []string{"string"},
				ModuleConfig: map[string]interface{}{
					vectorizer: map[string]interface{}{
						"vectorizePropertyName": false,
					},
				},
			},
		},
		ShardingConfig: map[string]interface{}{
			"actualCount":         float64(2),
			"actualVirtualCount":  float64(128),
			"desiredCount":        float64(2),
			"desiredVirtualCount": float64(128),
			"function":            "murmur3",
			"key":                 "_id",
			"strategy":            "hash",
			"virtualPerPhysical":  float64(128),
		},
	}
}

func Objects() []*models.Object {
	return []*models.Object{
		{
			Class: "MultiShard",
			ID:    MultiShardID1,
			Properties: map[string]interface{}{
				"name": "multi shard one",
			},
		},
		{
			Class: "MultiShard",
			ID:    MultiShardID2,
			Properties: map[string]interface{}{
				"name": "multi shard two",
			},
		},
		{
			Class: "MultiShard",
			ID:    MultiShardID3,
			Properties: map[string]interface{}{
				"name": "multi shard three",
			},
		},
	}
}
