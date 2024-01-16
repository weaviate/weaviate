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

package documents

import (
	"fmt"

	"github.com/go-openapi/strfmt"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema/crossref"
)

const (
	Document = "Document"
	Passage  = "Passage"
)

var DocumentIDs = []strfmt.UUID{
	"00000000-0000-0000-0000-000000000011",
	"00000000-0000-0000-0000-000000000012",
}

var PassageIDs = []strfmt.UUID{
	"00000000-0000-0000-0000-000000000001",
	"00000000-0000-0000-0000-000000000002",
	"00000000-0000-0000-0000-000000000003",
	"00000000-0000-0000-0000-000000000004",
	"00000000-0000-0000-0000-000000000005",
	"00000000-0000-0000-0000-000000000006",
	"00000000-0000-0000-0000-000000000007",
	"00000000-0000-0000-0000-000000000008",
}

var multiShardConfig = map[string]interface{}{
	"actualCount":         float64(4),
	"actualVirtualCount":  float64(128),
	"desiredCount":        float64(4),
	"desiredVirtualCount": float64(128),
	"function":            "murmur3",
	"key":                 "_id",
	"strategy":            "hash",
	"virtualPerPhysical":  float64(128),
}

var docTypes = []string{"private", "public"}

func ClassesContextionaryVectorizer(multishard bool) []*models.Class {
	return []*models.Class{
		document("text2vec-contextionary", multishard),
		passage("text2vec-contextionary", multishard),
	}
}

func document(vectorizer string, multishard bool) *models.Class {
	class := &models.Class{
		Class: Document,
		ModuleConfig: map[string]interface{}{
			vectorizer: map[string]interface{}{
				"vectorizeClassName": false,
			},
		},
		Properties: []*models.Property{
			{
				Name:     "title",
				DataType: []string{"text"},
				ModuleConfig: map[string]interface{}{
					vectorizer: map[string]interface{}{
						"vectorizePropertyName": false,
					},
				},
			},
			{
				Name:     "pageCount",
				DataType: []string{"int"},
				ModuleConfig: map[string]interface{}{
					vectorizer: map[string]interface{}{
						"vectorizePropertyName": false,
						"skip":                  true,
					},
				},
			},
			{
				Name:     "docType",
				DataType: []string{"text"},
				ModuleConfig: map[string]interface{}{
					vectorizer: map[string]interface{}{
						"vectorizePropertyName": false,
						"skip":                  true,
					},
				},
			},
		},
	}
	if multishard {
		class.ShardingConfig = multiShardConfig
	}
	return class
}

func passage(vectorizer string, multishard bool) *models.Class {
	class := &models.Class{
		Class: Passage,
		ModuleConfig: map[string]interface{}{
			vectorizer: map[string]interface{}{
				"vectorizeClassName": false,
			},
		},
		Properties: []*models.Property{
			{
				Name:     "content",
				DataType: []string{"text"},
				ModuleConfig: map[string]interface{}{
					vectorizer: map[string]interface{}{
						"vectorizePropertyName": false,
					},
				},
			},
			{
				Name:     "ofDocument",
				DataType: []string{Document},
				ModuleConfig: map[string]interface{}{
					vectorizer: map[string]interface{}{
						"vectorizePropertyName": false,
					},
				},
			},
		},
	}
	if multishard {
		class.ShardingConfig = multiShardConfig
	}
	return class
}

func Objects() []*models.Object {
	return append(documentObjects(), passageObjects()...)
}

func documentObjects() []*models.Object {
	getDocType := func(i int) string {
		if i == 0 {
			return docTypes[0]
		}
		return docTypes[1]
	}
	objects := make([]*models.Object, len(DocumentIDs))
	for i, id := range DocumentIDs {
		objects[i] = &models.Object{
			Class: Document,
			ID:    id,
			Properties: map[string]interface{}{
				"title":     fmt.Sprintf("Document title %v", i),
				"pageCount": 100 + i,
				"docType":   getDocType(i),
			},
		}
	}
	return objects
}

func passageObjects() []*models.Object {
	getDocumentID := func(i int) strfmt.UUID {
		if i < 6 {
			return DocumentIDs[0]
		}
		return DocumentIDs[1]
	}
	objects := make([]*models.Object, len(PassageIDs))
	for i, id := range PassageIDs {
		objects[i] = &models.Object{
			Class: Passage,
			ID:    id,
			Properties: map[string]interface{}{
				"content": fmt.Sprintf("Content of Passage %v", i),
				"ofDocument": []interface{}{
					map[string]interface{}{
						"beacon": crossref.NewLocalhost("Document", getDocumentID(i)).String(),
					},
				},
			},
		}
	}
	return objects
}
