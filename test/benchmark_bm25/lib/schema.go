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

package lib

import (
	"strings"

	"github.com/weaviate/weaviate/entities/models"
)

func SchemaFromDataset(ds Dataset) *models.Class {
	out := &models.Class{}
	out.Class = ClassNameFromDatasetID(ds.ID)
	out.VectorIndexConfig = map[string]interface{}{
		"skip": true,
	}
	out.Vectorizer = "none"
	out.InvertedIndexConfig = &models.InvertedIndexConfig{
		Stopwords: &models.StopwordConfig{
			Preset: "none",
		},
	}

	for _, prop := range ds.Corpus.IndexedProperties {
		t := true
		// all indexed props are indexed as text
		prop := &models.Property{
			Name:          SanitizePropName(prop),
			DataType:      []string{"text"},
			IndexInverted: &t,
		}

		out.Properties = append(out.Properties, prop)

	}

	for _, prop := range ds.Corpus.UnindexedProperties {
		// all indexed props are indexed as text
		f := false
		prop := &models.Property{
			Name:          SanitizePropName(prop),
			DataType:      []string{"string"},
			IndexInverted: &f,
			Tokenization:  "field",
		}

		out.Properties = append(out.Properties, prop)
	}

	filterProps := []*models.Property{
		{
			Name:     "modulo_10",
			DataType: []string{"int"},
		},
		{
			Name:     "modulo_100",
			DataType: []string{"int"},
		},
		{
			Name:     "modulo_1000",
			DataType: []string{"int"},
		},
	}

	out.Properties = append(out.Properties, filterProps...)

	return out
}

func ClassNameFromDatasetID(in string) string {
	if len(in) == 0 {
		panic("zero length dataset name")
	}

	return strings.ToUpper(string(in[0])) + strings.ToLower(in[1:])
}

func SanitizePropName(in string) string {
	if len(in) >= 2 && in[0] == '_' && in[1] != '_' {
		// single leading underscore is reseved, but we can append another one
		return "_" + in
	}

	return in
}
