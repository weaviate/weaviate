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

package modules

import (
	"context"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/search"
)

var objsToReturn = make(map[string]interface{})

func findObject(ctx context.Context, class string, id strfmt.UUID,
	props search.SelectProperties, adds additional.Properties, tenant string,
) (*search.Result, error) {
	obj, ok := objsToReturn[id.String()]
	if !ok {
		return nil, nil
	}

	return &search.Result{Schema: obj}, nil
}

func TestCompareRevectorize(t *testing.T) {
	class := &models.Class{
		Class:      "MyClass",
		Vectorizer: "my-module",
		Properties: []*models.Property{
			{Name: "text", DataType: []string{schema.DataTypeText.String()}},
			{Name: "text_array", DataType: []string{schema.DataTypeTextArray.String()}},
			{Name: "text", DataType: []string{schema.DataTypeText.String()}},
			{Name: "image", DataType: []string{schema.DataTypeBlob.String()}},
			{Name: "number", DataType: []string{schema.DataTypeInt.String()}},
			{Name: "text_not_vectorized", DataType: []string{schema.DataTypeText.String()}, ModuleConfig: map[string]interface{}{"my-module": map[string]interface{}{"skip": true}}},
		},
	}
	cfg := NewClassBasedModuleConfig(class, "my-module", "tenant", "")
	module := newDummyText2VecModule("my-module", []string{"image", "video"})

	cases := []struct {
		name      string
		oldProps  map[string]interface{}
		newProps  map[string]interface{}
		different bool
		disabled  bool
	}{
		{name: "same text prop", oldProps: map[string]interface{}{"text": "value1"}, newProps: map[string]interface{}{"text": "value1"}, different: false},
		{name: "different text prop", oldProps: map[string]interface{}{"text": "value1"}, newProps: map[string]interface{}{"text": "value2"}, different: true},
		{name: "different text - not vectorized", oldProps: map[string]interface{}{"text_not_vectorized": "value1"}, newProps: map[string]interface{}{"text_not_vectorized": "value2"}, different: false},
		{name: "same text array prop", oldProps: map[string]interface{}{"text_array": []string{"first sentence", "second long sentence"}}, newProps: map[string]interface{}{"text_array": []string{"first sentence", "second long sentence"}}, different: false},
		{name: "different text array prop", oldProps: map[string]interface{}{"text_array": []string{"first sentence", "second long sentence"}}, newProps: map[string]interface{}{"text_array": []string{"first sentence", "second different sentence"}}, different: true},
		{name: "different text array prop length", oldProps: map[string]interface{}{"text_array": []string{"first sentence", "second long sentence"}}, newProps: map[string]interface{}{"text_array": []string{"first sentence"}}, different: true},
		{name: "old object not present", oldProps: nil, newProps: map[string]interface{}{"text": "value1"}, different: true},
		{name: "changed prop does not matter", oldProps: map[string]interface{}{"number": 2}, newProps: map[string]interface{}{"number": 1}, different: false},
		{name: "media prop changed", oldProps: map[string]interface{}{"image": "abc"}, newProps: map[string]interface{}{"image": "def"}, different: true},
		{name: "many props changed", oldProps: map[string]interface{}{"image": "abc", "text": "abc", "text_array": []string{"abc"}}, newProps: map[string]interface{}{"image": "def", "text": "def", "text_array": []string{"def"}}, different: true},
		{name: "many props - only irrelevant changed", oldProps: map[string]interface{}{"image": "abc", "text": "abc", "text_array": []string{"abc"}, "number": 1}, newProps: map[string]interface{}{"image": "abc", "text": "abc", "text_array": []string{"abc"}, "number": 2}, different: false},
		{name: "new props are nil", oldProps: map[string]interface{}{"text": "value1"}, newProps: nil, different: true},
		{name: "same text prop, but feature globally disabled", oldProps: map[string]interface{}{"text": "value1"}, newProps: map[string]interface{}{"text": "value1"}, disabled: true, different: true},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			uid, _ := uuid.NewUUID()
			uidfmt := strfmt.UUID(uid.String())
			objNew := &models.Object{Class: class.Class, Properties: tt.newProps, ID: uidfmt}
			if tt.oldProps != nil {
				objsToReturn[uid.String()] = tt.oldProps
			}
			different, _, _, err := reVectorize(context.Background(), cfg, module, objNew, class, nil, "", findObject, tt.disabled)
			require.NoError(t, err)
			require.Equal(t, different, tt.different)
		})
	}
}

func TestCompareRevectorizeNamedVectors(t *testing.T) {
	class := &models.Class{
		Class: "MyClass",
		Properties: []*models.Property{
			{Name: "text", DataType: []string{schema.DataTypeText.String()}},
			{Name: "text_array", DataType: []string{schema.DataTypeTextArray.String()}},
		},
		VectorConfig: map[string]models.VectorConfig{
			"text": {
				Vectorizer: map[string]interface{}{
					"my-module": map[string]interface{}{
						"vectorizeClassName": false,
						"properties":         []string{"text"},
					},
				},
				VectorIndexType: "hnsw",
			},
			"text_array": {
				Vectorizer: map[string]interface{}{
					"my-module": map[string]interface{}{
						"vectorizeClassName": false,
						"properties":         []string{"text_array"},
					},
				},
				VectorIndexType: "hnsw",
			},
			"all": {
				Vectorizer: map[string]interface{}{
					"my-module": map[string]interface{}{
						"vectorizeClassName": false,
					},
				},
				VectorIndexType: "hnsw",
			},
			"all_explicit": {
				Vectorizer: map[string]interface{}{
					"my-module": map[string]interface{}{
						"vectorizeClassName": false,
					},
				},
				VectorIndexType: "hnsw",
			},
		},
	}
	cfg := NewClassBasedModuleConfig(class, "my-module", "tenant", "")
	module := newDummyText2VecModule("my-module", []string{"image", "video"})

	cases := []struct {
		name          string
		oldProps      map[string]interface{}
		newProps      map[string]interface{}
		targetVectors []string
		different     bool
	}{
		{name: "same text prop, part of target vec", oldProps: map[string]interface{}{"text": "value1"}, newProps: map[string]interface{}{"text": "value1"}, targetVectors: []string{"text"}, different: false},
		{name: "different text prop, part of target vec", oldProps: map[string]interface{}{"text": "value1"}, newProps: map[string]interface{}{"text": "value2"}, targetVectors: []string{"text"}, different: true},
		{name: "different text prop, not part of target vec", oldProps: map[string]interface{}{"text": "value1"}, newProps: map[string]interface{}{"text": "value2"}, targetVectors: []string{"text_array"}, different: false},
		{name: "multiple props text prop, not part of target vec", oldProps: map[string]interface{}{"text": "value1", "image": "abc"}, newProps: map[string]interface{}{"text": "value2", "image": "def"}, targetVectors: []string{"text_array"}, different: false},
		{name: "multiple props text prop, one is part of text prop", oldProps: map[string]interface{}{"text": "value1", "image": "abc"}, newProps: map[string]interface{}{"text": "value2", "image": "def"}, targetVectors: []string{"text_array", "image"}, different: false},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			uid, _ := uuid.NewUUID()
			uidfmt := strfmt.UUID(uid.String())
			objNew := &models.Object{Class: class.Class, Properties: tt.newProps, ID: uidfmt}
			if tt.oldProps != nil {
				objsToReturn[uid.String()] = tt.oldProps
			}
			disabled := false
			different, _, _, err := reVectorize(context.Background(), cfg, module, objNew, class, tt.targetVectors, "", findObject, disabled)
			require.NoError(t, err)
			require.Equal(t, different, tt.different)
		})
	}
}

func TestCompareRevectorizeDisabled(t *testing.T) {
	class := &models.Class{
		Class: "MyClass",
		Properties: []*models.Property{
			{Name: "text", DataType: []string{schema.DataTypeText.String()}},
		},
		VectorConfig: map[string]models.VectorConfig{
			"text": {
				Vectorizer: map[string]interface{}{
					"my-module": map[string]interface{}{
						"vectorizeClassName": false,
						"properties":         []string{"text"},
					},
				},
				VectorIndexType: "hnsw",
			},
		},
	}
	cfg := NewClassBasedModuleConfig(class, "my-module", "tenant", "")
	module := newDummyText2VecModule("my-module", []string{"image", "video"})

	props := map[string]interface{}{
		"text": "value1",
	}
	uid, _ := uuid.NewUUID()
	uidfmt := strfmt.UUID(uid.String())
	objNew := &models.Object{Class: class.Class, Properties: props, ID: uidfmt}
	disabled := true
	findObjectMock := func(ctx context.Context, class string, id strfmt.UUID,
		props search.SelectProperties, adds additional.Properties, tenant string,
	) (*search.Result, error) {
		panic("why did you call me?")
	}
	different, _, _, err := reVectorize(context.Background(), cfg, module, objNew, class, []string{"text"}, "", findObjectMock, disabled)
	require.NoError(t, err)
	require.Equal(t, different, true)
}
