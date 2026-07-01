//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package modules

import (
	"context"
	"testing"
	"time"

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
	cfg := NewClassBasedModuleConfig(class, "my-module", "tenant", "", nil)
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
	cfg := NewClassBasedModuleConfig(class, "my-module", "tenant", "", nil)
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
	cfg := NewClassBasedModuleConfig(class, "my-module", "tenant", "", nil)
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

// TestCompareRevectorize_NonTextSourceProperties: a non-text source property is
// vectorized when source properties are set, so changing it must re-vectorize.
func TestCompareRevectorize_NonTextSourceProperties(t *testing.T) {
	class := &models.Class{
		Class: "MyClass",
		Properties: []*models.Property{
			{Name: "title", DataType: []string{schema.DataTypeText.String()}},
			{Name: "price", DataType: []string{schema.DataTypeNumber.String()}},
			{Name: "qty", DataType: []string{schema.DataTypeInt.String()}},
			{Name: "released", DataType: []string{schema.DataTypeDate.String()}},
			{Name: "active", DataType: []string{schema.DataTypeBoolean.String()}},
			{Name: "sizes", DataType: []string{schema.DataTypeNumberArray.String()}},
			{Name: "meta", DataType: []string{schema.DataTypeObject.String()}},
		},
		VectorConfig: map[string]models.VectorConfig{
			"v": {
				Vectorizer: map[string]any{
					"my-module": map[string]any{"vectorizeClassName": false},
				},
				VectorIndexType: "hnsw",
			},
		},
	}
	cfg := NewClassBasedModuleConfig(class, "my-module", "tenant", "", nil)
	module := newDummyText2VecModule("my-module", []string{"image", "video"})

	cases := []struct {
		name        string
		sourceProps []string
		oldProps    map[string]any
		newProps    map[string]any
		different   bool
	}{
		{name: "number unchanged", sourceProps: []string{"price"}, oldProps: map[string]any{"price": 9.99}, newProps: map[string]any{"price": 9.99}, different: false},
		{name: "number changed", sourceProps: []string{"price"}, oldProps: map[string]any{"price": 9.99}, newProps: map[string]any{"price": 19.99}, different: true},
		{name: "int unchanged", sourceProps: []string{"qty"}, oldProps: map[string]any{"qty": 1}, newProps: map[string]any{"qty": 1}, different: false},
		{name: "int changed", sourceProps: []string{"qty"}, oldProps: map[string]any{"qty": 1}, newProps: map[string]any{"qty": 2}, different: true},
		{name: "date changed", sourceProps: []string{"released"}, oldProps: map[string]any{"released": "2024-01-01T00:00:00Z"}, newProps: map[string]any{"released": "2025-01-01T00:00:00Z"}, different: true},
		{name: "bool changed", sourceProps: []string{"active"}, oldProps: map[string]any{"active": true}, newProps: map[string]any{"active": false}, different: true},
		{name: "number array unchanged", sourceProps: []string{"sizes"}, oldProps: map[string]any{"sizes": []float64{1, 2}}, newProps: map[string]any{"sizes": []float64{1, 2}}, different: false},
		{name: "number array changed", sourceProps: []string{"sizes"}, oldProps: map[string]any{"sizes": []float64{1, 2}}, newProps: map[string]any{"sizes": []float64{1, 3}}, different: true},
		{name: "mixed text+number source, only number changed", sourceProps: []string{"title", "price"}, oldProps: map[string]any{"title": "a", "price": 9.99}, newProps: map[string]any{"title": "a", "price": 19.99}, different: true},
		{name: "non-source number changed -> skip", sourceProps: []string{"title"}, oldProps: map[string]any{"title": "a", "price": 9.99}, newProps: map[string]any{"title": "a", "price": 19.99}, different: false},
		// representation drift: same logical value, different Go types -> must not re-vectorize.
		{name: "date drift time.Time vs RFC3339 string, unchanged", sourceProps: []string{"released"}, oldProps: map[string]any{"released": time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)}, newProps: map[string]any{"released": "2024-01-01T00:00:00Z"}, different: false},
		// sub-second precision is dropped by the corpus, so the same instant must not re-vectorize.
		{name: "date sub-second drift string(micros) vs time.Time, unchanged", sourceProps: []string{"released"}, oldProps: map[string]any{"released": "2024-01-01T12:30:45.123456Z"}, newProps: map[string]any{"released": time.Date(2024, 1, 1, 12, 30, 45, 123456000, time.UTC)}, different: false},
		{name: "date millisecond drift string vs time.Time, unchanged", sourceProps: []string{"released"}, oldProps: map[string]any{"released": "2024-01-01T12:30:45.123Z"}, newProps: map[string]any{"released": time.Date(2024, 1, 1, 12, 30, 45, 123000000, time.UTC)}, different: false},
		{name: "date changed at seconds despite sub-second noise", sourceProps: []string{"released"}, oldProps: map[string]any{"released": "2024-01-01T12:30:45.123456Z"}, newProps: map[string]any{"released": time.Date(2024, 1, 1, 12, 30, 46, 0, time.UTC)}, different: true},
		{name: "int drift int64 vs float64, unchanged", sourceProps: []string{"qty"}, oldProps: map[string]any{"qty": int64(5)}, newProps: map[string]any{"qty": float64(5)}, different: false},
		{name: "empty array drift []interface{} vs []float64, unchanged", sourceProps: []string{"sizes"}, oldProps: map[string]any{"sizes": []any{}}, newProps: map[string]any{"sizes": []float64{}}, different: false},
		{name: "presence change (source prop removed)", sourceProps: []string{"price"}, oldProps: map[string]any{"price": 9.99}, newProps: map[string]any{}, different: true},
		{name: "object source prop unchanged", sourceProps: []string{"meta"}, oldProps: map[string]any{"meta": map[string]any{"a": "b"}}, newProps: map[string]any{"meta": map[string]any{"a": "b"}}, different: false},
		{name: "object source prop changed", sourceProps: []string{"meta"}, oldProps: map[string]any{"meta": map[string]any{"a": "b"}}, newProps: map[string]any{"meta": map[string]any{"a": "c"}}, different: true},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			uid, _ := uuid.NewUUID()
			uidfmt := strfmt.UUID(uid.String())
			objNew := &models.Object{Class: class.Class, Properties: tt.newProps, ID: uidfmt}
			objsToReturn[uid.String()] = tt.oldProps
			different, _, _, err := reVectorize(context.Background(), cfg, module, objNew, class, tt.sourceProps, "", findObject, false)
			require.NoError(t, err)
			require.Equal(t, tt.different, different)
		})
	}
}

// TestCompareRevectorize_SkipIgnoredWithSourceProperties: with source_properties set,
// a listed skip:true property is still vectorized, so the comparator must compare it.
func TestCompareRevectorize_SkipIgnoredWithSourceProperties(t *testing.T) {
	class := &models.Class{
		Class:      "MyClass",
		Vectorizer: "my-module",
		Properties: []*models.Property{
			{Name: "title", DataType: []string{schema.DataTypeText.String()}},
			{
				Name:         "price",
				DataType:     []string{schema.DataTypeNumber.String()},
				ModuleConfig: map[string]any{"my-module": map[string]any{"skip": true}},
			},
		},
		VectorConfig: map[string]models.VectorConfig{
			"v": {
				Vectorizer:      map[string]any{"my-module": map[string]any{"vectorizeClassName": false}},
				VectorIndexType: "hnsw",
			},
		},
	}
	cfg := NewClassBasedModuleConfig(class, "my-module", "tenant", "", nil)
	module := newDummyText2VecModule("my-module", []string{"image", "video"})

	uid, _ := uuid.NewUUID()
	uidfmt := strfmt.UUID(uid.String())
	objsToReturn[uid.String()] = map[string]any{"title": "a", "price": 9.99}
	objNew := &models.Object{
		Class:      class.Class,
		Properties: map[string]any{"title": "a", "price": 19.99},
		ID:         uidfmt,
	}
	different, _, _, err := reVectorize(context.Background(), cfg, module, objNew, class, []string{"price"}, "", findObject, false)
	require.NoError(t, err)
	require.True(t, different)
}

// TestCompareRevectorize_BlobSourceProperty: a blob is a base64 string the corpus
// vectorizes like text, so a changed blob must re-vectorize.
func TestCompareRevectorize_BlobSourceProperty(t *testing.T) {
	class := &models.Class{
		Class: "MyClass",
		Properties: []*models.Property{
			{Name: "title", DataType: []string{schema.DataTypeText.String()}},
			{Name: "thumbnail", DataType: []string{schema.DataTypeBlob.String()}},
		},
		VectorConfig: map[string]models.VectorConfig{
			"v": {
				Vectorizer:      map[string]any{"my-module": map[string]any{"vectorizeClassName": false}},
				VectorIndexType: "hnsw",
			},
		},
	}
	cfg := NewClassBasedModuleConfig(class, "my-module", "tenant", "", nil)
	module := newDummyText2VecModule("my-module", []string{"image", "video"})

	cases := []struct {
		name        string
		sourceProps []string
		oldProps    map[string]any
		newProps    map[string]any
		different   bool
	}{
		{name: "blob source prop changed", sourceProps: []string{"thumbnail"}, oldProps: map[string]any{"thumbnail": "QQ=="}, newProps: map[string]any{"thumbnail": "Qg=="}, different: true},
		{name: "blob source prop unchanged", sourceProps: []string{"thumbnail"}, oldProps: map[string]any{"thumbnail": "QQ=="}, newProps: map[string]any{"thumbnail": "QQ=="}, different: false},
		{name: "blob changed, no source props", sourceProps: nil, oldProps: map[string]any{"title": "a", "thumbnail": "QQ=="}, newProps: map[string]any{"title": "a", "thumbnail": "Qg=="}, different: true},
		{name: "non-source blob changed -> skip", sourceProps: []string{"title"}, oldProps: map[string]any{"title": "a", "thumbnail": "QQ=="}, newProps: map[string]any{"title": "a", "thumbnail": "Qg=="}, different: false},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			uid, _ := uuid.NewUUID()
			uidfmt := strfmt.UUID(uid.String())
			objNew := &models.Object{Class: class.Class, Properties: tt.newProps, ID: uidfmt}
			objsToReturn[uid.String()] = tt.oldProps
			different, _, _, err := reVectorize(context.Background(), cfg, module, objNew, class, tt.sourceProps, "", findObject, false)
			require.NoError(t, err)
			require.Equal(t, tt.different, different)
		})
	}
}
