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

package vectorizer

import (
	"context"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/moduletools"
	c "github.com/weaviate/weaviate/modules/text2vec-kserve/clients"
)

func Test_getVectorizationInput(t *testing.T) {
	type args struct {
		obj     *models.Object
		objDiff *moduletools.ObjectDiff
		cfg     moduletools.ClassConfig
	}
	tests := []struct {
		name            string
		args            args
		shouldVectorize bool
		texts           []string
	}{
		{
			name: "No diff, dont vectorize class name",
			args: args{
				obj: &models.Object{
					Class:      "Some class",
					Properties: nil,
					Vector:     nil,
				},
				objDiff: nil,
				cfg: &fakeClassConfig{
					classConfig: map[string]interface{}{
						"vectorizeClassName": false,
					},
				},
			},
			shouldVectorize: false,
			texts:           []string{},
		},
		{
			name: "No diff, vectorize class name",
			args: args{
				obj: &models.Object{
					Class:      "Some class",
					Properties: nil,
					Vector:     nil,
				},
				objDiff: nil,
				cfg: &fakeClassConfig{
					classConfig: map[string]interface{}{
						"vectorizeClassName": true,
					},
				},
			},
			shouldVectorize: true,
			texts:           []string{"some class"},
		},
		{
			name: "Text changed, dont vectorize class name",
			args: args{
				obj: &models.Object{
					Class: "Some class",
					Properties: map[string]interface{}{
						"text": "bar",
					},
					Vector: nil,
				},
				objDiff: moduletools.NewObjectDiff([]float32{1, 2, 3}).WithProp("text", "foo", "bar"),
				cfg: &fakeClassConfig{
					classConfig: map[string]interface{}{
						"vectorizeClassName": false,
					},
				},
			},
			shouldVectorize: true,
			texts:           []string{"bar"},
		},
		{
			name: "No diff, should vectorize property",
			args: args{
				obj: &models.Object{
					Class: "Some class",
					Properties: map[string]interface{}{
						"text": "foo",
					},
					Vector: nil,
				},
				objDiff: nil,
				cfg: &fakeClassConfig{
					classConfig: map[string]interface{}{
						"vectorizeClassName": false,
						"properties": []map[string]interface{}{{
							"name":     "text",
							"dataType": []string{"text"},
							"moduleConfig": map[string]interface{}{
								"text2vec-kserve": map[string]interface{}{
									"skip": "false",
								},
							},
						}},
					},
				},
			},
			shouldVectorize: true,
			texts:           []string{"foo"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			settings := NewClassSettings(tt.args.cfg)
			shouldVectorize, texts := getVectorizationInput(tt.args.obj, tt.args.objDiff, settings)
			assert.Equal(t, tt.shouldVectorize, shouldVectorize)
			if !reflect.DeepEqual(texts, tt.texts) {
				t.Errorf("getVectorizationInput() got = %v, want %v", texts, tt.texts)
			}
		})
	}
}

func TestVectorizer_Object(t *testing.T) {
	type fields struct {
		client c.Client
	}
	type args struct {
		ctx      context.Context
		obj      *models.Object
		objDiff  *moduletools.ObjectDiff
		settings ClassSettings
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := &Vectorizer{
				client: tt.fields.client,
			}
			if err := v.Object(tt.args.ctx, tt.args.obj, tt.args.objDiff, tt.args.settings); (err != nil) != tt.wantErr {
				t.Errorf("Vectorizer.Object() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
