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
	"strings"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/entities/schema"
	modgoogle "github.com/weaviate/weaviate/modules/text2vec-google"
	googlevectorizer "github.com/weaviate/weaviate/modules/text2vec-google/vectorizer"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/modulecomponents"
)

type recordingGoogleClient struct {
	inputs []string
}

func (c *recordingGoogleClient) VectorizeWithTitleProperty(ctx context.Context,
	input []string, titlePropertyValue string, cfg moduletools.ClassConfig,
) (*modulecomponents.VectorizationResult[[]float32], error) {
	c.inputs = append(c.inputs, input...)
	return &modulecomponents.VectorizationResult[[]float32]{Vector: [][]float32{{1}}}, nil
}

func (c *recordingGoogleClient) VectorizeQuery(ctx context.Context,
	input []string, cfg moduletools.ClassConfig,
) (*modulecomponents.VectorizationResult[[]float32], error) {
	return nil, nil
}

const googleVector = "gemini"

func googleKeyedClass(moduleKey, targetVector string, sourceProperties []string) *models.Class {
	settings := map[string]any{"vectorizeClassName": false}
	if sourceProperties != nil {
		settings["properties"] = sourceProperties
	}
	class := &models.Class{
		Class: "Docs",
		Properties: []*models.Property{
			{
				Name: "body", DataType: schema.DataTypeText.PropString(),
				ModuleConfig: map[string]any{moduleKey: map[string]any{"vectorizePropertyName": true}},
			},
			{
				Name: "secret", DataType: schema.DataTypeText.PropString(),
				ModuleConfig: map[string]any{moduleKey: map[string]any{"skip": true}},
			},
		},
	}
	if targetVector == "" {
		class.Vectorizer = moduleKey
		class.ModuleConfig = map[string]any{moduleKey: settings}
		return class
	}
	class.VectorConfig = map[string]models.VectorConfig{
		targetVector: {VectorIndexType: "hnsw", Vectorizer: map[string]any{moduleKey: settings}},
	}
	return class
}

func providerWithGoogleModule(t *testing.T) *Provider {
	t.Helper()
	logger, _ := test.NewNullLogger()
	p := NewProvider(logger, config.Config{})
	p.Register(modgoogle.New())
	return p
}

// Documents current behaviour, not the behaviour we want.
// Property settings stored under text2vec-palm have no effect.
func TestPalmKeyedPropertySettingsAreIgnoredWhenVectorizing(t *testing.T) {
	shapes := []struct {
		name             string
		targetVector     string
		sourceProperties []string
	}{
		{name: "legacy"},
		{name: "named vector", targetVector: googleVector},
		{name: "named vector with a properties list", targetVector: googleVector, sourceProperties: []string{"body"}},
	}
	keys := []struct {
		moduleKey            string
		skipHonoured         bool
		propertyNameIncluded bool
	}{
		{moduleKey: modgoogle.LegacyName, skipHonoured: false, propertyNameIncluded: false},
		{moduleKey: modgoogle.Name, skipHonoured: true, propertyNameIncluded: true},
	}
	for _, shape := range shapes {
		for _, key := range keys {
			t.Run(shape.name+"/"+key.moduleKey, func(t *testing.T) {
				class := googleKeyedClass(key.moduleKey, shape.targetVector, shape.sourceProperties)
				var modConfig map[string]any
				if shape.targetVector == "" {
					modConfig = class.ModuleConfig.(map[string]any)
				} else {
					modConfig = class.VectorConfig[shape.targetVector].Vectorizer.(map[string]any)
				}
				found := providerWithGoogleModule(t).getModule(modConfig)
				require.NotNil(t, found)
				require.Equal(t, modgoogle.Name, found.Name())

				client := &recordingGoogleClient{}
				cfg := NewClassBasedModuleConfig(class, found.Name(), "", shape.targetVector, nil)
				object := &models.Object{Class: class.Class, Properties: map[string]any{"body": "visible text", "secret": "hidden text"}}
				_, _, err := googlevectorizer.New(client).Object(context.Background(), object, cfg)
				require.NoError(t, err)
				require.Len(t, client.inputs, 1)
				embedded := client.inputs[0]

				skipHonoured := !strings.Contains(embedded, "hidden text")
				if shape.sourceProperties != nil {
					require.True(t, skipHonoured, "embedded text: %q", embedded)
				} else {
					require.Equal(t, key.skipHonoured, skipHonoured, "embedded text: %q", embedded)
				}
				require.Equal(t, key.propertyNameIncluded, strings.Contains(embedded, "body visible text"), "embedded text: %q", embedded)
			})
		}
	}
}

// Documents current behaviour, not the behaviour we want.
// A properties list stored under text2vec-palm is ignored by the revectorize check.
func TestPalmKeyedSourcePropertiesAreIgnoredByTheRevectorizeCheck(t *testing.T) {
	tests := []struct {
		moduleKey       string
		wantProperties  []string
		wantRevectorize bool
	}{
		{moduleKey: modgoogle.LegacyName, wantProperties: nil, wantRevectorize: true},
		{moduleKey: modgoogle.Name, wantProperties: []string{"body"}, wantRevectorize: false},
	}
	for _, tt := range tests {
		t.Run(tt.moduleKey, func(t *testing.T) {
			p := providerWithGoogleModule(t)
			class := googleKeyedClass(tt.moduleKey, googleVector, []string{"body"})
			class.Properties = append(class.Properties, &models.Property{Name: "notes", DataType: schema.DataTypeText.PropString()})
			modConfig := class.VectorConfig[googleVector].Vectorizer.(map[string]any)
			found := p.getModule(modConfig)
			require.NotNil(t, found)

			sourceProperties := p.sourcePropertiesFromModuleConfig(modConfig, found.Name())
			require.Equal(t, tt.wantProperties, sourceProperties)

			id := strfmt.UUID(uuid.NewString())
			objsToReturn[id.String()] = map[string]any{"body": "same", "notes": "before"}
			updated := &models.Object{Class: class.Class, ID: id, Properties: map[string]any{"body": "same", "notes": "after"}}
			cfg := NewClassBasedModuleConfig(class, found.Name(), "", googleVector, nil)
			revectorize, _, _, err := reVectorize(context.Background(), cfg, newDummyText2VecModule(found.Name(), nil),
				updated, class, sourceProperties, googleVector, findObject, false)
			require.NoError(t, err)
			require.Equal(t, tt.wantRevectorize, revectorize)
		})
	}
}
