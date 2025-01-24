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

package t2vbigram

import (
	"context"
	"testing"

	"github.com/sirupsen/logrus"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/handlers/rest/state"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/modules"
)

func TestBigramModule_Name(t *testing.T) {
	mod := New()
	assert.Equal(t, Name, mod.Name())
}

func TestBigramModule_Type(t *testing.T) {
	mod := New()
	assert.Equal(t, modulecapabilities.Text2Vec, mod.Type())
}

func TestBigramModule_Init(t *testing.T) {
	t.Setenv("BIGRAM", "alphabet")
	mod := New()
	params := newFakeModuleParams("data")
	err := mod.Init(context.Background(), params)
	assert.NoError(t, err)
	assert.Equal(t, "alphabet", mod.activeVectoriser)
}

type fakeModuleParams struct {
	logger   logrus.FieldLogger
	provider fakeStorageProvider
	config   config.Config
	appState *state.State
}

func newFakeModuleParams(dataPath string) *fakeModuleParams {
	logger, _ := logrustest.NewNullLogger()
	return &fakeModuleParams{
		logger:   logger,
		provider: fakeStorageProvider{dataPath: dataPath},
	}
}

func (f *fakeModuleParams) GetStorageProvider() moduletools.StorageProvider {
	return &f.provider
}

func (f *fakeModuleParams) GetAppState() interface{} {
	return f.appState
}

func (f *fakeModuleParams) GetLogger() logrus.FieldLogger {
	return f.logger
}

func (f *fakeModuleParams) GetConfig() config.Config {
	return f.config
}

type fakeStorageProvider struct {
	dataPath string
}

func (f *fakeStorageProvider) Storage(name string) (moduletools.Storage, error) {
	return nil, nil
}

func (f *fakeStorageProvider) DataPath() string {
	return f.dataPath
}

func TestBigramModule_VectorizeInput(t *testing.T) {
	mod := New()
	mod.activeVectoriser = "alphabet"
	input := "hello world"
	expectedVector, _ := alphabet2Vector(input)
	cfg := modules.NewClassBasedModuleConfig(&models.Class{}, mod.Name(), "", "")
	vector, err := mod.VectorizeInput(context.Background(), input, cfg)
	assert.NoError(t, err)
	assert.Equal(t, expectedVector, vector)
}

func TestText2Vec(t *testing.T) {
	input := "hello world"
	activeVectoriser := "alphabet"
	expectedVector, _ := alphabet2Vector(input)
	vector, err := text2vec(input, activeVectoriser)
	assert.NoError(t, err)
	assert.Equal(t, expectedVector, vector)

	activeVectoriser = "trigram"
	expectedVector, _ = trigramVector(input)
	vector, err = text2vec(input, activeVectoriser)
	assert.NoError(t, err)
	assert.Equal(t, expectedVector, vector)

	activeVectoriser = "bytepairs"
	expectedVector, _ = bytePairs2Vector(input)
	vector, err = text2vec(input, activeVectoriser)
	assert.NoError(t, err)
	assert.Equal(t, expectedVector, vector)

	activeVectoriser = "mod26"
	expectedVector, _ = mod26Vector(input)
	vector, err = text2vec(input, activeVectoriser)
	assert.NoError(t, err)
	assert.Equal(t, expectedVector, vector)
}

func TestAlphabet2Vector(t *testing.T) {
	input := "hello world"
	vector, err := alphabet2Vector(input)
	assert.NoError(t, err)
	assert.NotNil(t, vector)
	assert.Equal(t, 26*26, len(vector))
}

func TestMod26Vector(t *testing.T) {
	input := "hello world"
	vector, err := mod26Vector(input)
	assert.NoError(t, err)
	assert.NotNil(t, vector)
	assert.Equal(t, 26*26, len(vector))
}

func TestTrigramVector(t *testing.T) {
	input := "hello world"
	vector, err := trigramVector(input)
	assert.NoError(t, err)
	assert.NotNil(t, vector)
	assert.Equal(t, 26*26*26, len(vector))
}

func TestBytePairs2Vector(t *testing.T) {
	input := "hello world"
	vector, err := bytePairs2Vector(input)
	assert.NoError(t, err)
	assert.NotNil(t, vector)
	assert.Equal(t, 256*256-1, len(vector))
}

func TestStripNonAlphabets(t *testing.T) {
	input := "hello, world!"
	expected := "helloworld"
	output, err := stripNonAlphabets(input)
	require.NoError(t, err)
	assert.Equal(t, expected, output)
}

func TestAddVector(t *testing.T) {
	mod := New()
	vector := []float32{1, 2, 3}
	err := mod.AddVector("hello", vector)
	assert.NoError(t, err)
	assert.Equal(t, vector, mod.vectors["hello"])
}

func TestBigramModule_VectorizeInput_EmptyInput(t *testing.T) {
	mod := New()
	input := ""
	cfg := modules.NewClassBasedModuleConfig(&models.Class{}, mod.Name(), "", "")
	vector, err := mod.VectorizeInput(context.Background(), input, cfg)
	assert.Error(t, err)
	assert.Nil(t, vector)
}

func TestBigramModule_VectorizeInput_UnknownVectoriser(t *testing.T) {
	mod := New()
	mod.activeVectoriser = "unknown"
	input := "hello world"
	cfg := modules.NewClassBasedModuleConfig(&models.Class{}, mod.Name(), "", "")
	vector, err := mod.VectorizeInput(context.Background(), input, cfg)
	assert.Error(t, err)
	assert.Nil(t, vector)
}

func TestBigramModule_VectorizeObject(t *testing.T) {
	mod := New()
	mod.activeVectoriser = "alphabet"
	obj := &models.Object{
		Class: "Test",
		Properties: map[string]interface{}{
			"text": "hello world",
		},
	}
	cfg := modules.NewClassBasedModuleConfig(&models.Class{}, mod.Name(), "", "")
	vector, _, err := mod.VectorizeObject(context.Background(), obj, cfg)
	assert.NoError(t, err)
	assert.NotNil(t, vector)
}

func TestBigramModule_VectorizeBatch(t *testing.T) {
	mod := New()
	mod.activeVectoriser = "alphabet"
	objs := []*models.Object{
		{
			Class: "Test",
			Properties: map[string]interface{}{
				"text": "hello world",
			},
		},
		{
			Class: "Test",
			Properties: map[string]interface{}{
				"text": "hello world",
			},
		},
	}
	skipObject := []bool{false, false}
	cfg := modules.NewClassBasedModuleConfig(&models.Class{}, mod.Name(), "", "")
	vectors, _, errors := mod.VectorizeBatch(context.Background(), objs, skipObject, cfg)
	assert.Len(t, vectors, 2)
	assert.Len(t, errors, 0)
}

func TestBigramModule_VectorizeBatch_SkipObject(t *testing.T) {
	mod := New()
	mod.activeVectoriser = "alphabet"
	objs := []*models.Object{
		{
			Class: "Test",
			Properties: map[string]interface{}{
				"text": "hello world",
			},
		},
		{
			Class: "Test",
			Properties: map[string]interface{}{
				"text": "hello world",
			},
		},
	}
	skipObject := []bool{false, true}
	cfg := modules.NewClassBasedModuleConfig(&models.Class{}, mod.Name(), "", "")
	vectors, _, errors := mod.VectorizeBatch(context.Background(), objs, skipObject, cfg)
	assert.Len(t, vectors, 1)
	assert.Len(t, errors, 0)
}

func TestBigramModule_VectorizeBatch_Error(t *testing.T) {
	mod := New()
	mod.activeVectoriser = "alphabet"
	objs := []*models.Object{
		{
			Class: "Test",
			Properties: map[string]interface{}{
				"text": "hello world",
			},
		},
		{
			Class: "Test",
			Properties: map[string]interface{}{
				"text": "hello world",
			},
		},
	}
	skipObject := []bool{false, false}
	cfg := modules.NewClassBasedModuleConfig(&models.Class{}, mod.Name(), "", "")
	vectors, _, errors := mod.VectorizeBatch(context.Background(), objs, skipObject, cfg)
	assert.Len(t, vectors, 2)
	assert.Len(t, errors, 0)
}


func TestBigramModule_MetaInfo(t *testing.T) {
	mod := New()
	meta, err := mod.MetaInfo()
	assert.NoError(t, err)
	assert.NotNil(t, meta)
}

func TestBigramModule_AdditionalProperties(t *testing.T) {
	mod := New()
	props := mod.AdditionalProperties()
	assert.NotNil(t, props)
}

func TestBigramModule_VectorizableProperties(t *testing.T) {
	mod := New()
	vectorizable, props, err := mod.VectorizableProperties(modules.NewClassBasedModuleConfig(&models.Class{}, mod.Name(), "", ""))
	assert.True(t, vectorizable)
	assert.Nil(t, props)
	assert.NoError(t, err)
}

func TestBigramModule_ClassConfigDefaults(t *testing.T) {
	mod := New()
	props := mod.ClassConfigDefaults()
	assert.NotNil(t, props)
}

func TestBigramModule_PropertyConfigDefaults(t *testing.T) {
	mod := New()
	props := mod.PropertyConfigDefaults(nil)
	assert.NotNil(t, props)
}

func TestBigramModule_ValidateClass(t *testing.T) {
	mod := New()
	err := mod.ValidateClass(context.Background(), &models.Class{}, modules.NewClassBasedModuleConfig(&models.Class{}, mod.Name(), "", ""))
	assert.NoError(t, err)
}

func TestBigramModule_Arguments(t *testing.T) {
	mod := New()
	args := mod.Arguments()
	assert.NotNil(t, args)
}

func TestBigramModule_VectorSearches(t *testing.T) {
	mod := New()
	searches := mod.VectorSearches()
	assert.NotNil(t, searches)
}

func TestBigramModule_InitNearText(t *testing.T) {
	mod := New()
	err := mod.initNearText()
	assert.NoError(t, err)
}

func TestBigramModule_InitExtension(t *testing.T) {
	mod := New()
	err := mod.InitExtension(nil)
	assert.NoError(t, err)
}
