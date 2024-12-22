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

package test_suits

import (
	"os"
	"testing"

	"github.com/liutizhong/weaviate/test/docker"
)

func AllTests(endpoint string) func(t *testing.T) {
	return func(t *testing.T) {
		t.Run("hybrid", testHybrid(endpoint))
		t.Run("schema", testCreateSchema(endpoint))
		t.Run("schema with none vectorizer", testCreateSchemaWithNoneVectorizer(endpoint))
		t.Run("object", testCreateObject(endpoint))
		t.Run("batch", testBatchObject(endpoint))
		t.Run("none vectorizer", testCreateSchemaWithNoneVectorizer(endpoint))
		t.Run("mixed objects with none and vectorizer", testCreateSchemaWithMixedVectorizers(endpoint))
		t.Run("classes with properties setting", testCreateWithModulePropertiesObject(endpoint))
		t.Run("validation", testSchemaValidation(endpoint))
		t.Run("cross references", testReferenceProperties(endpoint))
		t.Run("objects with vectorizer and objects", testCreateSchemaWithVectorizerAndBYOV(endpoint))
		t.Run("generative modules", testNamedVectorsWithGenerativeModules(endpoint))
		t.Run("aggregate", testAggregate(endpoint))
		t.Run("vector index types", testVectorIndexTypesConfigurations(endpoint))
	}
}

func ComposeModules() (composeModules *docker.Compose) {
	composeModules = docker.New().
		WithText2VecContextionary().
		WithText2VecTransformers().
		WithText2VecOpenAI(os.Getenv("OPENAI_APIKEY"), os.Getenv("OPENAI_ORGANIZATION"), os.Getenv("AZURE_APIKEY")).
		WithText2VecCohere(os.Getenv("COHERE_APIKEY")).
		WithGenerativeOpenAI(os.Getenv("OPENAI_APIKEY"), os.Getenv("OPENAI_ORGANIZATION"), os.Getenv("AZURE_APIKEY")).
		WithGenerativeCohere(os.Getenv("COHERE_APIKEY"))
	return
}
