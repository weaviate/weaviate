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

package text2vecbase

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/modulecomponents/batch"
	objectsvectorizer "github.com/weaviate/weaviate/usecases/modulecomponents/vectorizer"
)

type fakeClassConfig struct {
	classConfig           map[string]interface{}
	vectorizeClassName    bool
	vectorizePropertyName bool
	skippedProperty       string
	excludedProperty      string
	// module specific settings
	cohereModel  string
	truncateType string
	baseURL      string
}

func (f fakeClassConfig) Class() map[string]interface{} {
	classSettings := map[string]interface{}{
		"vectorizeClassName": f.vectorizeClassName,
		"model":              f.cohereModel,
		"truncate":           f.truncateType,
		"baseURL":            f.baseURL,
	}
	return classSettings
}

func (f fakeClassConfig) PropertyIndexed(property string) bool {
	return !((property == f.skippedProperty) || (property == f.excludedProperty))
}

func (f fakeClassConfig) ClassByModuleName(moduleName string) map[string]interface{} {
	return f.classConfig
}

func (f fakeClassConfig) Property(propName string) map[string]interface{} {
	if propName == f.skippedProperty {
		return map[string]interface{}{
			"skip": true,
		}
	}
	if propName == f.excludedProperty {
		return map[string]interface{}{
			"vectorizePropertyName": false,
		}
	}
	if f.vectorizePropertyName {
		return map[string]interface{}{
			"vectorizePropertyName": true,
		}
	}
	return nil
}

func (f fakeClassConfig) Tenant() string {
	return ""
}

func (f fakeClassConfig) TargetVector() string {
	return ""
}

func (f fakeClassConfig) VectorizeClassName() bool {
	return f.classConfig["vectorizeClassName"].(bool)
}

func (f fakeClassConfig) VectorizePropertyName(propertyName string) bool {
	return f.vectorizePropertyName
}

func (f fakeClassConfig) Properties() []string {
	return nil
}

func (f fakeClassConfig) PropertiesDataTypes() map[string]schema.DataType {
	return nil
}

func BenchmarkBatchVectorizer(b *testing.B) {
	tokenizer := batch.ReturnBatchTokenizer(1, "", false)
	ctx := context.Background()
	cfg := &fakeClassConfig{vectorizePropertyName: false, classConfig: map[string]interface{}{"vectorizeClassName": false}}

	vectorizer := objectsvectorizer.New()
	encoderCache := batch.NewEncoderCache()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, _, err := tokenizer(ctx, []*models.Object{}, []bool{false}, cfg, vectorizer, encoderCache)
		require.NoError(b, err)
		require.Len(b, encoderCache, 1)
	}
}
