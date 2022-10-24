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

package modopenai

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/semi-technologies/weaviate/entities/moduletools"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOpenAIModuleIntegration(t *testing.T) {
	apiKey := os.Getenv("OPENAI_APIKEY")
	if apiKey == "" {
		t.Skip("Only running OpenAI integration test when OPENAI_APIKEY environment variable is set")
	}
	type testCase struct {
		name              string
		input             []string
		openAIType        string
		openAIModel       string
		expectedVector    []float32
		expectedVectorLen int
	}

	tests := []testCase{
		{
			name:              "ada-text",
			input:             []string{"hello"},
			openAIType:        "text",
			openAIModel:       "ada",
			expectedVector:    []float32{0.028831173, 0.017496463},
			expectedVectorLen: 1024,
		},
		{
			name:              "babbage-text",
			input:             []string{"hello"},
			openAIType:        "text",
			openAIModel:       "babbage",
			expectedVector:    []float32{-0.005670484, -0.007993165},
			expectedVectorLen: 2048,
		},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	logger, _ := test.NewNullLogger()

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			openai := New()
			initParams := moduletools.NewInitParams(&fakeStorageProvider{}, nil, logger)
			openai.Init(ctx, initParams)

			settings := &fakeSettings{
				openAIType:  test.openAIType,
				openAIModel: test.openAIModel,
			}

			vec, err := openai.vectorizer.Texts(ctx, test.input, settings)

			require.Nil(t, err)
			assert.Equal(t, test.expectedVectorLen, len(vec))
			assert.Equal(t, test.expectedVector[0], vec[0])
			assert.Equal(t, test.expectedVector[1], vec[len(vec)-1])
		})
	}
}

type fakeStorageProvider struct {
	dataPath string
}

func (sp fakeStorageProvider) Storage(name string) (moduletools.Storage, error) {
	return nil, nil
}

func (sp fakeStorageProvider) DataPath() string {
	return sp.dataPath
}

type fakeSettings struct {
	skippedProperty    string
	vectorizeClassName bool
	excludedProperty   string
	openAIType         string
	openAIModel        string
}

func (f *fakeSettings) PropertyIndexed(propName string) bool {
	return f.skippedProperty != propName
}

func (f *fakeSettings) VectorizePropertyName(propName string) bool {
	return f.excludedProperty != propName
}

func (f *fakeSettings) VectorizeClassName() bool {
	return f.vectorizeClassName
}

func (f *fakeSettings) Type() string {
	return f.openAIType
}

func (f *fakeSettings) Model() string {
	return f.openAIModel
}
