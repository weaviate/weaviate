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

package vectorizer_test

import (
	"testing"

	"github.com/weaviate/weaviate/modules/text2vec-aws/vectorizer"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/weaviate/weaviate/entities/moduletools"
)

func Test_classSettings_Validate(t *testing.T) {
	tests := []struct {
		name              string
		cfg               moduletools.ClassConfig
		wantService       string
		wantRegion        string
		wantModel         string
		wantEndpoint      string
		wantTargetModel   string
		wantTargetVariant string
		wantErr           error
	}{
		{
			name: "happy flow - Bedrock",
			cfg: fakeClassConfig{
				classConfig: map[string]interface{}{
					"service": "bedrock",
					"region":  "us-east-1",
					"model":   "amazon.titan-embed-text-v1",
				},
			},
			wantService: "bedrock",
			wantRegion:  "us-east-1",
			wantModel:   "amazon.titan-embed-text-v1",
			wantErr:     nil,
		},
		{
			name: "happy flow - SageMaker",
			cfg: fakeClassConfig{
				classConfig: map[string]interface{}{
					"service":  "sagemaker",
					"region":   "us-east-1",
					"endpoint": "my-sagemaker",
				},
			},
			wantService:  "sagemaker",
			wantRegion:   "us-east-1",
			wantEndpoint: "my-sagemaker",
			wantErr:      nil,
		},
		{
			name: "empty service",
			cfg: fakeClassConfig{
				classConfig: map[string]interface{}{
					"region": "us-east-1",
					"model":  "amazon.titan-embed-text-v1",
				},
			},
			wantService: "bedrock",
			wantRegion:  "us-east-1",
			wantModel:   "amazon.titan-embed-text-v1",
		},
		{
			name: "empty region - Bedrock",
			cfg: fakeClassConfig{
				classConfig: map[string]interface{}{
					"service": "bedrock",
					"model":   "amazon.titan-embed-text-v1",
				},
			},
			wantErr: errors.Errorf("region cannot be empty"),
		},
		{
			name: "wrong model",
			cfg: fakeClassConfig{
				classConfig: map[string]interface{}{
					"service": "bedrock",
					"region":  "us-west-1",
					"model":   "wrong-model",
				},
			},
			wantErr: errors.Errorf("wrong model, available models are: [amazon.titan-embed-text-v1 amazon.titan-embed-text-v2:0 cohere.embed-english-v3 cohere.embed-multilingual-v3]"),
		},
		{
			name: "all wrong",
			cfg: fakeClassConfig{
				classConfig: map[string]interface{}{
					"service": "",
					"region":  "",
					"model":   "",
				},
			},
			wantErr: errors.Errorf("wrong service, available services are: [bedrock sagemaker], " +
				"region cannot be empty"),
		},
		{
			name: "wrong properties",
			cfg: fakeClassConfig{
				classConfig: map[string]interface{}{
					"service":    "bedrock",
					"region":     "us-west-1",
					"model":      "cohere.embed-multilingual-v3",
					"properties": []interface{}{"prop1", 1111},
				},
			},
			wantErr: errors.Errorf("properties field value: 1111 must be a string"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ic := vectorizer.NewClassSettings(tt.cfg)
			if tt.wantErr != nil {
				assert.EqualError(t, ic.Validate(nil), tt.wantErr.Error())
			} else {
				assert.Equal(t, tt.wantService, ic.Service())
				assert.Equal(t, tt.wantRegion, ic.Region())
				assert.Equal(t, tt.wantModel, ic.Model())
				assert.Equal(t, tt.wantEndpoint, ic.Endpoint())
				assert.Equal(t, tt.wantTargetModel, ic.TargetModel())
				assert.Equal(t, tt.wantTargetVariant, ic.TargetVariant())
			}
		})
	}
}
