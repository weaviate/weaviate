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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/modules/text2vec-kserve/ent"
)

func Test_validateModuleConfig(t *testing.T) {
	tests := []struct {
		testCase  string
		cfg       moduletools.ClassConfig
		hasErrors bool
	}{
		{
			testCase: "Empty config",
			cfg: fakeClassConfig{
				classConfig: map[string]interface{}{},
			},
			hasErrors: true,
		},
		{
			testCase: "Empty url",
			cfg: fakeClassConfig{
				classConfig: map[string]interface{}{
					"url": "",
				},
			},
			hasErrors: true,
		},
		{
			testCase: "Empty modelName",
			cfg: fakeClassConfig{
				classConfig: map[string]interface{}{
					"url": "",
				},
			},
			hasErrors: true,
		},
		{
			testCase: "Empty modelInput",
			cfg: fakeClassConfig{
				classConfig: map[string]interface{}{
					"url":         "some.url.com",
					"modelName":   "test-model",
					"modelInput":  "",
					"modelOutput": "foo",
				},
			},
			hasErrors: true,
		},
		{
			testCase: "Empty modelOutput",
			cfg: fakeClassConfig{
				classConfig: map[string]interface{}{
					"url":         "some.url.com",
					"modelName":   "test-model",
					"modelOutput": "",
				},
			},
			hasErrors: true,
		},
		{
			testCase: "Minimal config",
			cfg: fakeClassConfig{
				classConfig: map[string]interface{}{
					"url":            "some.url.com",
					"protocol":       "gRPC",
					"connectionArgs": map[string]interface{}{},
					"modelName":      "test-model",
					"modelInput":     "input0",
					"modelOutput":    "output0",
					"embeddingDims":  "512",
				},
			},
			hasErrors: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.testCase, func(t *testing.T) {
			settings := NewClassSettings(tt.cfg)
			err := settings.validateModuleConfig(context.TODO())
			hasErrors := err != nil
			assert.Equal(t, tt.hasErrors, hasErrors, err)
		})
	}
}

func Test_validateKServeProtocol(t *testing.T) {
	type args struct {
		protocol ent.Protocol
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name:    "No protocol provided",
			args:    args{},
			wantErr: true,
		},
		{
			name: "gRPC protocol",
			args: args{
				protocol: "gRPC",
			},
			wantErr: false,
		},
		{
			name: "HTTPV1 protocol",
			args: args{
				protocol: "HttpV1",
			},
			wantErr: false,
		},
		{
			name: "HTTPV2 protocol",
			args: args{
				protocol: "HttpV2",
			},
			wantErr: false,
		},
		{
			name: "Protocol is case sensitive",
			args: args{
				protocol: "grpc",
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := validateProtocol(tt.args.protocol); (err != nil) != tt.wantErr {
				t.Errorf("validateKServeProtocol() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_validateKServeServiceUrl(t *testing.T) {
	type args struct {
		kServeService string
		protocol      ent.Protocol
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name:    "No url provided ",
			args:    args{},
			wantErr: true,
		},
		{
			name: "Missing url schema",
			args: args{
				kServeService: "some.bad.url:8000",
				protocol:      "HttpV1",
			},
			wantErr: true,
		},
		{
			name: "Incorrect url schema",
			args: args{
				kServeService: "uwu://some.bad.url:8000",
				protocol:      "HttpV1",
			},
			wantErr: true,
		},
		{
			name: "Valid http url",
			args: args{
				kServeService: "http://some.bad.url:8000",
				protocol:      "HttpV1",
			},
			wantErr: false,
		},
		{
			name: "Valid https url",
			args: args{
				kServeService: "https://some.bad.url:8000",
				protocol:      "HttpV1",
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := validateUrl(tt.args.kServeService, tt.args.protocol); (err != nil) != tt.wantErr {
				t.Errorf("validateKServeServiceUrl() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
