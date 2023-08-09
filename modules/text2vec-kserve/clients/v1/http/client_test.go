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

package http

import (
	"testing"
)

func Test_appendHTTPV1InferEndpoint(t *testing.T) {
	type args struct {
		u            string
		modelName    string
		modelVersion string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "Url and model non empty",
			args: args{
				u:         "some.url.com",
				modelName: "some-model",
			},
			want: "some.url.com/v1/models/some-model:predict",
		},
		{
			name: "With model version",
			args: args{
				u:            "some.url.com",
				modelName:    "some-model",
				modelVersion: "1",
			},
			want: "some.url.com/v1/models/some-model/versions/1:predict",
		},
		{
			name: "Escape model version",
			args: args{
				u:            "some.url.com",
				modelName:    "some-model",
				modelVersion: "%boop",
			},
			want: "some.url.com/v1/models/some-model/versions/%25boop:predict",
		},
		{
			name: "Url with port",
			args: args{
				u:         "some.url.com:80",
				modelName: "some-model",
			},
			want: "some.url.com:80/v1/models/some-model:predict",
		},
		{
			name: "Escape model name",
			args: args{
				u:         "some.url.com:80",
				modelName: "/this/is/a/weird/model",
			},
			want: "some.url.com:80/v1/models/%2Fthis%2Fis%2Fa%2Fweird%2Fmodel:predict",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := appendHTTPV1InferEndpoint(tt.args.u, tt.args.modelName, tt.args.modelVersion)
			if got != tt.want {
				t.Errorf("appendHTTPV1InferEndpoint() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_validateResponse(t *testing.T) {
	type args struct {
		r             *predictV1Response
		embeddingDims int32
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "Empty response",
			args: args{
				r:             &predictV1Response{},
				embeddingDims: 0,
			},
			wantErr: true,
		},
		{
			name: "Embedding dims are not correct",
			args: args{
				r: &predictV1Response{
					Predictions: [][]float32{{1, 2, 3}},
				},
				embeddingDims: 4,
			},
			wantErr: true,
		},
		{
			name: "More than one vector",
			args: args{
				r: &predictV1Response{
					Predictions: [][]float32{{1, 2, 3}, {1, 2, 3}},
				},
				embeddingDims: 3,
			},
			wantErr: true,
		},
		{
			name: "Valid response",
			args: args{
				r: &predictV1Response{
					Predictions: [][]float32{{1, 2, 3}},
				},
				embeddingDims: 3,
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := validateV1Response(tt.args.r, tt.args.embeddingDims); (err != nil) != tt.wantErr {
				t.Errorf("validateResponse() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
