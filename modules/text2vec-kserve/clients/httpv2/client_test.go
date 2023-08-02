package httpv2

import (
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_appendHTTPV2InferEndpoint(t *testing.T) {
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
			want: "some.url.com/v2/models/some-model/infer",
		},
		{
			name: "With model version",
			args: args{
				u:            "some.url.com",
				modelName:    "some-model",
				modelVersion: "1",
			},
			want: "some.url.com/v2/models/some-model/versions/1/infer",
		},
		{
			name: "Escape model version",
			args: args{
				u:            "some.url.com",
				modelName:    "some-model",
				modelVersion: "%boop",
			},
			want: "some.url.com/v2/models/some-model/versions/%25boop/infer",
		},
		{
			name: "Url with port",
			args: args{
				u:         "some.url.com:80",
				modelName: "some-model",
			},
			want: "some.url.com:80/v2/models/some-model/infer",
		},
		{
			name: "Unescaped model name",
			args: args{
				u:         "some.url.com:80",
				modelName: "/this/is/a/weird/model",
			},
			want: "some.url.com:80/v2/models/%2Fthis%2Fis%2Fa%2Fweird%2Fmodel/infer",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := appendHTTPV2InferEndpoint(tt.args.u, tt.args.modelName, tt.args.modelVersion); got != tt.want {
				t.Errorf("appendHTTPV2InferEndpoint() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_parseV2ResponseBody(t *testing.T) {
	type args struct {
		s string
	}
	tests := []struct {
		name    string
		args    args
		want    *predictV2Response
		wantErr bool
	}{
		{
			name: "Empty response",
			args: args{
				s: "",
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "Invalid JSON",
			args: args{
				s: `{
					"beep": "boop
				}`,
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "Valid response",
			args: args{
				s: `
				{
					"model_name": "some model",
					"model_version": "123",
					"outputs": [
						{
							"name": "output1",
							"shape": [1,3],
							"datatype": "FP32",
							"data": [1.0, 2.0, 3.0]
						}
					]
				}
				`,
			},
			want: &predictV2Response{
				ModelName:    "some model",
				ModelVersion: "123",
				Outputs: []outputTensor{
					{
						Name:     "output1",
						Shape:    []int32{1, 3},
						Data:     []float32{1.0, 2.0, 3.0},
						Datatype: "FP32",
					},
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := strings.NewReader(tt.args.s)
			got, err := parseV2ResponseBody(r)
			assert.Equal(t, err != nil, tt.wantErr, err)

			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("parseV2ResponseBody() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_validateV2Response(t *testing.T) {
	type args struct {
		r             *predictV2Response
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
				r:             &predictV2Response{},
				embeddingDims: 0,
			},
			wantErr: true,
		},
		{
			name: "Contains more than one output",
			args: args{
				r: &predictV2Response{
					ModelName:    "foo",
					ModelVersion: "1",
					Outputs: []outputTensor{
						{
							Name:     "o1",
							Shape:    []int32{1, 1},
							Data:     []float32{1.0},
							Datatype: "FP32",
						},
						{
							Name:     "o2",
							Shape:    []int32{1, 1},
							Data:     []float32{1.0},
							Datatype: "FP32",
						},
					},
				},
				embeddingDims: 0,
			},
			wantErr: true,
		},
		{
			name: "Wrong output shape",
			args: args{
				r: &predictV2Response{
					ModelName:    "foo",
					ModelVersion: "1",
					Outputs: []outputTensor{
						{
							Name:     "o1",
							Shape:    []int32{1, 1, 1},
							Data:     []float32{},
							Datatype: "FP32",
						},
					},
				},
				embeddingDims: 0,
			},
			wantErr: true,
		},
		{
			name: "Mismatched embedding dimensions",
			args: args{
				r: &predictV2Response{
					ModelName:    "foo",
					ModelVersion: "1",
					Outputs: []outputTensor{
						{
							Name:     "o1",
							Shape:    []int32{1, 3},
							Data:     []float32{1, 2, 3},
							Datatype: "FP32",
						},
					},
				},
				embeddingDims: 4,
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateV2Response(tt.args.r, tt.args.embeddingDims)
			assert.Equal(t, err != nil, tt.wantErr, err)
		})
	}
}
