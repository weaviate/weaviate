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

package projector

import (
	"reflect"
	"testing"

	"github.com/tailor-inc/graphql/language/ast"
)

func Test_parseFeatureProjectionArguments(t *testing.T) {
	type args struct {
		args []*ast.Argument
	}
	tests := []struct {
		name string
		args args
		want *Params
	}{
		{
			name: "Should create with no params",
			args: args{
				args: []*ast.Argument{},
			},
			want: &Params{
				Enabled: true,
			},
		},
		{
			name: "Should create with all params",
			args: args{
				args: []*ast.Argument{
					createArg("algorithm", "tsne"),
					createArg("dimensions", "3"),
					createArg("iterations", "100"),
					createArg("learningRate", "15"),
					createArg("perplexity", "10"),
				},
			},
			want: &Params{
				Enabled:      true,
				Algorithm:    ptString("tsne"),
				Dimensions:   ptInt(3),
				Iterations:   ptInt(100),
				LearningRate: ptInt(15),
				Perplexity:   ptInt(10),
			},
		},
		{
			name: "Should create with only algorithm param",
			args: args{
				args: []*ast.Argument{
					createArg("algorithm", "tsne"),
				},
			},
			want: &Params{
				Enabled:   true,
				Algorithm: ptString("tsne"),
			},
		},
		{
			name: "Should create with only dimensions param",
			args: args{
				args: []*ast.Argument{
					createArg("dimensions", "3"),
				},
			},
			want: &Params{
				Enabled:    true,
				Dimensions: ptInt(3),
			},
		},
		{
			name: "Should create with only iterations param",
			args: args{
				args: []*ast.Argument{
					createArg("iterations", "100"),
				},
			},
			want: &Params{
				Enabled:    true,
				Iterations: ptInt(100),
			},
		},
		{
			name: "Should create with only learningRate param",
			args: args{
				args: []*ast.Argument{
					createArg("learningRate", "15"),
				},
			},
			want: &Params{
				Enabled:      true,
				LearningRate: ptInt(15),
			},
		},
		{
			name: "Should create with only perplexity param",
			args: args{
				args: []*ast.Argument{
					createArg("perplexity", "10"),
				},
			},
			want: &Params{
				Enabled:    true,
				Perplexity: ptInt(10),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := parseFeatureProjectionArguments(tt.args.args); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("parseFeatureProjectionArguments() = %v, want %v", got, tt.want)
			}
		})
	}
}

func createArg(name string, value string) *ast.Argument {
	n := ast.Name{
		Value: name,
	}
	val := ast.StringValue{
		Kind:  "Kind",
		Value: value,
	}
	arg := ast.Argument{
		Name:  ast.NewName(&n),
		Kind:  "Kind",
		Value: ast.NewStringValue(&val),
	}
	a := ast.NewArgument(&arg)
	return a
}
