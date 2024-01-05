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

package tokens

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/tailor-inc/graphql/language/ast"
)

func Test_parseTokenArguments(t *testing.T) {
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
			args: args{},
			want: &Params{},
		},
		{
			name: "Should create with all params (and distance)",
			args: args{
				args: []*ast.Argument{
					createArg("distance", "0.4"),
					createArg("limit", "1"),
					createListArg("properties", []string{"prop1", "prop2"}),
				},
			},
			want: &Params{
				Properties: []string{"prop1", "prop2"},
				Distance:   ptFloat64(0.4),
				Limit:      ptInt(1),
			},
		},
		{
			name: "Should create with all params (and certainty)",
			args: args{
				args: []*ast.Argument{
					createArg("certainty", "0.4"),
					createArg("limit", "1"),
					createListArg("properties", []string{"prop1", "prop2"}),
				},
			},
			want: &Params{
				Properties: []string{"prop1", "prop2"},
				Certainty:  ptFloat64(0.4),
				Limit:      ptInt(1),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &TokenProvider{}
			if got := p.parseTokenArguments(tt.args.args); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("parseTokenArguments() = %v, want %v", got, tt.want)
			}
			actual := p.parseTokenArguments(tt.args.args)
			assert.Equal(t, tt.want, actual)
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

func createListArg(name string, valuesIn []string) *ast.Argument {
	n := ast.Name{
		Value: name,
	}

	valuesAst := make([]ast.Value, len(valuesIn))
	for i, value := range valuesIn {
		valuesAst[i] = &ast.StringValue{
			Kind:  "Kind",
			Value: value,
		}
	}
	vals := ast.ListValue{
		Kind:   "Kind",
		Values: valuesAst,
	}
	arg := ast.Argument{
		Name:  ast.NewName(&n),
		Kind:  "Kind",
		Value: &vals,
	}
	a := ast.NewArgument(&arg)
	return a
}

func ptFloat64(in float64) *float64 {
	return &in
}
