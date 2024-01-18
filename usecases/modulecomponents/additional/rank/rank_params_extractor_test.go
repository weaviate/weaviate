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

package rank

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/tailor-inc/graphql/language/ast"
)

func Test_parseCrossRankerArguments(t *testing.T) {
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
			name: "Should create with query param",
			args: args{
				args: []*ast.Argument{
					createStringArg("query", "sample query"),
				},
			},
			want: &Params{
				Query: strPtr("sample query"),
			},
		},
		{
			name: "Should create with property param",
			args: args{
				args: []*ast.Argument{
					createStringArg("property", "sample property"),
				},
			},
			want: &Params{
				Property: strPtr("sample property"),
			},
		},
		{
			name: "Should create with all params",
			args: args{
				args: []*ast.Argument{
					createStringArg("query", "sample query"),
					createStringArg("property", "sample property"),
				},
			},
			want: &Params{
				Query:    strPtr("sample query"),
				Property: strPtr("sample property"),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &ReRankerProvider{}
			if got := p.parseReRankerArguments(tt.args.args); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("parseReRankerArguments() = %v, want %v", got, tt.want)
			}
			actual := p.parseReRankerArguments(tt.args.args)
			assert.Equal(t, tt.want, actual)
		})
	}
}

func createStringArg(name, value string) *ast.Argument {
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
		Value: &val,
	}
	a := ast.NewArgument(&arg)
	return a
}

func strPtr(s string) *string {
	return &s
}
