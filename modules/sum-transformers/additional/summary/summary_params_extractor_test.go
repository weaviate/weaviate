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

package summary

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/tailor-inc/graphql/language/ast"
)

func Test_parseSummaryArguments(t *testing.T) {
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
			name: "Should create with all params",
			args: args{
				args: []*ast.Argument{
					createListArg("properties", []string{"prop1", "prop2"}),
				},
			},
			want: &Params{
				Properties: []string{"prop1", "prop2"},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &SummaryProvider{}
			if got := p.parseSummaryArguments(tt.args.args); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("parseSummaryArguments() = %v, want %v", got, tt.want)
			}
			actual := p.parseSummaryArguments(tt.args.args)
			assert.Equal(t, tt.want, actual)
		})
	}
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
