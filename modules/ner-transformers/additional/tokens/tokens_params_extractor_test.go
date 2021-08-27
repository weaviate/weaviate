//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2021 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package tokens

import (
	"reflect"
	"testing"

	"github.com/graphql-go/graphql/language/ast"
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
			name: "Should create with all params",
			args: args{
				args: []*ast.Argument{
					createArg("certainty", "0.9"),
					createArg("limit", "1"),
					createListArg("properties", []interface{}{"prop1", "prop2"}), // "[\"prop1\"]"), // ,
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

func createListArg(name string, values []interface{}) *ast.Argument {
	n := ast.Name{
		Value: name,
	}
	val := ast.Value{}

	vals := ast.ListValue{
		Kind:   "Kind",
		Values: values,
	}
	arg := ast.Argument{
		Name:  ast.NewName(&n),
		Kind:  "Kind",
		Value: ast.NewStringValue(&val),
	}
	a := ast.NewArgument(&arg)
	return a
}

func ptFloat64(in float64) *float64 {
	return &in
}
