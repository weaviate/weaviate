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

	"github.com/graphql-go/graphql"
)

func Test_additionalTokensField(t *testing.T) {
	type fields struct {
		ner nerClient
	}
	type args struct {
		classname string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *graphql.Field
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &TokenProvider{
				ner: tt.fields.ner,
			}
			if got := p.additionalTokensField(tt.args.classname); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("TokenProvider.additionalTokensField() = %v, want %v", got, tt.want)
			}
		})
	}
}
