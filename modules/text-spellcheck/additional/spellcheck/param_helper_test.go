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

package spellcheck

import (
	"reflect"
	"testing"
)

type fakeNearText struct {
	Values []string
}

type fakeAsk struct {
	Question string
}

func Test_paramHelper_getTexts(t *testing.T) {
	type args struct {
		argumentModuleParams map[string]interface{}
	}
	tests := []struct {
		name    string
		args    args
		want    string
		want1   []string
		wantErr bool
	}{
		{
			name: "should get values from nearText",
			args: args{
				argumentModuleParams: map[string]interface{}{
					"nearText": fakeNearText{Values: []string{"a", "b"}},
				},
			},
			want:    "nearText",
			want1:   []string{"a", "b"},
			wantErr: false,
		},
		{
			name: "should get values from ask",
			args: args{
				argumentModuleParams: map[string]interface{}{
					"ask": fakeAsk{Question: "a"},
				},
			},
			want:    "ask",
			want1:   []string{"a"},
			wantErr: false,
		},
		{
			name: "should be empty",
			args: args{
				argumentModuleParams: map[string]interface{}{},
			},
			want:    "",
			want1:   []string{},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &paramHelper{}
			got, got1, err := p.getTexts(tt.args.argumentModuleParams)
			if (err != nil) != tt.wantErr {
				t.Errorf("paramHelper.getTexts() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("paramHelper.getTexts() got = %v, want %v", got, tt.want)
			}
			if !reflect.DeepEqual(got1, tt.want1) {
				t.Errorf("paramHelper.getTexts() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}
