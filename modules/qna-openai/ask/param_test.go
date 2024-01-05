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

package ask

import "testing"

func Test_validateAskFn(t *testing.T) {
	type args struct {
		param interface{}
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "should validate",
			args: args{
				param: &AskParams{
					Question: "question",
				},
			},
		},
		{
			name: "should not validate when empty question",
			args: args{
				param: &AskParams{
					Question: "",
				},
			},
			wantErr: true,
		},
		{
			name: "should not validate when empty params",
			args: args{
				param: &AskParams{},
			},
			wantErr: true,
		},
		{
			name: "should not validate when distance and certainty are present",
			args: args{
				param: &AskParams{
					Distance:  0.1,
					Certainty: 0.1,
				},
			},
			wantErr: true,
		},
		{
			name: "should not validate when param passed is struct, not a pointer to struct",
			args: args{
				param: AskParams{
					Question: "question",
				},
			},
			wantErr: true,
		},
	}
	provider := New(nil)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := provider.validateAskFn(tt.args.param); (err != nil) != tt.wantErr {
				t.Errorf("validateAskFn() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
