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

package answer

import "testing"

func Test_validateAnswerFn(t *testing.T) {
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
				param: &AnswerParams{
					Question: "question",
					Limit:    1,
				},
			},
		},
		{
			name: "should not validate when empty question",
			args: args{
				param: &AnswerParams{
					Question: "",
					Limit:    1,
				},
			},
			wantErr: true,
		},
		{
			name: "should not validate when empty params",
			args: args{
				param: &AnswerParams{},
			},
			wantErr: true,
		},
		{
			name: "should not validate when param passed is struct, not a pointer to struct",
			args: args{
				param: AnswerParams{
					Question: "question",
					Limit:    1,
				},
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := validateAnswerFn(tt.args.param); (err != nil) != tt.wantErr {
				t.Errorf("validateAnswerFn() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
