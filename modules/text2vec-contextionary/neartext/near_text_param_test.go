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

package neartext

import "testing"

func Test_validateNearText(t *testing.T) {
	type args struct {
		param interface{}
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			"May be empty",
			args{
				param: &NearTextParams{},
			},
			false,
		},
		{
			"Must be pointer",
			args{
				param: NearTextParams{},
			},
			true,
		},
		{
			"With just values",
			args{
				param: &NearTextParams{
					Values: []string{"foobar"},
				},
			},
			false,
		},
		{
			"With values, certainty, limit",
			args{
				param: &NearTextParams{
					Values:    []string{"foobar"},
					Limit:     100,
					Certainty: 0.9,
				},
			},
			false,
		},
		{
			"When moveTo with force must also provide either values or objects",
			args{
				param: &NearTextParams{
					Values:    []string{"foobar"},
					Limit:     100,
					Certainty: 0.9,
					MoveTo: ExploreMove{
						Force: 0.9,
					},
				},
			},
			true,
		},
		{
			"When moveAway with force must also provide either values or objects",
			args{
				param: &NearTextParams{
					Values:    []string{"foobar"},
					Limit:     100,
					Certainty: 0.9,
					MoveAwayFrom: ExploreMove{
						Force: 0.9,
					},
				},
			},
			true,
		},
		{
			"When moveTo and moveAway with force must also provide either values or objects",
			args{
				param: &NearTextParams{
					Values:    []string{"foobar"},
					Limit:     100,
					Certainty: 0.9,
					MoveTo: ExploreMove{
						Force: 0.9,
					},
					MoveAwayFrom: ExploreMove{
						Force: 0.9,
					},
				},
			},
			true,
		},
		{
			"When moveTo or moveAway is with force must also provide either values or objects",
			args{
				param: &NearTextParams{
					Values:    []string{"foobar"},
					Limit:     100,
					Certainty: 0.9,
					MoveTo: ExploreMove{
						Values: []string{"move to"},
						Force:  0.9,
					},
					MoveAwayFrom: ExploreMove{
						Force: 0.9,
					},
				},
			},
			true,
		},
		{
			"When moveTo or moveAway is with force must provide values or objects",
			args{
				param: &NearTextParams{
					Values:    []string{"foobar"},
					Limit:     100,
					Certainty: 0.9,
					MoveTo: ExploreMove{
						Values: []string{"move to"},
						Force:  0.9,
					},
					MoveAwayFrom: ExploreMove{
						Objects: []ObjectMove{
							{ID: "some-uuid"},
						},
						Force: 0.9,
					},
				},
			},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := validateNearTextFn(tt.args.param); (err != nil) != tt.wantErr {
				t.Errorf("validateNearText() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
