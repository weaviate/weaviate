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

package nearText

import (
	"testing"
)

func Test_validateNearText(t *testing.T) {
	tests := []struct {
		name    string
		param   NearTextParams
		wantErr bool
	}{
		{
			"May be empty",
			NearTextParams{},
			false,
		},

		{
			"With just values",
			NearTextParams{
				Values: []string{"foobar"},
			},
			false,
		},
		{
			"With values, distance, limit",
			NearTextParams{
				Values:       []string{"foobar"},
				Limit:        100,
				Distance:     0.9,
				WithDistance: true,
			},
			false,
		},
		{
			"With values, certainty, limit",
			NearTextParams{
				Values:    []string{"foobar"},
				Limit:     100,
				Certainty: 0.9,
			},
			false,
		},
		{
			"With certainty and distance",
			NearTextParams{
				Values:       []string{"foobar"},
				Certainty:    0.9,
				Distance:     0.1,
				WithDistance: true,
			},
			true,
		},
		{
			"When moveTo with force must also provide either values or objects (with distance)",
			NearTextParams{
				Values:       []string{"foobar"},
				Limit:        100,
				Distance:     0.9,
				WithDistance: true,
				MoveTo: ExploreMove{
					Force: 0.9,
				},
			},
			true,
		},
		{
			"When moveTo with force must also provide either values or objects (with certainty)",
			NearTextParams{
				Values:    []string{"foobar"},
				Limit:     100,
				Certainty: 0.9,
				MoveTo: ExploreMove{
					Force: 0.9,
				},
			},
			true,
		},
		{
			"When moveAway with force must also provide either values or objects (with distance)",
			NearTextParams{
				Values:       []string{"foobar"},
				Limit:        100,
				Distance:     0.9,
				WithDistance: true,
				MoveAwayFrom: ExploreMove{
					Force: 0.9,
				},
			},
			true,
		},
		{
			"When moveAway with force must also provide either values or objects (with certainty)",
			NearTextParams{
				Values:    []string{"foobar"},
				Limit:     100,
				Certainty: 0.9,
				MoveAwayFrom: ExploreMove{
					Force: 0.9,
				},
			},
			true,
		},
		{
			"When moveTo and moveAway with force must also provide either values or objects (with distance)",
			NearTextParams{
				Values:       []string{"foobar"},
				Limit:        100,
				Distance:     0.9,
				WithDistance: true,
				MoveTo: ExploreMove{
					Force: 0.9,
				},
				MoveAwayFrom: ExploreMove{
					Force: 0.9,
				},
			},
			true,
		},
		{
			"When moveTo and moveAway with force must also provide either values or objects (with certainty)",
			NearTextParams{
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
			true,
		},
		{
			"When moveTo or moveAway is with force must also provide either values or objects (with distance)",
			NearTextParams{
				Values:       []string{"foobar"},
				Limit:        100,
				Distance:     0.9,
				WithDistance: true,
				MoveTo: ExploreMove{
					Values: []string{"move to"},
					Force:  0.9,
				},
				MoveAwayFrom: ExploreMove{
					Force: 0.9,
				},
			},
			true,
		},
		{
			"When moveTo or moveAway is with force must also provide either values or objects (with certainty)",
			NearTextParams{
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
			true,
		},
		{
			"When moveTo or moveAway is with force must provide values or objects (with distance)",
			NearTextParams{
				Values:       []string{"foobar"},
				Limit:        100,
				Distance:     0.9,
				WithDistance: true,
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
			false,
		},
		{
			"When moveTo or moveAway is with force must provide values or objects (with certainty)",
			NearTextParams{
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
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.param.Validate(); (err != nil) != tt.wantErr {
				t.Errorf("validateNearText() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
