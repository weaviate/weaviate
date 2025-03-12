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
	"reflect"
	"testing"

	"github.com/weaviate/weaviate/entities/dto"
)

func Test_extractNearTextFn(t *testing.T) {
	type args struct {
		source map[string]interface{}
	}
	tests := []struct {
		name       string
		args       args
		want       *NearTextParams
		wantTarget *dto.TargetCombination
	}{
		{
			"Extract with concepts",
			args{
				source: map[string]interface{}{
					"concepts": []interface{}{"c1", "c2", "c3"},
				},
			},
			&NearTextParams{
				Values: []string{"c1", "c2", "c3"},
			},
			nil,
		},
		{
			"Extract with concepts, distance, limit and network",
			args{
				source: map[string]interface{}{
					"concepts": []interface{}{"c1", "c2", "c3"},
					"distance": float64(0.4),
					"limit":    100,
					"network":  true,
				},
			},
			&NearTextParams{
				Values:       []string{"c1", "c2", "c3"},
				Distance:     0.4,
				WithDistance: true,
				Limit:        100,
				Network:      true,
			},
			nil,
		},
		{
			"Extract with concepts, certainty, limit and network",
			args{
				source: map[string]interface{}{
					"concepts":  []interface{}{"c1", "c2", "c3"},
					"certainty": float64(0.4),
					"limit":     100,
					"network":   true,
				},
			},
			&NearTextParams{
				Values:    []string{"c1", "c2", "c3"},
				Certainty: 0.4,
				Limit:     100,
				Network:   true,
			},
			nil,
		},
		{
			"Extract with moveTo, moveAwayFrom, and distance",
			args{
				source: map[string]interface{}{
					"concepts": []interface{}{"c1", "c2", "c3"},
					"distance": float64(0.89),
					"limit":    500,
					"network":  false,
					"moveTo": map[string]interface{}{
						"concepts": []interface{}{"positive"},
						"force":    float64(0.5),
					},
					"moveAwayFrom": map[string]interface{}{
						"concepts": []interface{}{"epic"},
						"force":    float64(0.25),
					},
				},
			},
			&NearTextParams{
				Values:       []string{"c1", "c2", "c3"},
				Distance:     0.89,
				WithDistance: true,
				Limit:        500,
				Network:      false,
				MoveTo: ExploreMove{
					Values: []string{"positive"},
					Force:  0.5,
				},
				MoveAwayFrom: ExploreMove{
					Values: []string{"epic"},
					Force:  0.25,
				},
			},
			nil,
		},
		{
			"Extract with moveTo, moveAwayFrom, and certainty",
			args{
				source: map[string]interface{}{
					"concepts":  []interface{}{"c1", "c2", "c3"},
					"certainty": float64(0.89),
					"limit":     500,
					"network":   false,
					"moveTo": map[string]interface{}{
						"concepts": []interface{}{"positive"},
						"force":    float64(0.5),
					},
					"moveAwayFrom": map[string]interface{}{
						"concepts": []interface{}{"epic"},
						"force":    float64(0.25),
					},
				},
			},
			&NearTextParams{
				Values:    []string{"c1", "c2", "c3"},
				Certainty: 0.89,
				Limit:     500,
				Network:   false,
				MoveTo: ExploreMove{
					Values: []string{"positive"},
					Force:  0.5,
				},
				MoveAwayFrom: ExploreMove{
					Values: []string{"epic"},
					Force:  0.25,
				},
			},
			nil,
		},
		{
			"Extract with moveTo, moveAwayFrom, distance (and objects)",
			args{
				source: map[string]interface{}{
					"concepts": []interface{}{"c1", "c2", "c3"},
					"distance": float64(0.89),
					"limit":    500,
					"network":  false,
					"moveTo": map[string]interface{}{
						"concepts": []interface{}{"positive"},
						"force":    float64(0.5),
						"objects": []interface{}{
							map[string]interface{}{
								"id": "moveTo-uuid1",
							},
							map[string]interface{}{
								"beacon": "weaviate://localhost/moveTo-uuid2",
							},
							map[string]interface{}{
								"beacon": "weaviate://localhost/moveTo-uuid3",
							},
						},
					},
					"moveAwayFrom": map[string]interface{}{
						"concepts": []interface{}{"epic"},
						"force":    float64(0.25),
						"objects": []interface{}{
							map[string]interface{}{
								"id": "moveAwayFrom-uuid1",
							},
							map[string]interface{}{
								"id": "moveAwayFrom-uuid2",
							},
							map[string]interface{}{
								"beacon": "weaviate://localhost/moveAwayFrom-uuid3",
							},
							map[string]interface{}{
								"beacon": "weaviate://localhost/moveAwayFrom-uuid4",
							},
						},
					},
				},
			},
			&NearTextParams{
				Values:       []string{"c1", "c2", "c3"},
				Distance:     0.89,
				WithDistance: true,
				Limit:        500,
				Network:      false,
				MoveTo: ExploreMove{
					Values: []string{"positive"},
					Force:  0.5,
					Objects: []ObjectMove{
						{ID: "moveTo-uuid1"},
						{Beacon: "weaviate://localhost/moveTo-uuid2"},
						{Beacon: "weaviate://localhost/moveTo-uuid3"},
					},
				},
				MoveAwayFrom: ExploreMove{
					Values: []string{"epic"},
					Force:  0.25,
					Objects: []ObjectMove{
						{ID: "moveAwayFrom-uuid1"},
						{ID: "moveAwayFrom-uuid2"},
						{Beacon: "weaviate://localhost/moveAwayFrom-uuid3"},
						{Beacon: "weaviate://localhost/moveAwayFrom-uuid4"},
					},
				},
			},
			nil,
		},
		{
			"Extract with moveTo, moveAwayFrom, certainty (and objects)",
			args{
				source: map[string]interface{}{
					"concepts":  []interface{}{"c1", "c2", "c3"},
					"certainty": float64(0.89),
					"limit":     500,
					"network":   false,
					"moveTo": map[string]interface{}{
						"concepts": []interface{}{"positive"},
						"force":    float64(0.5),
						"objects": []interface{}{
							map[string]interface{}{
								"id": "moveTo-uuid1",
							},
							map[string]interface{}{
								"beacon": "weaviate://localhost/moveTo-uuid2",
							},
							map[string]interface{}{
								"beacon": "weaviate://localhost/moveTo-uuid3",
							},
						},
					},
					"moveAwayFrom": map[string]interface{}{
						"concepts": []interface{}{"epic"},
						"force":    float64(0.25),
						"objects": []interface{}{
							map[string]interface{}{
								"id": "moveAwayFrom-uuid1",
							},
							map[string]interface{}{
								"id": "moveAwayFrom-uuid2",
							},
							map[string]interface{}{
								"beacon": "weaviate://localhost/moveAwayFrom-uuid3",
							},
							map[string]interface{}{
								"beacon": "weaviate://localhost/moveAwayFrom-uuid4",
							},
						},
					},
				},
			},
			&NearTextParams{
				Values:    []string{"c1", "c2", "c3"},
				Certainty: 0.89,
				Limit:     500,
				Network:   false,
				MoveTo: ExploreMove{
					Values: []string{"positive"},
					Force:  0.5,
					Objects: []ObjectMove{
						{ID: "moveTo-uuid1"},
						{Beacon: "weaviate://localhost/moveTo-uuid2"},
						{Beacon: "weaviate://localhost/moveTo-uuid3"},
					},
				},
				MoveAwayFrom: ExploreMove{
					Values: []string{"epic"},
					Force:  0.25,
					Objects: []ObjectMove{
						{ID: "moveAwayFrom-uuid1"},
						{ID: "moveAwayFrom-uuid2"},
						{Beacon: "weaviate://localhost/moveAwayFrom-uuid3"},
						{Beacon: "weaviate://localhost/moveAwayFrom-uuid4"},
					},
				},
			},
			nil,
		},
		{
			"Extract with moveTo, moveAwayFrom, distance (and doubled objects)",
			args{
				source: map[string]interface{}{
					"concepts": []interface{}{"c1", "c2", "c3"},
					"distance": float64(0.89),
					"limit":    500,
					"network":  false,
					"moveTo": map[string]interface{}{
						"concepts": []interface{}{"positive"},
						"force":    float64(0.5),
						"objects": []interface{}{
							map[string]interface{}{
								"id":     "moveTo-uuid1",
								"beacon": "weaviate://localhost/moveTo-uuid2",
							},
							map[string]interface{}{
								"id":     "moveTo-uuid1",
								"beacon": "weaviate://localhost/moveTo-uuid2",
							},
						},
					},
					"moveAwayFrom": map[string]interface{}{
						"concepts": []interface{}{"epic"},
						"force":    float64(0.25),
						"objects": []interface{}{
							map[string]interface{}{
								"id":     "moveAwayFrom-uuid1",
								"beacon": "weaviate://localhost/moveAwayFrom-uuid1",
							},
							map[string]interface{}{
								"id":     "moveAwayFrom-uuid2",
								"beacon": "weaviate://localhost/moveAwayFrom-uuid2",
							},
							map[string]interface{}{
								"beacon": "weaviate://localhost/moveAwayFrom-uuid3",
							},
							map[string]interface{}{
								"beacon": "weaviate://localhost/moveAwayFrom-uuid4",
							},
						},
					},
				},
			},
			&NearTextParams{
				Values:       []string{"c1", "c2", "c3"},
				Distance:     0.89,
				WithDistance: true,
				Limit:        500,
				Network:      false,
				MoveTo: ExploreMove{
					Values: []string{"positive"},
					Force:  0.5,
					Objects: []ObjectMove{
						{ID: "moveTo-uuid1", Beacon: "weaviate://localhost/moveTo-uuid2"},
						{ID: "moveTo-uuid1", Beacon: "weaviate://localhost/moveTo-uuid2"},
					},
				},
				MoveAwayFrom: ExploreMove{
					Values: []string{"epic"},
					Force:  0.25,
					Objects: []ObjectMove{
						{ID: "moveAwayFrom-uuid1", Beacon: "weaviate://localhost/moveAwayFrom-uuid1"},
						{ID: "moveAwayFrom-uuid2", Beacon: "weaviate://localhost/moveAwayFrom-uuid2"},
						{Beacon: "weaviate://localhost/moveAwayFrom-uuid3"},
						{Beacon: "weaviate://localhost/moveAwayFrom-uuid4"},
					},
				},
			},
			nil,
		},
		{
			"Extract with moveTo, moveAwayFrom, certainty (and doubled objects)",
			args{
				source: map[string]interface{}{
					"concepts":  []interface{}{"c1", "c2", "c3"},
					"certainty": float64(0.89),
					"limit":     500,
					"network":   false,
					"moveTo": map[string]interface{}{
						"concepts": []interface{}{"positive"},
						"force":    float64(0.5),
						"objects": []interface{}{
							map[string]interface{}{
								"id":     "moveTo-uuid1",
								"beacon": "weaviate://localhost/moveTo-uuid2",
							},
							map[string]interface{}{
								"id":     "moveTo-uuid1",
								"beacon": "weaviate://localhost/moveTo-uuid2",
							},
						},
					},
					"moveAwayFrom": map[string]interface{}{
						"concepts": []interface{}{"epic"},
						"force":    float64(0.25),
						"objects": []interface{}{
							map[string]interface{}{
								"id":     "moveAwayFrom-uuid1",
								"beacon": "weaviate://localhost/moveAwayFrom-uuid1",
							},
							map[string]interface{}{
								"id":     "moveAwayFrom-uuid2",
								"beacon": "weaviate://localhost/moveAwayFrom-uuid2",
							},
							map[string]interface{}{
								"beacon": "weaviate://localhost/moveAwayFrom-uuid3",
							},
							map[string]interface{}{
								"beacon": "weaviate://localhost/moveAwayFrom-uuid4",
							},
						},
					},
				},
			},
			&NearTextParams{
				Values:    []string{"c1", "c2", "c3"},
				Certainty: 0.89,
				Limit:     500,
				Network:   false,
				MoveTo: ExploreMove{
					Values: []string{"positive"},
					Force:  0.5,
					Objects: []ObjectMove{
						{ID: "moveTo-uuid1", Beacon: "weaviate://localhost/moveTo-uuid2"},
						{ID: "moveTo-uuid1", Beacon: "weaviate://localhost/moveTo-uuid2"},
					},
				},
				MoveAwayFrom: ExploreMove{
					Values: []string{"epic"},
					Force:  0.25,
					Objects: []ObjectMove{
						{ID: "moveAwayFrom-uuid1", Beacon: "weaviate://localhost/moveAwayFrom-uuid1"},
						{ID: "moveAwayFrom-uuid2", Beacon: "weaviate://localhost/moveAwayFrom-uuid2"},
						{Beacon: "weaviate://localhost/moveAwayFrom-uuid3"},
						{Beacon: "weaviate://localhost/moveAwayFrom-uuid4"},
					},
				},
			},
			nil,
		},
		{
			"Extract with concepts and targetVectors",
			args{
				source: map[string]interface{}{
					"concepts":      []interface{}{"c1", "c2", "c3"},
					"targetVectors": []interface{}{"targetVector"},
				},
			},
			&NearTextParams{
				Values:        []string{"c1", "c2", "c3"},
				TargetVectors: []string{"targetVector"},
			},
			&dto.TargetCombination{Type: dto.Minimum},
		},
	}

	testsWithAutocorrect := []struct {
		name       string
		args       args
		want       *NearTextParams
		wantTarget *dto.TargetCombination
	}{
		{
			"Extract with concepts",
			args{
				source: map[string]interface{}{
					"concepts":    []interface{}{"c1", "c2", "c3"},
					"autocorrect": true,
				},
			},
			&NearTextParams{
				Values:      []string{"c1", "c2", "c3"},
				Autocorrect: true,
			},
			nil,
		},
		{
			"Extract with concepts and perform autocorrect",
			args{
				source: map[string]interface{}{
					"concepts":    []interface{}{"transform this", "c2", "transform this"},
					"autocorrect": true,
				},
			},
			&NearTextParams{
				Values:      []string{"transformed text", "c2", "transformed text"},
				Autocorrect: true,
			},
			nil,
		},
		{
			"Extract with moveTo, moveAwayFrom, distance (and doubled objects) and autocorrect",
			args{
				source: map[string]interface{}{
					"concepts":    []interface{}{"transform this", "c1", "c2", "c3", "transform this"},
					"distance":    float64(0.89),
					"limit":       500,
					"network":     false,
					"autocorrect": true,
					"moveTo": map[string]interface{}{
						"concepts": []interface{}{"positive"},
						"force":    float64(0.5),
						"objects": []interface{}{
							map[string]interface{}{
								"id":     "moveTo-uuid1",
								"beacon": "weaviate://localhost/moveTo-uuid2",
							},
							map[string]interface{}{
								"id":     "moveTo-uuid1",
								"beacon": "weaviate://localhost/moveTo-uuid2",
							},
						},
					},
					"moveAwayFrom": map[string]interface{}{
						"concepts": []interface{}{"epic"},
						"force":    float64(0.25),
						"objects": []interface{}{
							map[string]interface{}{
								"id":     "moveAwayFrom-uuid1",
								"beacon": "weaviate://localhost/moveAwayFrom-uuid1",
							},
							map[string]interface{}{
								"id":     "moveAwayFrom-uuid2",
								"beacon": "weaviate://localhost/moveAwayFrom-uuid2",
							},
							map[string]interface{}{
								"beacon": "weaviate://localhost/moveAwayFrom-uuid3",
							},
							map[string]interface{}{
								"beacon": "weaviate://localhost/moveAwayFrom-uuid4",
							},
						},
					},
				},
			},
			&NearTextParams{
				Values:       []string{"transformed text", "c1", "c2", "c3", "transformed text"},
				Distance:     0.89,
				WithDistance: true,
				Limit:        500,
				Network:      false,
				Autocorrect:  true,
				MoveTo: ExploreMove{
					Values: []string{"positive"},
					Force:  0.5,
					Objects: []ObjectMove{
						{ID: "moveTo-uuid1", Beacon: "weaviate://localhost/moveTo-uuid2"},
						{ID: "moveTo-uuid1", Beacon: "weaviate://localhost/moveTo-uuid2"},
					},
				},
				MoveAwayFrom: ExploreMove{
					Values: []string{"epic"},
					Force:  0.25,
					Objects: []ObjectMove{
						{ID: "moveAwayFrom-uuid1", Beacon: "weaviate://localhost/moveAwayFrom-uuid1"},
						{ID: "moveAwayFrom-uuid2", Beacon: "weaviate://localhost/moveAwayFrom-uuid2"},
						{Beacon: "weaviate://localhost/moveAwayFrom-uuid3"},
						{Beacon: "weaviate://localhost/moveAwayFrom-uuid4"},
					},
				},
			},
			nil,
		},
		{
			"Extract with moveTo, moveAwayFrom, certainty (and doubled objects) and autocorrect",
			args{
				source: map[string]interface{}{
					"concepts":    []interface{}{"transform this", "c1", "c2", "c3", "transform this"},
					"certainty":   float64(0.89),
					"limit":       500,
					"network":     false,
					"autocorrect": true,
					"moveTo": map[string]interface{}{
						"concepts": []interface{}{"positive"},
						"force":    float64(0.5),
						"objects": []interface{}{
							map[string]interface{}{
								"id":     "moveTo-uuid1",
								"beacon": "weaviate://localhost/moveTo-uuid2",
							},
							map[string]interface{}{
								"id":     "moveTo-uuid1",
								"beacon": "weaviate://localhost/moveTo-uuid2",
							},
						},
					},
					"moveAwayFrom": map[string]interface{}{
						"concepts": []interface{}{"epic"},
						"force":    float64(0.25),
						"objects": []interface{}{
							map[string]interface{}{
								"id":     "moveAwayFrom-uuid1",
								"beacon": "weaviate://localhost/moveAwayFrom-uuid1",
							},
							map[string]interface{}{
								"id":     "moveAwayFrom-uuid2",
								"beacon": "weaviate://localhost/moveAwayFrom-uuid2",
							},
							map[string]interface{}{
								"beacon": "weaviate://localhost/moveAwayFrom-uuid3",
							},
							map[string]interface{}{
								"beacon": "weaviate://localhost/moveAwayFrom-uuid4",
							},
						},
					},
				},
			},
			&NearTextParams{
				Values:      []string{"transformed text", "c1", "c2", "c3", "transformed text"},
				Certainty:   0.89,
				Limit:       500,
				Network:     false,
				Autocorrect: true,
				MoveTo: ExploreMove{
					Values: []string{"positive"},
					Force:  0.5,
					Objects: []ObjectMove{
						{ID: "moveTo-uuid1", Beacon: "weaviate://localhost/moveTo-uuid2"},
						{ID: "moveTo-uuid1", Beacon: "weaviate://localhost/moveTo-uuid2"},
					},
				},
				MoveAwayFrom: ExploreMove{
					Values: []string{"epic"},
					Force:  0.25,
					Objects: []ObjectMove{
						{ID: "moveAwayFrom-uuid1", Beacon: "weaviate://localhost/moveAwayFrom-uuid1"},
						{ID: "moveAwayFrom-uuid2", Beacon: "weaviate://localhost/moveAwayFrom-uuid2"},
						{Beacon: "weaviate://localhost/moveAwayFrom-uuid3"},
						{Beacon: "weaviate://localhost/moveAwayFrom-uuid4"},
					},
				},
			},
			nil,
		},
		{
			"Extract with concepts and targetVectors",
			args{
				source: map[string]interface{}{
					"concepts":      []interface{}{"c1", "c2", "c3"},
					"targetVectors": []interface{}{"targetVector"},
					"autocorrect":   true,
				},
			},
			&NearTextParams{
				Values:        []string{"c1", "c2", "c3"},
				TargetVectors: []string{"targetVector"},
				Autocorrect:   true,
			},
			&dto.TargetCombination{Type: dto.Minimum},
		},
		{
			name: "should extract properly with concepts and targets set",
			args: args{
				source: map[string]interface{}{
					"concepts": []interface{}{"c1", "c2", "c3"},
					"targets": map[string]interface{}{
						"targetVectors":     []interface{}{"targetVector1", "targetVector2"},
						"combinationMethod": dto.ManualWeights,
						"weights":           map[string]interface{}{"targetVector1": 0.5, "targetVector2": 0.5},
					},
				},
			},
			want: &NearTextParams{
				Values:        []string{"c1", "c2", "c3"},
				TargetVectors: []string{"targetVector1", "targetVector2"},
			},
			wantTarget: &dto.TargetCombination{Type: dto.ManualWeights, Weights: []float32{0.5, 0.5}},
		},
	}
	testsWithAutocorrect = append(testsWithAutocorrect, tests...)

	t.Run("should extract values", func(t *testing.T) {
		provider := New(nil)
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				got, target, err := provider.extractNearTextFn(tt.args.source)
				if !reflect.DeepEqual(got, tt.want) || !reflect.DeepEqual(target, tt.wantTarget) || err != nil {
					t.Errorf("extractNearTextFn() = %v, want %v, %v with error %v", got, tt.want, tt.wantTarget, err)
				}
			})
		}
	})
	t.Run("should extract values with transformer", func(t *testing.T) {
		provider := New(&fakeTransformer{})
		for _, tt := range testsWithAutocorrect {
			t.Run(tt.name, func(t *testing.T) {
				got, target, err := provider.extractNearTextFn(tt.args.source)
				if !reflect.DeepEqual(got, tt.want) || !reflect.DeepEqual(target, tt.wantTarget) || err != nil {
					t.Errorf("extractNearTextFn() = %v, want %v, %v with error %v", got, tt.want, tt.wantTarget, err)
				}
			})
		}
	})
}
