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

package traverser

// TODO: do we still need this? there is no more spark support with 0.20.x
// func Test_ParamsHashing(t *testing.T) {
// 	params := func() AggregateParams {
// 		return AggregateParams{
// 			Analytics: filters.AnalyticsProps{UseAnalyticsEngine: true},
// 			ClassName: schema.ClassName("MyBestClass"),
// 			Filters:   nil,
// 			Kind:      kind.Thing,
// 			Properties: []AggregateProperty{
// 				AggregateProperty{
// 					Name:        schema.PropertyName("bestprop"),
// 					Aggregators: []Aggregator{CountAggregator},
// 				},
// 			},
// 		}
// 	}
// 	hash := func() string { return "a71e85e0741fccd63b33281b26270d43" }

// 	t.Run("it generates a hash", func(t *testing.T) {
// 		p := params()
// 		h, err := p.AnalyticsHash()
// 		require.Nil(t, err)
// 		assert.Equal(t, h, hash())
// 	})

// 	t.Run("it generates the same hash if analytical props are changed", func(t *testing.T) {
// 		p := params()
// 		p.Analytics.ForceRecalculate = true
// 		h, err := p.AnalyticsHash()
// 		require.Nil(t, err)
// 		assert.Equal(t, hash(), h)
// 	})

// 	t.Run("it generates a different hash if a prop is changed", func(t *testing.T) {
// 		p := params()
// 		p.Properties[0].Aggregators[0] = MaximumAggregator
// 		h, err := p.AnalyticsHash()
// 		require.Nil(t, err)
// 		assert.NotEqual(t, hash(), h)
// 	})

// 	t.Run("it generates a different hash if where filter is added", func(t *testing.T) {
// 		p := params()
// 		p.Filters = &filters.LocalFilter{Root: &filters.Clause{Value: &filters.Value{Value: "foo"}}}
// 		h, err := p.AnalyticsHash()
// 		require.Nil(t, err)
// 		assert.NotEqual(t, hash(), h)
// 	})
// }
