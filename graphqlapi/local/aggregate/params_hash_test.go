/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2019 Weaviate. All rights reserved.
 * LICENSE: https://github.com/creativesoftwarefdn/weaviate/blob/develop/LICENSE.md
 * DESIGN & CONCEPT: Bob van Luijt (@bobvanluijt)
 * CONTACT: hello@creativesoftwarefdn.org
 */package aggregate

import (
	"testing"

	"github.com/creativesoftwarefdn/weaviate/database/schema"
	"github.com/creativesoftwarefdn/weaviate/database/schema/kind"
	cf "github.com/creativesoftwarefdn/weaviate/graphqlapi/local/common_filters"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_ParamsHashing(t *testing.T) {

	params := func() Params {
		return Params{
			Analytics: cf.AnalyticsProps{UseAnaltyicsEngine: true},
			ClassName: schema.ClassName("MyBestClass"),
			Filters:   nil,
			Kind:      kind.THING_KIND,
			Properties: []Property{
				Property{
					Name:        schema.PropertyName("bestprop"),
					Aggregators: []Aggregator{Count},
				},
			},
		}
	}
	hash := func() string { return "a71e85e0741fccd63b33281b26270d43" }

	t.Run("it generates a hash", func(t *testing.T) {
		p := params()
		h, err := p.AnalyticsHash()
		require.Nil(t, err)
		assert.Equal(t, h, hash())
	})

	t.Run("it generates the same hash if analytical props are changed", func(t *testing.T) {
		p := params()
		p.Analytics.ForceRecalculate = true
		h, err := p.AnalyticsHash()
		require.Nil(t, err)
		assert.Equal(t, hash(), h)
	})

	t.Run("it generates a different hash if a prop is changed", func(t *testing.T) {
		p := params()
		p.Properties[0].Aggregators[0] = Maximum
		h, err := p.AnalyticsHash()
		require.Nil(t, err)
		assert.NotEqual(t, hash(), h)
	})

	t.Run("it generates a different hash if where filter is added", func(t *testing.T) {
		p := params()
		p.Filters = &cf.LocalFilter{Root: &cf.Clause{Value: &cf.Value{Value: "foo"}}}
		h, err := p.AnalyticsHash()
		require.Nil(t, err)
		assert.NotEqual(t, hash(), h)
	})
}
