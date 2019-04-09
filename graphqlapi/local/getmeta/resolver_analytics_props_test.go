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
 */
package getmeta

import (
	"testing"

	"github.com/creativesoftwarefdn/weaviate/config"
	"github.com/creativesoftwarefdn/weaviate/database/schema"
	"github.com/creativesoftwarefdn/weaviate/database/schema/kind"
	"github.com/creativesoftwarefdn/weaviate/graphqlapi/local/common_filters"
)

func Test_ExtractAnalyticsPropsFromGetMeta(t *testing.T) {
	t.Parallel()

	query :=
		"{ GetMeta { Things { Car(forceRecalculate: true, useAnalyticsEngine: true) { horsepower { mean } } } } }"
	analytics := common_filters.AnalyticsProps{
		UseAnaltyicsEngine: true,
		ForceRecalculate:   true,
	}
	cfg := config.Config{
		AnalyticsEngine: config.AnalyticsEngine{
			Enabled: true,
		},
	}
	expectedParams := &Params{
		Kind:      kind.THING_KIND,
		ClassName: schema.ClassName("Car"),
		Properties: []MetaProperty{
			{
				Name:                "horsepower",
				StatisticalAnalyses: []StatisticalAnalysis{Mean},
			},
		},
		Analytics: analytics,
	}
	resolver := newMockResolver(cfg)
	resolver.On("LocalGetMeta", expectedParams).
		Return(map[string]interface{}{}, nil).Once()

	resolver.AssertResolve(t, query)
}
