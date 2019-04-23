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

	"github.com/creativesoftwarefdn/weaviate/entities/filters"
	"github.com/creativesoftwarefdn/weaviate/entities/schema"
	"github.com/creativesoftwarefdn/weaviate/entities/schema/kind"
	"github.com/creativesoftwarefdn/weaviate/usecases/config"
	"github.com/creativesoftwarefdn/weaviate/usecases/kinds"
)

func Test_ExtractAnalyticsPropsFromGetMeta(t *testing.T) {
	t.Parallel()

	query :=
		"{ GetMeta { Things { Car(forceRecalculate: true, useAnalyticsEngine: true) { horsepower { mean } } } } }"
	analytics := filters.AnalyticsProps{
		UseAnaltyicsEngine: true,
		ForceRecalculate:   true,
	}
	cfg := config.Config{
		AnalyticsEngine: config.AnalyticsEngine{
			Enabled: true,
		},
	}
	expectedParams := &kinds.GetMetaParams{
		Kind:      kind.Thing,
		ClassName: schema.ClassName("Car"),
		Properties: []kinds.MetaProperty{
			{
				Name:                "horsepower",
				StatisticalAnalyses: []kinds.StatisticalAnalysis{kinds.Mean},
			},
		},
		Analytics: analytics,
	}
	resolver := newMockResolver(cfg)
	resolver.On("LocalGetMeta", expectedParams).
		Return(map[string]interface{}{}, nil).Once()

	resolver.AssertResolve(t, query)
}
