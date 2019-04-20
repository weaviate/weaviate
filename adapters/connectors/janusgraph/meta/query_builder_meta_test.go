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
package meta

import (
	"testing"

	"github.com/creativesoftwarefdn/weaviate/usecases/kinds"
)

func Test_QueryBuilder_MetaProps(t *testing.T) {
	tests := testCases{
		testCase{
			name: "with meta.count",
			inputProps: []kinds.MetaProperty{
				kinds.MetaProperty{
					Name:                "meta",
					StatisticalAnalyses: []kinds.StatisticalAnalysis{kinds.Count},
				},
			},
			expectedQuery: `
				.union(
						count().project("count").project("meta")
				)
				.group().by(select(keys).unfold()).by(
					select(values).unfold().group()
					.by( select(keys).unfold())
					.by( select(values).unfold())
				)
			`,
		},
	}

	tests.AssertQuery(t, nil)
}
