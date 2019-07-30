//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2019 Weaviate. All rights reserved.
//  LICENSE WEAVIATE OPEN SOURCE: https://www.semi.technology/playbook/playbook/contract-weaviate-OSS.html
//  LICENSE WEAVIATE ENTERPRISE: https://www.semi.technology/playbook/contract-weaviate-enterprise.html
//  CONCEPT: Bob van Luijt (@bobvanluijt)
//  CONTACT: hello@semi.technology
//

package meta

import (
	"testing"

	"github.com/semi-technologies/weaviate/usecases/traverser"
)

func Test_QueryBuilder_MetaProps(t *testing.T) {
	tests := testCases{
		testCase{
			name: "with meta.count",
			inputProps: []traverser.MetaProperty{
				traverser.MetaProperty{
					Name:                "meta",
					StatisticalAnalyses: []traverser.StatisticalAnalysis{traverser.Count},
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
