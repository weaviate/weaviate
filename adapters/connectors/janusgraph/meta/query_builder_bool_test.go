/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2019 Weaviate. All rights reserved.
 * LICENSE WEAVIATE OPEN SOURCE: https://www.semi.technology/playbook/playbook/contract-weaviate-OSS.html
 * LICENSE WEAVIATE ENTERPRISE: https://www.semi.technology/playbook/contract-weaviate-enterprise.html
 * CONCEPT: Bob van Luijt (@bobvanluijt)
 * CONTACT: hello@semi.technology
 */
package meta

import (
	"testing"

	"github.com/semi-technologies/weaviate/usecases/kinds"
)

func Test_QueryBuilder_BoolProps(t *testing.T) {
	tests := testCases{
		testCase{
			name: "with only a boolean, with only count",
			inputProps: []kinds.MetaProperty{
				kinds.MetaProperty{
					Name:                "isCapital",
					StatisticalAnalyses: []kinds.StatisticalAnalysis{kinds.Count},
				},
			},
			expectedQuery: `
				.union(
					values("isCapital").union(
						count().project("count").project("isCapital")
					)
				)
				.group().by(select(keys).unfold()).by(
					select(values).unfold().group()
					.by( select(keys).unfold())
					.by( select(values).unfold())
				)
			`,
		},

		testCase{
			name: "with count and type",
			inputProps: []kinds.MetaProperty{
				kinds.MetaProperty{
					Name:                "isCapital",
					StatisticalAnalyses: []kinds.StatisticalAnalysis{kinds.Count, kinds.Type},
				},
			},
			expectedQuery: `
				.union(
					values("isCapital").union(
						count().project("count").project("isCapital")
					)
				)
				.group().by(select(keys).unfold()).by(
					select(values).unfold().group()
					.by( select(keys).unfold())
					.by( select(values).unfold())
				)
			`,
		},

		testCase{
			name: "with only a boolean, with only totalTrue",
			inputProps: []kinds.MetaProperty{
				kinds.MetaProperty{
					Name:                "isCapital",
					StatisticalAnalyses: []kinds.StatisticalAnalysis{kinds.TotalTrue},
				},
			},
			expectedQuery: `
				.union(
					values("isCapital").union(
						groupCount().unfold().project("isCapital")
					)
				)
				.group().by(select(keys).unfold()).by(
					select(values).unfold().group()
					.by( select(keys).unfold())
					.by( select(values).unfold())
				)
			`,
		},

		testCase{
			name: "with all boolean props combined",
			inputProps: []kinds.MetaProperty{
				kinds.MetaProperty{
					Name: "isCapital",
					StatisticalAnalyses: []kinds.StatisticalAnalysis{
						kinds.Count, kinds.TotalTrue, kinds.TotalFalse, kinds.PercentageTrue, kinds.PercentageFalse,
					},
				},
			},
			expectedQuery: `
				.union(
					values("isCapital").union(
						count().project("count").project("isCapital"),
						groupCount().unfold().project("isCapital")
					)
				)
				.group().by(select(keys).unfold()).by(
					select(values).unfold().group()
					.by( select(keys).unfold())
					.by( select(values).unfold())
				)
			`,
		},
		testCase{
			name: "with only a boolean, with only all true/false props",
			inputProps: []kinds.MetaProperty{
				kinds.MetaProperty{
					Name: "isCapital",
					StatisticalAnalyses: []kinds.StatisticalAnalysis{
						kinds.TotalTrue, kinds.TotalFalse, kinds.PercentageTrue, kinds.PercentageFalse,
					},
				},
			},
			expectedQuery: `
				.union(
					values("isCapital").union(
						groupCount().unfold().project("isCapital")
					)
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

func Test_QueryBuilderWithNamesource(t *testing.T) {
	tests := testCases{
		testCase{
			name: "with only a boolean, with only count",
			inputProps: []kinds.MetaProperty{
				kinds.MetaProperty{
					Name:                "isCapital",
					StatisticalAnalyses: []kinds.StatisticalAnalysis{kinds.Count},
				},
			},
			expectedQuery: `
				.union(
					values("prop_20").union(
						count().project("count").project("isCapital")
					)
				)
				.group().by(select(keys).unfold()).by(
					select(values).unfold().group()
					.by( select(keys).unfold())
					.by( select(values).unfold())
				)
			`,
		},
	}

	tests.AssertQuery(t, &fakeNameSource{})
}
