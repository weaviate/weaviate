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

package fetchfuzzy

import (
	"testing"
)

func Test_QueryBuilder(t *testing.T) {
	tests := testCases{
		{
			name:        "with a single search term",
			inputParams: []string{"searchterm"},
			expectedQuery: `
				g.V().or(
					has("prop_1", textContainsFuzzy("searchterm")),
					has("prop_11", textContainsFuzzy("searchterm"))
				).limit(20).valueMap("uuid", "kind")
			`,
		},
	}

	tests.AssertQuery(t)
}

func Test_QueryBuilder_Many(t *testing.T) {
	tests := testCases{
		{
			name:        "with a multiple search terms",
			inputParams: []string{"one", "two", "three"},
			expectedQuery: `
				g.V().or(
					has("prop_1", textContainsFuzzy("one")),
					has("prop_1", textContainsFuzzy("two")),
					has("prop_1", textContainsFuzzy("three")),
					has("prop_11", textContainsFuzzy("one")),
					has("prop_11", textContainsFuzzy("two")),
					has("prop_11", textContainsFuzzy("three"))
				).limit(20).valueMap("uuid", "kind")
			`,
		},
	}

	tests.AssertQuery(t)
}
