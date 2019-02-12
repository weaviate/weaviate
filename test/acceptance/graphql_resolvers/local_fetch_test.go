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
package test

import (
	"testing"

	"github.com/creativesoftwarefdn/weaviate/test/acceptance/helper"
	"github.com/stretchr/testify/assert"
)

func TestLocalFetchLargeCities(t *testing.T) {
	result := AssertGraphQL(t, helper.RootAuth, `
		{
			Local {
				Fetch {
					Things(where: {
						class: {
							name: "town",
							certainty: 0.5
						},
						properties: {
							name: "population",
							operator: GreaterThan,
							valueInt: 1800000,
							certainty: 1
						}
					}) {
						beacon
					}
				}
			}
		}
	`)

	t.Run("finds exactly one result", func(t *testing.T) {
		beacons := result.Get("Local", "Fetch", "Things").Result
		expectedLen := 1 // only Berlin
		assert.Len(t, beacons, expectedLen)
	})
}

func TestLocalFetchSmallCities(t *testing.T) {
	result := AssertGraphQL(t, helper.RootAuth, `
		{
			Local {
				Fetch {
					Things(where: {
						class: {
							name: "town",
							certainty: 0.5
						},
						properties: {
							name: "population",
							operator: LessThanEqual,
							valueInt: 1800000,
							certainty: 1
						}
					}) {
						beacon
					}
				}
			}
		}
	`)

	t.Run("finds exactly one result", func(t *testing.T) {
		beacons := result.Get("Local", "Fetch", "Things").Result
		expectedLen := 3 // Amsterdam, Rotterdam, Dusselsorf
		assert.Len(t, beacons, expectedLen)
	})
}
