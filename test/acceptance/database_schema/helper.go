/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2019 Weaviate. All rights reserved.
 * LICENSE: https://github.com/semi-technologies/weaviate/blob/develop/LICENSE.md
 * DESIGN & CONCEPT: Bob van Luijt (@bobvanluijt)
 * CONTACT: hello@semi.technology
 */
package test

import (
	"testing"

	"github.com/semi-technologies/weaviate/test/acceptance/helper"
)

// Helper function to get all the names of Thing classes.
func GetThingClassNames(t *testing.T) []string {
	resp, err := helper.Client(t).Schema.WeaviateSchemaDump(nil, nil)
	var names []string

	// Extract all names
	helper.AssertRequestOk(t, resp, err, func() {
		for _, class := range resp.Payload.Things.Classes {
			names = append(names, class.Class)
		}
	})

	return names
}

// Helper function to get all the names of Action classes.
func GetActionClassNames(t *testing.T) []string {
	resp, err := helper.Client(t).Schema.WeaviateSchemaDump(nil, nil)
	var names []string

	// Extract all names
	helper.AssertRequestOk(t, resp, err, func() {
		for _, class := range resp.Payload.Actions.Classes {
			names = append(names, class.Class)
		}
	})

	return names
}
