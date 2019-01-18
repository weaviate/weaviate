/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2018 Weaviate. All rights reserved.
 * LICENSE: https://github.com/creativesoftwarefdn/weaviate/blob/develop/LICENSE.md
 * AUTHOR: Bob van Luijt (bob@kub.design)
 * See www.creativesoftwarefdn.org for details
 * Contact: @CreativeSofwFdn / bob@kub.design
 */
package test

import (
	"testing"

	"github.com/creativesoftwarefdn/weaviate/test/acceptance/helper"
)

// Helper function to get all the names of Thing classes.
func GetThingClassNames(t *testing.T) []string {
	resp, err := helper.Client(t).Schema.WeaviateSchemaDump(nil)
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
	resp, err := helper.Client(t).Schema.WeaviateSchemaDump(nil)
	var names []string

	// Extract all names
	helper.AssertRequestOk(t, resp, err, func() {
		for _, class := range resp.Payload.Actions.Classes {
			names = append(names, class.Class)
		}
	})

	return names
}
