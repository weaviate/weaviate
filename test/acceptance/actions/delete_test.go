/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2019 Weaviate. All rights reserved.
 * LICENSE: https://github.com/creativesoftwarefdn/weaviate/blob/develop/LICENSE.md
 * DESIGN: Bob van Luijt (bob@k10y.co)
 */
package test

// Acceptance tests for actions

import (
	"github.com/creativesoftwarefdn/weaviate/client/actions"
	"github.com/creativesoftwarefdn/weaviate/test/acceptance/helper"
	"testing"
)

func TestCanAddAndRemoveAction(t *testing.T) {
	actionId := assertCreateAction(t, "TestAction", map[string]interface{}{})

	// Yes, it's created
	_ = assertGetAction(t, actionId)

	// Now perorm the the deletion
	delResp, err := helper.Client(t).Actions.WeaviateActionsDelete(actions.NewWeaviateActionsDeleteParams().WithActionID(actionId), helper.RootAuth)
	helper.AssertRequestOk(t, delResp, err, nil)

	// And verify that the action is gone
	getResp, err := helper.Client(t).Actions.WeaviateActionsGet(actions.NewWeaviateActionsGetParams().WithActionID(actionId), helper.RootAuth)
	helper.AssertRequestFail(t, getResp, err, nil)
}
