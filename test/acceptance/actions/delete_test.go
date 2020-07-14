//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2020 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package test

// Acceptance tests for actions

import (
	"testing"

	"github.com/semi-technologies/weaviate/client/actions"
	"github.com/semi-technologies/weaviate/test/acceptance/helper"
)

func removingActions(t *testing.T) {
	actionId := assertCreateAction(t, "TestAction", map[string]interface{}{})

	// Yes, it's created
	_ = assertGetActionEventually(t, actionId)

	// Now perorm the the deletion
	delResp, err := helper.Client(t).Actions.ActionsDelete(actions.NewActionsDeleteParams().WithID(actionId), nil)
	helper.AssertRequestOk(t, delResp, err, nil)

	_ = assertGetActionFailsEventually(t, actionId)

	// And verify that the action is gone
	getResp, err := helper.Client(t).Actions.ActionsGet(actions.NewActionsGetParams().WithID(actionId), nil)
	helper.AssertRequestFail(t, getResp, err, nil)
}
