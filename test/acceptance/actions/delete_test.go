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

package test

// Acceptance tests for actions

import (
	"testing"
	"time"

	"github.com/semi-technologies/weaviate/client/actions"
	"github.com/semi-technologies/weaviate/test/acceptance/helper"
)

func TestCanAddAndRemoveAction(t *testing.T) {
	actionId := assertCreateAction(t, "TestAction", map[string]interface{}{})

	// Yes, it's created
	_ = assertGetActionEventually(t, actionId)

	// Now perorm the the deletion
	delResp, err := helper.Client(t).Actions.ActionsDelete(actions.NewActionsDeleteParams().WithID(actionId), nil)
	helper.AssertRequestOk(t, delResp, err, nil)

	// This should be improved by polling rather then sleeping, but since it's a
	// very low sleep period, this should do it for now as long as we don't
	// repeat that too often and don't find this to be flaky. If we do see
	// flakyness around this test, a polling mechanism is in order.
	time.Sleep(50 * time.Millisecond)

	// And verify that the action is gone
	getResp, err := helper.Client(t).Actions.ActionsGet(actions.NewActionsGetParams().WithID(actionId), nil)
	helper.AssertRequestFail(t, getResp, err, nil)
}
