//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2019 SeMI Holding B.V. (registered @ Dutch Chamber of Commerce no 75221632). All rights reserved.
//  LICENSE WEAVIATE OPEN SOURCE: https://www.semi.technology/playbook/playbook/contract-weaviate-OSS.html
//  LICENSE WEAVIATE ENTERPRISE: https://www.semi.technology/playbook/contract-weaviate-enterprise.html
//  CONCEPT: Bob van Luijt (@bobvanluijt)
//  CONTACT: hello@semi.technology
//

package test

// Acceptance tests for actions

import (
	"testing"

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

	_ = assertGetActionFailsEventually(t, actionId)

	// And verify that the action is gone
	getResp, err := helper.Client(t).Actions.ActionsGet(actions.NewActionsGetParams().WithID(actionId), nil)
	helper.AssertRequestFail(t, getResp, err, nil)
}
