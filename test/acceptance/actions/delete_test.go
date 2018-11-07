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
	_ = assertGetThing(t, actionId)

	// Now perorm the the deletion
	delResp, err := helper.Client(t).Actions.WeaviateActionsDelete(actions.NewWeaviateActionsDeleteParams().WithActionID(actionId), helper.RootAuth)
	helper.AssertRequestOk(t, delResp, err, nil)

	// And verify that the action is gone
	getResp, err := helper.Client(t).Actions.WeaviateActionsGet(actions.NewWeaviateActionsGetParams().WithActionID(actionId), helper.RootAuth)
	helper.AssertRequestFail(t, getResp, err, nil)
}
