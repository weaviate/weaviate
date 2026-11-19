//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package rest

import (
	"net/http"
	"testing"

	"github.com/weaviate/weaviate/adapters/handlers/rest/operations"
	"github.com/weaviate/weaviate/adapters/handlers/rest/operations/users"
	"github.com/weaviate/weaviate/usecases/license"
)

func TestSetupDBUserExpirationHandlers_DBUsersOff(t *testing.T) {
	api := operations.NewWeaviateAPI(nil)
	setupDBUserExpirationHandlers(api, license.FeatureOff, func(*operations.WeaviateAPI) {
		t.Fatal("setupWL ran with DB users off")
	})

	resp := api.UsersSetUserExpirationHandler.Handle(users.SetUserExpirationParams{UserID: "user"}, nil)
	requireAnswer(t, resp, http.StatusUnprocessableEntity, "db user management is not enabled")
}
