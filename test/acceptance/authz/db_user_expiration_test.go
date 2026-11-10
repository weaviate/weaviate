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

package authz

import (
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/helper"
)

// TestDBUserExpirationMultinode pins that every node applies a DB user's
// expiry when it is written through any node.
func TestDBUserExpirationMultinode(t *testing.T) {
	compose, down := composeUpSharedCluster(t)
	defer down()

	in24h := helper.ExpiryIn(24 * time.Hour)
	in48h := in24h.Add(24 * time.Hour)

	// requireListedOnEveryNode waits until each node lists userID with
	// expiresAt, or with no expiry when expiresAt is nil.
	requireListedOnEveryNode := func(t *testing.T, userID string, expiresAt *time.Time) {
		t.Helper()
		waitForUserOnEveryNode(t, compose, userID, func(c *assert.CollectT, node int, found *models.DBUserInfo) {
			if assert.NotNil(c, found, "node %d does not list %s", node, userID) {
				assert.Equal(c, (*strfmt.DateTime)(expiresAt), found.ExpiresAt, "node %d", node)
			}
		})
	}

	const userID = "exp-mn-user"

	t.Run("create, set and clear reach every node", func(t *testing.T) {
		helper.CreateUserWithExpiry(t, userID, sharedRootKey, &in24h)
		requireListedOnEveryNode(t, userID, &in24h)

		helper.SetupClient(compose.GetWeaviateNode(2).URI())
		helper.SetUserExpiration(t, userID, sharedRootKey, &in48h)
		requireListedOnEveryNode(t, userID, &in48h)

		helper.SetupClient(compose.GetWeaviateNode(3).URI())
		helper.SetUserExpiration(t, userID, sharedRootKey, nil)
		requireListedOnEveryNode(t, userID, nil)
	})
}
