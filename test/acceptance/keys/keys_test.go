package test

// Acceptance tests for keys.

import (
	"fmt"
	"testing"

	"sort"
	"time"

	"github.com/go-openapi/runtime"
	"github.com/go-openapi/strfmt"

	"github.com/stretchr/testify/assert"

	"github.com/creativesoftwarefdn/weaviate/client/keys"
	"github.com/creativesoftwarefdn/weaviate/models"
	"github.com/creativesoftwarefdn/weaviate/test/acceptance/helper"

	connutils "github.com/creativesoftwarefdn/weaviate/connectors/utils"
)

const fakeKeyId strfmt.UUID = "11111111-1111-1111-1111-111111111111"

// Simple sanity check to ensure that we can fetch the root key configured to this test run.
func TestGettingRootKey(t *testing.T) {
	t.Parallel()

	params := keys.NewWeaviateKeysGetParams().WithKeyID(helper.RootApiKey)
	resp, err := helper.Client(t).Keys.WeaviateKeysGet(params, helper.RootAuth)

	helper.AssertRequestOk(t, resp, err, func() {
		assert.Equal(t, helper.RootApiKey, resp.Payload.KeyID)
	})
}

// This test ensures that we can create sub keys,
// that they are linked correctly to their parent keys,
// and that the persmissions are handled correctly.
func TestNestedKeys(t *testing.T) {
	t.Parallel() // run TestNestedKeys in parallel with other top level tests.

	// Create a new key.
	t.Run("Create root child", func(t *testing.T) {
		params := keys.NewWeaviateKeyCreateParams().WithBody(&models.KeyCreate{
			Delete:         true,
			Email:          "string",
			IPOrigin:       []string{"127.0.0.*", "*"},
			KeyExpiresUnix: -1,
			Read:           false,
			Write:          false,
			Execute:        true,
		})

		resp, err := helper.Client(t).Keys.WeaviateKeyCreate(params, helper.RootAuth)

		// We store the details of the created key in here.
		// These are re-used in sub tests.
		var childAPIToken strfmt.UUID
		var childAPIKey strfmt.UUID

		helper.AssertRequestOk(t, resp, err, func() {
			keyResponse := resp.Payload

			// Check permissions
			assert.Equal(t, true, keyResponse.Delete)
			assert.Equal(t, true, keyResponse.Execute)
			assert.Equal(t, false, keyResponse.Read)
			assert.Equal(t, false, keyResponse.Write)

			// Store the returned key & token for further tests.
			childAPIKey = keyResponse.KeyID
			childAPIToken = keyResponse.Token

			// Ensure UUID-format of both key & token.
			assert.Regexp(t, strfmt.UUIDPattern, childAPIKey)
			assert.Regexp(t, strfmt.UUIDPattern, childAPIToken)
		})

		defer cleanupKey(childAPIKey)

		// Now create a key that is has as a parent: the child of the root key
		t.Run("Create a grand child", func(t *testing.T) {
			childAuth := helper.CreateAuth(childAPIKey, string(childAPIToken))

			params := keys.NewWeaviateKeyCreateParams().WithBody(&models.KeyCreate{
				Delete:         false,
				Email:          "string",
				IPOrigin:       []string{"127.0.0.*", "*"},
				KeyExpiresUnix: -1,
				Read:           true,
				Write:          true,
				Execute:        false,
			})

			resp, err := helper.Client(t).Keys.WeaviateKeyCreate(params, childAuth)

			// We store the details of the created key in here.
			// These are re-used in sub tests.
			var grandChildAPIToken strfmt.UUID
			var grandChildAPIKey strfmt.UUID
			// TODO in old tests known as newSubAPIToken

			helper.AssertRequestOk(t, resp, err, func() {
				keyResponse := resp.Payload

				// Check that the parent of the grand child, is the child
				assert.Equal(t, childAPIKey, keyResponse.Parent.NrDollarCref)

				// Check permissions
				assert.Equal(t, false, keyResponse.Delete)
				assert.Equal(t, false, keyResponse.Execute)
				assert.Equal(t, true, keyResponse.Read)
				assert.Equal(t, true, keyResponse.Write)

				// Store the returned key & token for further tests.
				grandChildAPIKey = keyResponse.KeyID
				grandChildAPIToken = keyResponse.Token

				// Ensure UUID-format of both key & token.
				assert.Regexp(t, strfmt.UUIDPattern, grandChildAPIKey)
				assert.Regexp(t, strfmt.UUIDPattern, grandChildAPIToken)

				assert.Equal(t, int64(-1), keyResponse.KeyExpiresUnix)
			})

			defer cleanupKey(grandChildAPIKey)
		})
	})
}

// Make sure that we can't create a key that expires in the past.
// TODO Convert this to a Unit test.
func TestKeyCantExpireInThePast(t *testing.T) {
	t.Parallel() // run TestKeyExpiration in parallel with other top level tests.

	// First create a key that is supposed to expire an hour ago.
	expire_at := time.Now().Add(-1 * time.Hour).Unix()
	params := keys.NewWeaviateKeyCreateParams().WithBody(&models.KeyCreate{
		IPOrigin:       []string{"127.0.0.*", "*"},
		KeyExpiresUnix: expire_at,
	})

	resp, err := helper.Client(t).Keys.WeaviateKeyCreate(params, helper.RootAuth)

	helper.AssertRequestFail(t, resp, err, func() {
		errorMessage := err.(*keys.WeaviateKeyCreateUnprocessableEntity).Payload.Error.Message
		assert.Equal(t, "Key expiry time is in the past.", errorMessage)
	})
}

// Test that a child key not expire later than its parent.
func TestChildKeyCannotOutliveParent(t *testing.T) {
	t.Parallel() // run TestKeyExpiration in parallel with other top level tests.

	// First create a key that expires in two seconds
	expire_at := time.Now().Add(10 * time.Second)
	params := keys.NewWeaviateKeyCreateParams().WithBody(&models.KeyCreate{
		IPOrigin:       []string{"127.0.0.*", "*"},
		KeyExpiresUnix: connutils.MakeUnixMillisecond(expire_at),
	})

	resp, err := helper.Client(t).Keys.WeaviateKeyCreate(params, helper.RootAuth)

	var expiringToken strfmt.UUID
	var expiringKey strfmt.UUID

	var expiringAuth runtime.ClientAuthInfoWriterFunc

	helper.AssertRequestOk(t, resp, err, func() {
		keyResponse := resp.Payload

		// Store the returned key & token for further tests.
		expiringKey = keyResponse.KeyID
		expiringToken = keyResponse.Token
		expiringAuth = helper.CreateAuth(expiringKey, string(expiringToken))
	})

	defer cleanupKey(expiringKey)

	t.Run("Cannot create key that expires later than it's parent", func(t *testing.T) {
		child_expire_at := expire_at.Add(1 * time.Hour)

		params := keys.NewWeaviateKeyCreateParams().WithBody(&models.KeyCreate{
			IPOrigin:       []string{"127.0.0.*", "*"},
			KeyExpiresUnix: connutils.MakeUnixMillisecond(child_expire_at),
		})

		// NOTE we perform the following request with the experingAuth credentials
		resp, err := helper.Client(t).Keys.WeaviateKeyCreate(params, expiringAuth)

		helper.AssertRequestFail(t, resp, err, func() {
			errorMessage := err.(*keys.WeaviateKeyCreateUnprocessableEntity).Payload.Error.Message
			assert.Equal(t, "Key expiry time is later than the expiry time of parent.", errorMessage)
		})
	})
}

func TestExpiredKeys(t *testing.T) {
	t.Parallel() // run TestKeyExpiration in parallel with other top level tests.

	// First create a key that expires in two seconds
	expire_at := time.Now().Add(1 * time.Second)
	params := keys.NewWeaviateKeyCreateParams().WithBody(&models.KeyCreate{
		IPOrigin:       []string{"127.0.0.*", "*"},
		KeyExpiresUnix: connutils.MakeUnixMillisecond(expire_at),
	})

	resp, err := helper.Client(t).Keys.WeaviateKeyCreate(params, helper.RootAuth)

	var expiringToken strfmt.UUID
	var expiringKey strfmt.UUID

	var expiringAuth runtime.ClientAuthInfoWriterFunc

	helper.AssertRequestOk(t, resp, err, func() {
		keyResponse := resp.Payload

		// Store the returned key & token for further tests.
		expiringKey = keyResponse.KeyID
		expiringToken = keyResponse.Token
		expiringAuth = helper.CreateAuth(expiringKey, string(expiringToken))
	})

	defer cleanupKey(expiringKey)

	// Make sure that the key is expired.
	time.Sleep(1 * time.Second)

	t.Run("Cannot use key if it is expired", func(t *testing.T) {
		// Attempt to get information about current key, with expired key.
		params := keys.NewWeaviateKeysMeGetParams()
		resp, err := helper.Client(t).Keys.WeaviateKeysMeGet(params, expiringAuth)

		helper.AssertRequestFail(t, resp, err, func() {
			_, ok := err.(*keys.WeaviateKeysMeGetUnauthorized)
			if !ok {
				t.Errorf("Did not get unauthorized response!")
			}
		})
	})
}

// Thest that the /keys/me endpoint retuns the key that is used to talk to Weaviate
func TestGetCurrentlyUsedKey(t *testing.T) {
	t.Parallel()

	params := keys.NewWeaviateKeysMeGetParams()
	resp, err := helper.Client(t).Keys.WeaviateKeysMeGet(params, helper.RootAuth)

	helper.AssertRequestOk(t, resp, err, func() {
		keyResponse := resp.Payload

		// Store the returned key & token for further tests.
		meKey := keyResponse.KeyID
		assert.Equal(t, helper.RootApiKey, meKey)
	})
}

// Test that a child key should be readable by a parent's key.
func TestGetChildKey(t *testing.T) {
	t.Parallel()

	params := keys.NewWeaviateKeyCreateParams().WithBody(&models.KeyCreate{
		IPOrigin:       []string{"127.0.0.*", "*"},
		KeyExpiresUnix: -1,
	})

	resp, err := helper.Client(t).Keys.WeaviateKeyCreate(params, helper.RootAuth)

	var childAPIKey strfmt.UUID

	helper.AssertRequestOk(t, resp, err, func() {
		// Store the returned key & token for further tests.
		childAPIKey = resp.Payload.KeyID
	})

	defer cleanupKey(childAPIKey)

	t.Run("Getting child key is allowed", func(t *testing.T) {
		params := keys.NewWeaviateKeysGetParams().WithKeyID(childAPIKey)
		resp, err := helper.Client(t).Keys.WeaviateKeysGet(params, helper.RootAuth)
		helper.AssertRequestOk(t, resp, err, nil)
	})
}

// Test that a child key cannot read it's parent key
func TestChildCannotGetParentKey(t *testing.T) {
	t.Parallel()

	params := keys.NewWeaviateKeyCreateParams().WithBody(&models.KeyCreate{
		IPOrigin:       []string{"127.0.0.*", "*"},
		KeyExpiresUnix: -1,
	})

	resp, err := helper.Client(t).Keys.WeaviateKeyCreate(params, helper.RootAuth)

	var childAPIToken strfmt.UUID
	var childAPIKey strfmt.UUID

	helper.AssertRequestOk(t, resp, err, func() {
		// Store the returned key & token for further tests.
		childAPIKey = resp.Payload.KeyID
		childAPIToken = resp.Payload.Token
	})

	defer cleanupKey(childAPIKey)

	t.Run("Child cannot get parent", func(t *testing.T) {
		childAuth := helper.CreateAuth(childAPIKey, string(childAPIToken))
		params := keys.NewWeaviateKeysGetParams().WithKeyID(helper.RootApiKey)
		resp, err := helper.Client(t).Keys.WeaviateKeysGet(params, childAuth)
		helper.AssertRequestFail(t, resp, err, nil)
	})
}

func TestGetNonExistingKey(t *testing.T) {
	t.Parallel()

	params := keys.NewWeaviateKeysGetParams().WithKeyID(fakeKeyId)
	resp, err := helper.Client(t).Keys.WeaviateKeysGet(params, helper.RootAuth)
	helper.AssertRequestFail(t, resp, err, func() {
		_, ok := err.(*keys.WeaviateKeysGetNotFound)
		if !ok {
			t.Errorf("Did not get not found response, but %#v", err)
		}
	})
}

// A parent can list the children of a key.
func TestParentGetChildrenOfKey(t *testing.T) {
	t.Parallel()

	params := keys.NewWeaviateKeyCreateParams().WithBody(&models.KeyCreate{
		IPOrigin:       []string{"127.0.0.*", "*"},
		KeyExpiresUnix: -1,
	})

	respParent, err := helper.Client(t).Keys.WeaviateKeyCreate(params, helper.RootAuth)

	var parentAPIKey strfmt.UUID
	var parentAPIToken strfmt.UUID

	helper.AssertRequestOk(t, respParent, err, func() {
		// Store the returned key & token for further tests.
		parentAPIKey = respParent.Payload.KeyID
		parentAPIToken = respParent.Payload.Token
	})

	defer cleanupKey(parentAPIKey)

	parentAuth := helper.CreateAuth(parentAPIKey, string(parentAPIToken))

	respA, err := helper.Client(t).Keys.WeaviateKeyCreate(params, parentAuth)

	var childAPIKeyA strfmt.UUID

	helper.AssertRequestOk(t, respA, err, func() {
		// Store the returned key & token for further tests.
		childAPIKeyA = respA.Payload.KeyID
	})

	defer cleanupKey(childAPIKeyA)

	respB, err := helper.Client(t).Keys.WeaviateKeyCreate(params, parentAuth)

	var childAPIKeyB strfmt.UUID

	helper.AssertRequestOk(t, respB, err, func() {
		// Store the returned key & token for further tests.
		childAPIKeyB = respB.Payload.KeyID
	})

	defer cleanupKey(childAPIKeyB)

	createdChildren := []string{string(childAPIKeyA), string(childAPIKeyB)}
	sort.Strings(createdChildren)

	t.Run("Get children of key", func(t *testing.T) {
		params := keys.NewWeaviateKeysChildrenGetParams().WithKeyID(parentAPIKey)
		resp, err := helper.Client(t).Keys.WeaviateKeysChildrenGet(params, helper.RootAuth)

		// And assert that they have the same key id's
		helper.AssertRequestOk(t, resp, err, func() {
			children := resp.Payload.Children
			assert.Equal(t, 2, len(children))

			foundChildren := []string{string(children[0].NrDollarCref), string(children[1].NrDollarCref)}
			sort.Strings(foundChildren)

			assert.Equal(t, createdChildren, foundChildren)
		})
	})
}

// A child cannot list the children of it's parent.
func TestChildCannotListParentsChildren(t *testing.T) {
	t.Parallel()

	params := keys.NewWeaviateKeyCreateParams().WithBody(&models.KeyCreate{
		IPOrigin:       []string{"127.0.0.*", "*"},
		KeyExpiresUnix: -1,
	})

	respParent, err := helper.Client(t).Keys.WeaviateKeyCreate(params, helper.RootAuth)

	var parentAPIKey strfmt.UUID
	var parentAPIToken strfmt.UUID

	helper.AssertRequestOk(t, respParent, err, func() {
		// Store the returned key & token for further tests.
		parentAPIKey = respParent.Payload.KeyID
		parentAPIToken = respParent.Payload.Token
	})

	defer cleanupKey(parentAPIKey)

	parentAuth := helper.CreateAuth(parentAPIKey, string(parentAPIToken))

	respA, err := helper.Client(t).Keys.WeaviateKeyCreate(params, parentAuth)

	var childAPIKeyA strfmt.UUID

	helper.AssertRequestOk(t, respA, err, func() {
		// Store the returned key & token for further tests.
		childAPIKeyA = respA.Payload.KeyID
	})

	defer cleanupKey(childAPIKeyA)

	respB, err := helper.Client(t).Keys.WeaviateKeyCreate(params, parentAuth)

	var childAPIKeyB strfmt.UUID
	var childAPITokenB strfmt.UUID

	helper.AssertRequestOk(t, respB, err, func() {
		// Store the returned key & token for further tests.
		childAPIKeyB = respB.Payload.KeyID
		childAPITokenB = respB.Payload.Token
	})

	childAuth := helper.CreateAuth(childAPIKeyB, string(childAPITokenB))

	defer cleanupKey(childAPIKeyB)

	createdChildren := []string{string(childAPIKeyA), string(childAPIKeyB)}
	sort.Strings(createdChildren)

	t.Run("Child cannot get children of parent", func(t *testing.T) {
		params := keys.NewWeaviateKeysChildrenGetParams().WithKeyID(parentAPIKey)
		resp, err := helper.Client(t).Keys.WeaviateKeysChildrenGet(params, childAuth)

		helper.AssertRequestFail(t, resp, err, func() {
			_, ok := err.(*keys.WeaviateKeysChildrenGetForbidden)
			if !ok {
				t.Errorf("Did not get forbidden response, but %#v", err)
			}
		})
	})
}

func TestGetChildrenFromNonExistingKey(t *testing.T) {
	t.Parallel()
	params := keys.NewWeaviateKeysChildrenGetParams().WithKeyID(fakeKeyId)
	resp, err := helper.Client(t).Keys.WeaviateKeysChildrenGet(params, helper.RootAuth)

	helper.AssertRequestFail(t, resp, err, func() {
		_, ok := err.(*keys.WeaviateKeysChildrenGetNotFound)
		if !ok {
			t.Errorf("Did not get not found response, but %#v", err)
		}
	})
}

func TestGetChildKeysThroughMeKey(t *testing.T) {
	t.Parallel()

	params := keys.NewWeaviateKeyCreateParams().WithBody(&models.KeyCreate{
		IPOrigin:       []string{"127.0.0.*", "*"},
		KeyExpiresUnix: -1,
	})

	respParent, err := helper.Client(t).Keys.WeaviateKeyCreate(params, helper.RootAuth)

	var parentAPIKey strfmt.UUID
	var parentAPIToken strfmt.UUID

	helper.AssertRequestOk(t, respParent, err, func() {
		// Store the returned key & token for further tests.
		parentAPIKey = respParent.Payload.KeyID
		parentAPIToken = respParent.Payload.Token
	})

	defer cleanupKey(parentAPIKey)

	parentAuth := helper.CreateAuth(parentAPIKey, string(parentAPIToken))

	respA, err := helper.Client(t).Keys.WeaviateKeyCreate(params, parentAuth)

	var childAPIKeyA strfmt.UUID

	helper.AssertRequestOk(t, respA, err, func() {
		// Store the returned key & token for further tests.
		childAPIKeyA = respA.Payload.KeyID
	})

	defer cleanupKey(childAPIKeyA)

	respB, err := helper.Client(t).Keys.WeaviateKeyCreate(params, parentAuth)

	var childAPIKeyB strfmt.UUID

	helper.AssertRequestOk(t, respB, err, func() {
		// Store the returned key & token for further tests.
		childAPIKeyB = respB.Payload.KeyID
	})

	defer cleanupKey(childAPIKeyB)

	createdChildren := []string{string(childAPIKeyA), string(childAPIKeyB)}
	sort.Strings(createdChildren)

	t.Run("Get children of requesting key", func(t *testing.T) {
		params := keys.NewWeaviateKeysMeChildrenGetParams()
		resp, err := helper.Client(t).Keys.WeaviateKeysMeChildrenGet(params, parentAuth)

		// And assert that they have the same key id's
		helper.AssertRequestOk(t, resp, err, func() {
			children := resp.Payload.Children
			assert.Equal(t, 2, len(children))

			foundChildren := []string{string(children[0].NrDollarCref), string(children[1].NrDollarCref)}
			sort.Strings(foundChildren)

			assert.Equal(t, createdChildren, foundChildren)
		})
	})
}

func TestRenewToken(t *testing.T) {
	t.Parallel()

	params := keys.NewWeaviateKeyCreateParams().WithBody(&models.KeyCreate{
		IPOrigin:       []string{"127.0.0.*", "*"},
		KeyExpiresUnix: -1,
	})

	resp, err := helper.Client(t).Keys.WeaviateKeyCreate(params, helper.RootAuth)

	var APIKey strfmt.UUID
	var APIToken strfmt.UUID

	helper.AssertRequestOk(t, resp, err, func() {
		// Store the returned key & token for further tests.
		APIKey = resp.Payload.KeyID
		APIToken = resp.Payload.Token
	})

	defer cleanupKey(APIKey)

	t.Run("Renew token", func(t *testing.T) {
		params := keys.NewWeaviateKeysRenewTokenParams().WithKeyID(APIKey)
		resp, err := helper.Client(t).Keys.WeaviateKeysRenewToken(params, helper.RootAuth)

		helper.AssertRequestOk(t, resp, err, func() {
			updatedAPIKey := resp.Payload.KeyID
			updatedAPIToken := resp.Payload.Token

			assert.Equal(t, APIKey, updatedAPIKey)
			assert.NotEqual(t, APIToken, updatedAPIToken)
		})
	})
}

func TestChildKeyCannotRenewParent(t *testing.T) {
	t.Parallel()

	params := keys.NewWeaviateKeyCreateParams().WithBody(&models.KeyCreate{
		IPOrigin:       []string{"127.0.0.*", "*"},
		KeyExpiresUnix: -1,
	})

	resp, err := helper.Client(t).Keys.WeaviateKeyCreate(params, helper.RootAuth)

	var APIKey strfmt.UUID
	var APIToken strfmt.UUID

	helper.AssertRequestOk(t, resp, err, func() {
		// Store the returned key & token for further tests.
		APIKey = resp.Payload.KeyID
		APIToken = resp.Payload.Token
	})

	auth := helper.CreateAuth(APIKey, string(APIToken))

	defer cleanupKey(APIKey)

	t.Run("Child cannot renew parent token", func(t *testing.T) {
		params := keys.NewWeaviateKeysRenewTokenParams().WithKeyID(helper.RootApiKey)
		resp, err := helper.Client(t).Keys.WeaviateKeysRenewToken(params, auth)

		helper.AssertRequestFail(t, resp, err, func() {
			_, ok := err.(*keys.WeaviateKeysRenewTokenForbidden)
			if !ok {
				t.Errorf("Did not get unauthorized response, but %#v!", err)
			}
		})
	})
}

func TestKeyCannotRenewItself(t *testing.T) {
	t.Parallel()

	params := keys.NewWeaviateKeyCreateParams().WithBody(&models.KeyCreate{
		IPOrigin:       []string{"127.0.0.*", "*"},
		KeyExpiresUnix: -1,
	})

	resp, err := helper.Client(t).Keys.WeaviateKeyCreate(params, helper.RootAuth)

	var APIKey strfmt.UUID
	var APIToken strfmt.UUID

	helper.AssertRequestOk(t, resp, err, func() {
		// Store the returned key & token for further tests.
		APIKey = resp.Payload.KeyID
		APIToken = resp.Payload.Token
	})

	auth := helper.CreateAuth(APIKey, string(APIToken))

	defer cleanupKey(APIKey)

	t.Run("Child cannot renew its own token", func(t *testing.T) {
		t.Skip("This was already skipped in the original acceptance test!")
		params := keys.NewWeaviateKeysRenewTokenParams().WithKeyID(APIKey)
		resp, err := helper.Client(t).Keys.WeaviateKeysRenewToken(params, auth)

		helper.AssertRequestFail(t, resp, err, func() {
			_, ok := err.(*keys.WeaviateKeysRenewTokenForbidden)
			if !ok {
				t.Errorf("Did not get unauthorized response, but %#v!", err)
			}
		})
	})
}

func TestRenewTokenOfNonExistingKey(t *testing.T) {
	t.Parallel()

	params := keys.NewWeaviateKeysRenewTokenParams().WithKeyID(fakeKeyId)
	resp, err := helper.Client(t).Keys.WeaviateKeysRenewToken(params, helper.RootAuth)

	helper.AssertRequestFail(t, resp, err, func() {
		_, ok := err.(*keys.WeaviateKeysRenewTokenNotFound)
		if !ok {
			t.Errorf("Did not get unauthorized response, but %#v!", err)
		}
	})
}

func cleanupKey(uuid strfmt.UUID) {
	params := keys.NewWeaviateKeysDeleteParams().WithKeyID(uuid)
	resp, err := helper.Client(nil).Keys.WeaviateKeysDelete(params, helper.RootAuth)
	if err != nil {
		panic(fmt.Sprintf("Could not clean up key '%s', because %v. Response: %#v", string(uuid), err, resp))
	}
}
