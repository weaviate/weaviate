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

// Package restapi with all rest API functions.
package restapi

import (
	middleware "github.com/go-openapi/runtime/middleware"
	"github.com/go-openapi/strfmt"

	"github.com/creativesoftwarefdn/weaviate/auth"
	"github.com/creativesoftwarefdn/weaviate/connectors/utils"
	"github.com/creativesoftwarefdn/weaviate/models"
	"github.com/creativesoftwarefdn/weaviate/restapi/operations"
	"github.com/creativesoftwarefdn/weaviate/restapi/operations/keys"
)

func configureAPIKeys(api *operations.WeaviateAPI) {
	api.KeysWeaviateKeyCreateHandler = keys.WeaviateKeyCreateHandlerFunc(func(params keys.WeaviateKeyCreateParams, principal interface{}) middleware.Responder {
		// Create current User object from principal
		key := principal.(*models.KeyTokenGetResponse)

		// Fill the new User object
		url := serverConfig.GetHostAddress()
		newKey := &models.KeyTokenGetResponse{}
		newKey.KeyID = connutils.GenerateUUID()
		newKey.Token = connutils.GenerateUUID()
		newKey.Parent = &models.SingleRef{
			LocationURL:  &url,
			NrDollarCref: principal.(*models.KeyTokenGetResponse).KeyID,
			Type:         string(connutils.RefTypeKey),
		}
		newKey.KeyCreate = *params.Body

		// Get context from request
		ctx := params.HTTPRequest.Context()

		// Key expiry time is in the past
		currentUnix := connutils.NowUnix()
		if newKey.KeyExpiresUnix != -1 && newKey.KeyExpiresUnix < currentUnix {
			return keys.NewWeaviateKeyCreateUnprocessableEntity().WithPayload(createErrorResponseObject("Key expiry time is in the past."))
		}

		// Key expiry time is later than the expiry time of parent
		if key.KeyExpiresUnix != -1 && key.KeyExpiresUnix < newKey.KeyExpiresUnix {
			return keys.NewWeaviateKeyCreateUnprocessableEntity().WithPayload(createErrorResponseObject("Key expiry time is later than the expiry time of parent."))
		}

		// Save to DB
		insertErr := dbConnector.AddKey(ctx, &newKey.Key, newKey.KeyID, connutils.TokenHasher(newKey.Token))
		if insertErr != nil {
			messaging.ErrorMessage(insertErr)
			return keys.NewWeaviateKeyCreateUnprocessableEntity().WithPayload(createErrorResponseObject(insertErr.Error()))
		}

		// Return SUCCESS
		return keys.NewWeaviateKeyCreateOK().WithPayload(newKey)

	})
	api.KeysWeaviateKeysChildrenGetHandler = keys.WeaviateKeysChildrenGetHandlerFunc(func(params keys.WeaviateKeysChildrenGetParams, principal interface{}) middleware.Responder {
		// Initialize response
		keyResponse := models.KeyGetResponse{}

		// Get context from request
		ctx := params.HTTPRequest.Context()

		// First check on 'not found', otherwise it will say 'forbidden' in stead of 'not found'
		errGet := dbConnector.GetKey(ctx, params.KeyID, &keyResponse)

		// Not found
		if errGet != nil {
			return keys.NewWeaviateKeysChildrenGetNotFound()
		}

		// Check on permissions
		keyObject, _ := principal.(*models.KeyTokenGetResponse)
		if !auth.IsOwnKeyOrLowerInTree(ctx, keyObject, params.KeyID, dbConnector) {
			return keys.NewWeaviateKeysChildrenGetForbidden()
		}

		// Get the children
		childIDs := []strfmt.UUID{}
		childIDs, _ = auth.GetKeyChildrenUUIDs(ctx, dbConnector, params.KeyID, true, childIDs, 1, 0)

		// Initiate response object
		responseObject := &models.KeyChildrenGetResponse{}
		responseObject.Children = generateMultipleRefObject(childIDs)

		// Return children with 'OK'
		return keys.NewWeaviateKeysChildrenGetOK().WithPayload(responseObject)
	})
	api.KeysWeaviateKeysDeleteHandler = keys.WeaviateKeysDeleteHandlerFunc(func(params keys.WeaviateKeysDeleteParams, principal interface{}) middleware.Responder {
		// Initialize response
		keyResponse := models.KeyGetResponse{}

		// Get context from request
		ctx := params.HTTPRequest.Context()

		// First check on 'not found', otherwise it will say 'forbidden' in stead of 'not found'
		errGet := dbConnector.GetKey(ctx, params.KeyID, &keyResponse)

		// Not found
		if errGet != nil {
			return keys.NewWeaviateKeysDeleteNotFound()
		}

		// Check on permissions, only delete allowed if lower in tree (not own key)
		keyObject, _ := principal.(*models.KeyTokenGetResponse)
		if !auth.IsOwnKeyOrLowerInTree(ctx, keyObject, params.KeyID, dbConnector) || keyObject.KeyID == params.KeyID {
			return keys.NewWeaviateKeysDeleteForbidden()
		}

		// Remove key from database if found
		deleteKey(ctx, dbConnector, params.KeyID)

		// Return 'No Content'
		return keys.NewWeaviateKeysDeleteNoContent()
	})
	api.KeysWeaviateKeysGetHandler = keys.WeaviateKeysGetHandlerFunc(func(params keys.WeaviateKeysGetParams, principal interface{}) middleware.Responder {
		// Initialize response
		keyResponse := models.KeyGetResponse{}

		// Get context from request
		ctx := params.HTTPRequest.Context()

		// Get item from database
		err := dbConnector.GetKey(ctx, params.KeyID, &keyResponse)

		// Object is deleted or not-existing
		if err != nil {
			return keys.NewWeaviateKeysGetNotFound()
		}

		// Check on permissions
		keyObject, _ := principal.(*models.KeyTokenGetResponse)
		if !auth.IsOwnKeyOrLowerInTree(ctx, keyObject, params.KeyID, dbConnector) {
			return keys.NewWeaviateKeysGetForbidden()
		}

		// Get is successful
		return keys.NewWeaviateKeysGetOK().WithPayload(&keyResponse)
	})
	api.KeysWeaviateKeysMeChildrenGetHandler = keys.WeaviateKeysMeChildrenGetHandlerFunc(func(params keys.WeaviateKeysMeChildrenGetParams, principal interface{}) middleware.Responder {
		// First check on 'not found', otherwise it will say 'forbidden' in stead of 'not found'
		currentKey := principal.(*models.KeyTokenGetResponse)

		// Get context from request
		ctx := params.HTTPRequest.Context()

		// Get the children
		childIDs := []strfmt.UUID{}
		childIDs, _ = auth.GetKeyChildrenUUIDs(ctx, dbConnector, currentKey.KeyID, true, childIDs, 1, 0)

		// Initiate response object
		responseObject := &models.KeyChildrenGetResponse{}
		responseObject.Children = generateMultipleRefObject(childIDs)

		// Return children with 'OK'
		return keys.NewWeaviateKeysMeChildrenGetOK().WithPayload(responseObject)
	})
	api.KeysWeaviateKeysMeGetHandler = keys.WeaviateKeysMeGetHandlerFunc(func(params keys.WeaviateKeysMeGetParams, principal interface{}) middleware.Responder {
		// Initialize response object
		responseObject := models.KeyGetResponse{}

		// Get context from request
		ctx := params.HTTPRequest.Context()

		isRoot := false
		responseObject.Key.IsRoot = &isRoot

		// Get item from database
		err := dbConnector.GetKey(ctx, principal.(*models.KeyTokenGetResponse).KeyID, &responseObject)

		// Object is deleted or not-existing
		if err != nil {
			return keys.NewWeaviateKeysGetNotFound()
		}

		// Get is successful
		return keys.NewWeaviateKeysMeGetOK().WithPayload(&responseObject)
	})
	api.KeysWeaviateKeysRenewTokenHandler = keys.WeaviateKeysRenewTokenHandlerFunc(func(params keys.WeaviateKeysRenewTokenParams, principal interface{}) middleware.Responder {
		// Initialize response
		keyResponse := models.KeyGetResponse{}

		// Get context from request
		ctx := params.HTTPRequest.Context()

		// First check on 'not found', otherwise it will say 'forbidden' in stead of 'not found'
		errGet := dbConnector.GetKey(ctx, params.KeyID, &keyResponse)

		// Not found
		if errGet != nil {
			return keys.NewWeaviateKeysRenewTokenNotFound()
		}

		// Check on permissions
		keyObject, _ := principal.(*models.KeyTokenGetResponse)
		if !auth.IsOwnKeyOrLowerInTree(ctx, keyObject, params.KeyID, dbConnector) {
			return keys.NewWeaviateKeysRenewTokenForbidden()
		}

		// Can't renew own unless root
		if keyObject.KeyID == params.KeyID && keyObject.Parent != nil {
			return keys.NewWeaviateKeysRenewTokenForbidden()
		}

		// Generate new token
		newToken := connutils.GenerateUUID()

		// Update the key in the database
		insertErr := dbConnector.UpdateKey(ctx, &keyResponse.Key, keyResponse.KeyID, connutils.TokenHasher(newToken))
		if insertErr != nil {
			messaging.ErrorMessage(insertErr)
			return keys.NewWeaviateKeysRenewTokenUnprocessableEntity().WithPayload(createErrorResponseObject(insertErr.Error()))
		}

		// Build new token response object
		renewObject := &models.KeyTokenGetResponse{}
		renewObject.KeyGetResponse = keyResponse
		renewObject.Token = newToken

		return keys.NewWeaviateKeysRenewTokenOK().WithPayload(renewObject)
	})
}
