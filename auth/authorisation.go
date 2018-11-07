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

package auth

import (
	"context"
	errors_ "errors"
	"strings"

	"github.com/go-openapi/strfmt"

	"github.com/creativesoftwarefdn/weaviate/database/connectors"
	"github.com/creativesoftwarefdn/weaviate/models"
)

// IsOwnKeyOrLowerInTree returns whether a key is his own or in his children
func IsOwnKeyOrLowerInTree(ctx context.Context, currentKey *models.KeyTokenGetResponse, userKeyID strfmt.UUID, databaseConnector dbconnector.DatabaseConnector) bool {
	// If is own key, return true
	if strings.EqualFold(string(userKeyID), string(currentKey.KeyID)) {
		return true
	}

	// Get all child id's
	childIDs := []strfmt.UUID{}
	childIDs, _ = GetKeyChildrenUUIDs(ctx, databaseConnector, currentKey.KeyID, true, childIDs, 0, 0)

	// Check ID is in childIds
	isChildID := false
	for _, childID := range childIDs {
		if childID == userKeyID {
			isChildID = true
		}
	}

	// If ID is in the child ID's, you are allowed to do the action
	if isChildID {
		return true
	}

	return false
}

// ActionsAllowed returns information whether an action is allowed based on given several input vars.
func ActionsAllowed(ctx context.Context, actions []string, validateObject interface{}, databaseConnector dbconnector.DatabaseConnector, objectOwnerUUID interface{}) (bool, error) {
	// Get the user by the given principal
	keyObject := validateObject.(*models.KeyTokenGetResponse)

	// Check whether the given owner of the object is in the children, if the ownerID is given
	correctChild := false
	if objectOwnerUUID != nil {
		correctChild = IsOwnKeyOrLowerInTree(ctx, keyObject, objectOwnerUUID.(strfmt.UUID), databaseConnector)
	} else {
		correctChild = true
	}

	// Return false if the object's owner is not the logged in user or one of its childs.
	if !correctChild {
		return false, errors_.New("the object does not belong to the given token or to one of the token's children")
	}

	// All possible actions in a map to check it more easily
	actionsToCheck := map[string]bool{
		"read":    false,
		"write":   false,
		"execute": false,
		"delete":  false,
	}

	// Add 'true' if an action has to be checked on its rights.
	for _, action := range actions {
		actionsToCheck[action] = true
	}

	// Check every action on its rights, if rights are needed and the key has not that kind of rights, return false.
	if actionsToCheck["read"] && !keyObject.Read {
		return false, errors_.New("read rights are needed to perform this action")
	}

	// Idem
	if actionsToCheck["write"] && !keyObject.Write {
		return false, errors_.New("write rights are needed to perform this action")
	}

	// Idem
	if actionsToCheck["delete"] && !keyObject.Delete {
		return false, errors_.New("delete rights are needed to perform this action")
	}

	// Idem
	if actionsToCheck["execute"] && !keyObject.Execute {
		return false, errors_.New("execute rights are needed to perform this action")
	}

	return true, nil
}

// GetKeyChildrenUUIDs returns children recursively based on its parameters.
func GetKeyChildrenUUIDs(ctx context.Context, databaseConnector dbconnector.DatabaseConnector, parentUUID strfmt.UUID, filterOutDeleted bool, allIDs []strfmt.UUID, maxDepth int, depth int) ([]strfmt.UUID, error) {
	// Append on every depth
	if depth > 0 {
		allIDs = append(allIDs, parentUUID)
	}

	// Init children var
	children := []*models.KeyGetResponse{}

	// Get children from the db-connector
	err := databaseConnector.GetKeyChildren(ctx, parentUUID, &children)

	// Return error
	if err != nil {
		return allIDs, err
	}

	// For every depth, get the ID's
	if maxDepth == 0 || depth < maxDepth {
		for _, child := range children {
			allIDs, err = GetKeyChildrenUUIDs(ctx, databaseConnector, child.KeyID, filterOutDeleted, allIDs, maxDepth, depth+1)
		}
	}

	return allIDs, err
}
