/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2019 Weaviate. All rights reserved.
 * LICENSE: https://github.com/creativesoftwarefdn/weaviate/blob/develop/LICENSE.md
 * DESIGN & CONCEPT: Bob van Luijt (@bobvanluijt)
 * CONTACT: hello@creativesoftwarefdn.org
 */package models

import (
	"github.com/creativesoftwarefdn/weaviate/entities/models"
	"github.com/go-openapi/strfmt"
)

// Action is a helper type that groups all the info about one action in a
// batch that belongs together, i.e. uuid, action body and error state.
//
// Consumers of a Action (i.e. database connector) should always check
// whether an error is already present by the time they receive a batch action.
// Errors can be introduced at all levels, e.g. validation.
//
// However, error'd actions are not removed to make sure that the list in
// Actions matches the order and content of the incoming batch request
type Action struct {
	OriginalIndex int
	Err           error
	Action        *models.Action
	UUID          strfmt.UUID
}

// Actions groups many Action items together. The order matches the
// order from the original request. It can be turned into the expected response
// type using the .Response() method
type Actions []Action

// Response uses the information contained in every Action (uuid, action
// body and error state) to form the expected response for the Batching
// request.
func (b Actions) Response() []*models.ActionsGetResponse {
	response := make([]*models.ActionsGetResponse, len(b), len(b))
	for i, action := range b {
		var errorResponse *models.ErrorResponse
		if action.Err != nil {
			errorResponse = errPayloadFromSingleErr(action.Err)
		}

		action.Action.ID = action.UUID
		response[i] = &models.ActionsGetResponse{
			Action: *action.Action,
			Result: &models.ActionsGetResponseAO1Result{
				Errors: errorResponse,
			},
		}
	}

	return response
}
