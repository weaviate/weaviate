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

// Thing is a helper type that groups all the info about one thing in a
// batch that belongs together, i.e. uuid, thing body and error state.
//
// Consumers of a Thing (i.e. database connector) should always check
// whether an error is already present by the time they receive a batch thing.
// Errors can be introduced at all levels, e.g. validation.
//
// However, error'd things are not removed to make sure that the list in
// Things matches the order and content of the incoming batch request
type Thing struct {
	OriginalIndex int
	Err           error
	Thing         *models.Thing
	UUID          strfmt.UUID
}

// Things groups many Thing items together. The order matches the
// order from the original request. It can be turned into the expected response
// type using the .Response() method
type Things []Thing

// Response uses the information contained in every Thing (uuid, thing
// body and error state) to form the expected response for the Batching
// request.
func (b Things) Response() []*models.ThingsGetResponse {
	response := make([]*models.ThingsGetResponse, len(b), len(b))
	for i, thing := range b {
		var errorResponse *models.ErrorResponse
		if thing.Err != nil {
			errorResponse = errPayloadFromSingleErr(thing.Err)
		}

		thing.Thing.ID = thing.UUID
		response[i] = &models.ThingsGetResponse{
			Thing: *thing.Thing,
			Result: &models.ThingsGetResponseAO1Result{
				Errors: errorResponse,
			},
		}
	}

	return response
}
