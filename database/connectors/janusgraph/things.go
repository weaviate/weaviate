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
 */

package janusgraph

import (
	"context"

	"github.com/go-openapi/strfmt"

	"fmt"

	"github.com/creativesoftwarefdn/weaviate/database/schema"
	"github.com/creativesoftwarefdn/weaviate/database/schema/kind"
	connutils "github.com/creativesoftwarefdn/weaviate/database/utils"
	"github.com/creativesoftwarefdn/weaviate/models"
)

func (j *Janusgraph) AddThing(ctx context.Context, thing *models.Thing, UUID strfmt.UUID) error {
	sanitizedClassName := schema.AssertValidClassName(thing.AtClass)
	return j.addClass(kind.THING_KIND, sanitizedClassName, UUID, thing.AtContext, thing.CreationTimeUnix, thing.LastUpdateTimeUnix, thing.Schema)
}

func (j *Janusgraph) AddThingsBatch(ctx context.Context, things []*models.Thing, uuids []strfmt.UUID) error {
	return j.addThingsBatch(things, uuids)
}

func (j *Janusgraph) GetThing(ctx context.Context, UUID strfmt.UUID, thingResponse *models.ThingGetResponse) error {
	return j.getClass(kind.THING_KIND, UUID,
		&thingResponse.AtClass,
		&thingResponse.AtContext,
		&thingResponse.ThingID,
		&thingResponse.CreationTimeUnix,
		&thingResponse.LastUpdateTimeUnix,
		&thingResponse.Schema)
}

func (j *Janusgraph) GetThings(ctx context.Context, UUIDs []strfmt.UUID, response *models.ThingsListResponse) error {
	// TODO gh-612: Optimize query to perform just _one_ JanusGraph lookup.

	response.TotalResults = 0
	response.Things = make([]*models.ThingGetResponse, 0)

	for _, uuid := range UUIDs {
		var thing_response models.ThingGetResponse
		err := j.GetThing(ctx, uuid, &thing_response)

		if err == nil {
			response.TotalResults += 1
			response.Things = append(response.Things, &thing_response)
		} else {
			return fmt.Errorf("%s: thing with UUID '%v' not found", connutils.StaticThingNotFound, uuid)
		}
	}

	return nil
}

func (j *Janusgraph) ListThings(ctx context.Context, first int, offset int, wheres []*connutils.WhereQuery, response *models.ThingsListResponse) error {
	response.TotalResults = 0
	response.Things = make([]*models.ThingGetResponse, 0)

	return j.listClass(kind.THING_KIND, nil, first, offset, nil, func(uuid strfmt.UUID) {
		var thing_response models.ThingGetResponse
		err := j.GetThing(ctx, uuid, &thing_response)

		if err == nil {
			response.TotalResults += 1
			response.Things = append(response.Things, &thing_response)
		} else {
			// Silently ignorre the potentially removed things.
		}
	})
}

func (j *Janusgraph) UpdateThing(ctx context.Context, thing *models.Thing, UUID strfmt.UUID) error {
	sanitizedClassName := schema.AssertValidClassName(thing.AtClass)
	return j.updateClass(kind.THING_KIND, sanitizedClassName, UUID, thing.AtContext, thing.LastUpdateTimeUnix, thing.Schema)
}

func (j *Janusgraph) DeleteThing(ctx context.Context, thing *models.Thing, UUID strfmt.UUID) error {
	return j.deleteClass(kind.THING_KIND, UUID)
}

func (j *Janusgraph) HistoryThing(ctx context.Context, UUID strfmt.UUID, history *models.ThingHistory) error {
	return nil
}

func (j *Janusgraph) MoveToHistoryThing(ctx context.Context, thing *models.Thing, UUID strfmt.UUID, deleted bool) error {
	return nil
}
