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
	batchmodels "github.com/creativesoftwarefdn/weaviate/restapi/batch/models"
)

func (j *Janusgraph) AddThing(ctx context.Context, thing *models.Thing, UUID strfmt.UUID) error {
	sanitizedClassName := schema.AssertValidClassName(thing.Class)
	return j.addClass(kind.THING_KIND, sanitizedClassName, UUID, thing.CreationTimeUnix, thing.LastUpdateTimeUnix, thing.Schema)
}

func (j *Janusgraph) AddThingsBatch(ctx context.Context, things batchmodels.Things) error {
	return j.addThingsBatch(things)
}

func (j *Janusgraph) GetThing(ctx context.Context, UUID strfmt.UUID, thingResponse *models.Thing) error {
	return j.getClass(kind.THING_KIND, UUID,
		&thingResponse.Class,
		&thingResponse.ID,
		&thingResponse.CreationTimeUnix,
		&thingResponse.LastUpdateTimeUnix,
		&thingResponse.Schema)
}

func (j *Janusgraph) GetThings(ctx context.Context, UUIDs []strfmt.UUID, response *models.ThingsListResponse) error {
	// TODO gh-612: Optimize query to perform just _one_ JanusGraph lookup.

	response.TotalResults = 0
	response.Things = make([]*models.Thing, 0)

	for _, uuid := range UUIDs {
		var thing_response models.Thing
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

func (j *Janusgraph) ListThings(ctx context.Context, limit int, wheres []*connutils.WhereQuery, response *models.ThingsListResponse) error {
	response.TotalResults = 0
	response.Things = make([]*models.Thing, 0)

	return j.listClass(kind.THING_KIND, nil, limit, nil, func(uuid strfmt.UUID) {
		var thing_response models.Thing
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
	sanitizedClassName := schema.AssertValidClassName(thing.Class)
	return j.updateClass(kind.THING_KIND, sanitizedClassName, UUID, thing.LastUpdateTimeUnix, thing.Schema)
}

func (j *Janusgraph) DeleteThing(ctx context.Context, thing *models.Thing, UUID strfmt.UUID) error {
	return j.deleteClass(kind.THING_KIND, UUID)
}
