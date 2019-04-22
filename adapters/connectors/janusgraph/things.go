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

	"github.com/creativesoftwarefdn/weaviate/entities/models"
	"github.com/creativesoftwarefdn/weaviate/entities/schema"
	"github.com/creativesoftwarefdn/weaviate/entities/schema/kind"
	"github.com/creativesoftwarefdn/weaviate/usecases/kinds"
)

func (j *Janusgraph) AddThing(ctx context.Context, thing *models.Thing, UUID strfmt.UUID) error {
	sanitizedClassName := schema.AssertValidClassName(thing.Class)
	return j.addClass(ctx, kind.Thing, sanitizedClassName, UUID, thing.CreationTimeUnix, thing.LastUpdateTimeUnix, thing.Schema)
}

func (j *Janusgraph) AddThingsBatch(ctx context.Context, things kinds.BatchThings) error {
	return j.addThingsBatch(ctx, things)
}

func (j *Janusgraph) GetThing(ctx context.Context, UUID strfmt.UUID, thingResponse *models.Thing) error {
	return j.getClass(ctx, kind.Thing, UUID,
		&thingResponse.Class,
		&thingResponse.ID,
		&thingResponse.CreationTimeUnix,
		&thingResponse.LastUpdateTimeUnix,
		&thingResponse.Schema)
}

func (j *Janusgraph) ListThings(ctx context.Context, limit int, response *models.ThingsListResponse) error {
	response.TotalResults = 0
	response.Things = make([]*models.Thing, 0)

	return j.listClass(ctx, kind.Thing, nil, limit, nil, func(uuid strfmt.UUID) {
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
	return j.updateClass(ctx, kind.Thing, sanitizedClassName, UUID, thing.LastUpdateTimeUnix, thing.Schema)
}

func (j *Janusgraph) DeleteThing(ctx context.Context, thing *models.Thing, UUID strfmt.UUID) error {
	return j.deleteClass(ctx, kind.Thing, UUID)
}
