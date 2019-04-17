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

	batchmodels "github.com/creativesoftwarefdn/weaviate/adapters/handlers/rest/batch/models"
	"github.com/creativesoftwarefdn/weaviate/entities/models"
	"github.com/creativesoftwarefdn/weaviate/entities/schema"
	"github.com/creativesoftwarefdn/weaviate/entities/schema/kind"
)

func (j *Janusgraph) AddAction(ctx context.Context, action *models.Action, UUID strfmt.UUID) error {
	sanitizedClassName := schema.AssertValidClassName(action.Class)
	return j.addClass(kind.ACTION_KIND, sanitizedClassName, UUID, action.CreationTimeUnix, action.LastUpdateTimeUnix, action.Schema)
}

func (j *Janusgraph) AddActionsBatch(ctx context.Context, actions batchmodels.Actions) error {
	return j.addActionsBatch(actions)
}

func (j *Janusgraph) GetAction(ctx context.Context, UUID strfmt.UUID, actionResponse *models.Action) error {
	return j.getClass(kind.ACTION_KIND, UUID,
		&actionResponse.Class,
		&actionResponse.ID,
		&actionResponse.CreationTimeUnix,
		&actionResponse.LastUpdateTimeUnix,
		&actionResponse.Schema)
}

func (j *Janusgraph) ListActions(ctx context.Context, limit int, response *models.ActionsListResponse) error {
	response.TotalResults = 0
	response.Actions = make([]*models.Action, 0)

	return j.listClass(kind.ACTION_KIND, nil, limit, nil, func(uuid strfmt.UUID) {
		var action_response models.Action
		err := j.GetAction(ctx, uuid, &action_response)

		if err == nil {
			response.TotalResults += 1
			response.Actions = append(response.Actions, &action_response)
		} else {
			// Silently ignorre the potentially removed actions.
		}
	})
}

func (j *Janusgraph) UpdateAction(ctx context.Context, action *models.Action, UUID strfmt.UUID) error {
	sanitizedClassName := schema.AssertValidClassName(action.Class)
	return j.updateClass(kind.ACTION_KIND, sanitizedClassName, UUID, action.LastUpdateTimeUnix, action.Schema)
}

func (j *Janusgraph) DeleteAction(ctx context.Context, action *models.Action, UUID strfmt.UUID) error {
	return j.deleteClass(kind.ACTION_KIND, UUID)
}
