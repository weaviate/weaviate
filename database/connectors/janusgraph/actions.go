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

package janusgraph

import (
	"context"

	"github.com/go-openapi/strfmt"

	"github.com/creativesoftwarefdn/weaviate/database/connectors/utils"
	"github.com/creativesoftwarefdn/weaviate/database/schema"
	"github.com/creativesoftwarefdn/weaviate/database/schema/kind"
	"github.com/creativesoftwarefdn/weaviate/models"
)

func (j *Janusgraph) AddAction(ctx context.Context, action *models.Action, UUID strfmt.UUID) error {
	sanitizedClassName := schema.AssertValidClassName(action.AtClass)
	return j.addClass(kind.ACTION_KIND, sanitizedClassName, UUID, action.AtContext, action.CreationTimeUnix, action.LastUpdateTimeUnix, action.Key, action.Schema)
}

func (j *Janusgraph) GetAction(ctx context.Context, UUID strfmt.UUID, actionResponse *models.ActionGetResponse) error {
	return j.getClass(kind.ACTION_KIND, UUID,
		&actionResponse.AtClass,
		&actionResponse.AtContext,
		&actionResponse.ActionID,
		&actionResponse.CreationTimeUnix,
		&actionResponse.LastUpdateTimeUnix,
		&actionResponse.Schema,
		&actionResponse.Key)
}

func (j *Janusgraph) GetActions(ctx context.Context, UUIDs []strfmt.UUID, actionsResponse *models.ActionsListResponse) error {
	return nil
}

func (j *Janusgraph) ListActions(ctx context.Context, UUID strfmt.UUID, first int, offset int, wheres []*connutils.WhereQuery, actionsResponse *models.ActionsListResponse) error {
	return nil
}

func (j *Janusgraph) UpdateAction(ctx context.Context, action *models.Action, UUID strfmt.UUID) error {
	return nil
}

func (j *Janusgraph) DeleteAction(ctx context.Context, action *models.Action, UUID strfmt.UUID) error {
	return nil
}

func (j *Janusgraph) HistoryAction(ctx context.Context, UUID strfmt.UUID, history *models.ActionHistory) error {
	return nil
}

func (j *Janusgraph) MoveToHistoryAction(ctx context.Context, action *models.Action, UUID strfmt.UUID, deleted bool) error {
	return nil
}
