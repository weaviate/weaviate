//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package schema

import (
	"context"
	"fmt"

	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/usecases/sharding"
)

// GetSchema retrieves a locally cached copy of the schema
func (m *Manager) GetSchema(principal *models.Principal) (schema.Schema, error) {
	err := m.authorizer.Authorize(principal, "list", "schema/*")
	if err != nil {
		return schema.Schema{}, err
	}

	return schema.Schema{
		Objects: m.state.ObjectSchema,
	}, nil
}

// GetSchemaSkipAuth can never be used as a response to a user request as it
// could leak the schema to an unauthorized user, is intended to be used for
// non-user triggered processes, such as regular updates / maintenance / etc
func (m *Manager) GetSchemaSkipAuth() schema.Schema {
	return schema.Schema{
		Objects: m.state.ObjectSchema,
	}
}

func (m *Manager) IndexedInverted(className, propertyName string) bool {
	class := m.getClassByName(className)
	if class == nil {
		return false
	}

	for _, prop := range class.Properties {
		if prop.Name == propertyName {
			if prop.IndexInverted == nil {
				return true
			}

			return *prop.IndexInverted
		}
	}

	return false
}

func (m *Manager) GetClass(ctx context.Context, principal *models.Principal,
	name string,
) (*models.Class, error) {
	err := m.authorizer.Authorize(principal, "list", "schema/*")
	if err != nil {
		return nil, err
	}

	return m.getClassByName(name), nil
}

func (m *Manager) getClassByName(name string) *models.Class {
	s := schema.Schema{
		Objects: m.state.ObjectSchema,
	}

	return s.FindClassByName(schema.ClassName(name))
}

func (m *Manager) ShardingState(className string) *sharding.State {
	return m.state.ShardingState[className]
}

func (m *Manager) GetShardsStatus(ctx context.Context, principal *models.Principal,
	className string,
) (models.ShardStatusList, error) {
	err := m.authorizer.Authorize(principal, "list", fmt.Sprintf("schema/%s/shards", className))
	if err != nil {
		return nil, err
	}

	shardsStatus, err := m.migrator.GetShardsStatus(ctx, className)
	if err != nil {
		return nil, err
	}

	resp := models.ShardStatusList{}

	for name, status := range shardsStatus {
		resp = append(resp, &models.ShardStatusGetResponse{
			Name:   name,
			Status: status,
		})
	}

	return resp, nil
}
