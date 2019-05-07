/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2019 Weaviate. All rights reserved.
 * LICENSE: https://github.com/semi-technologies/weaviate/blob/develop/LICENSE.md
 * DESIGN & CONCEPT: Bob van Luijt (@bobvanluijt)
 * CONTACT: hello@semi.technology
 */package schema

import "github.com/semi-technologies/weaviate/entities/schema"

// GetSchema retrieves a locally cached copy of the schema
func (m *Manager) GetSchema() schema.Schema {
	return schema.Schema{
		Actions: m.state.ActionSchema,
		Things:  m.state.ThingSchema,
	}
}
