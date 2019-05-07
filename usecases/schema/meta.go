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

import (
	"context"

	"github.com/semi-technologies/weaviate/entities/schema/kind"
	"github.com/go-openapi/strfmt"
)

// UpdateMeta for a kind
func (m *Manager) UpdateMeta(ctx context.Context, kind kind.Kind,
	atContext strfmt.URI, maintainer strfmt.Email, name string) error {
	semanticSchema := m.state.SchemaFor(kind)
	semanticSchema.Maintainer = maintainer
	semanticSchema.Name = name

	return m.saveSchema(ctx)
}
