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

	"github.com/go-openapi/strfmt"
)

// UpdateMeta for object
func (m *Manager) UpdateMeta(ctx context.Context,
	atContext strfmt.URI, maintainer strfmt.Email, name string) error {
	semanticSchema := m.state.SchemaFor()
	semanticSchema.Maintainer = maintainer
	semanticSchema.Name = name

	return m.saveSchema(ctx)
}
