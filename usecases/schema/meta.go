//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package schema

import (
	"context"

	"github.com/go-openapi/strfmt"
)

// UpdateMeta for object
func (m *Manager) UpdateMeta(ctx context.Context,
	atContext strfmt.URI, maintainer strfmt.Email, name string,
) error {
	m.state.ObjectSchema.Maintainer = maintainer
	m.state.ObjectSchema.Name = name

	return m.saveSchema(ctx, nil)
}
