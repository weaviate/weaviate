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
 */package schema

import "github.com/creativesoftwarefdn/weaviate/entities/schema"

// GetSchema returns a local cached version of the current schema
func (m *Manager) GetSchema() (schema.Schema, error) {
	connectorLock, err := m.db.ConnectorLock()
	if err != nil {
		return schema.Schema{}, err
	}

	defer connectorLock.Unlock()

	return connectorLock.GetSchema(), nil
}
