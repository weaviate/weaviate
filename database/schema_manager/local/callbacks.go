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
package local

import (
	db_schema "github.com/creativesoftwarefdn/weaviate/database/schema"
)

func (l *localSchemaManager) RegisterSchemaUpdateCallback(callback func(updatedSchema db_schema.Schema)) {
	l.callbacks = append(l.callbacks, callback)
}

func (l *localSchemaManager) TriggerSchemaUpdateCallbacks() {
	schema := l.GetSchema()

	for _, cb := range l.callbacks {
		cb(schema)
	}
}
