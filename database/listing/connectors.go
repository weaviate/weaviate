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
package listing

import (
	"fmt"

	"github.com/creativesoftwarefdn/weaviate/config"
	dbconnector "github.com/creativesoftwarefdn/weaviate/database/connectors"
	"github.com/creativesoftwarefdn/weaviate/database/connectors/foobar"
	"github.com/creativesoftwarefdn/weaviate/database/connectors/janusgraph"
)

// Build a new connector based on it's name. Returns nil if the connector is unknown.
func NewConnector(name string, config interface{}, appConfig config.Environment) (err error, conn dbconnector.DatabaseConnector) {
	switch name {
	case "janusgraph":
		err, conn = janusgraph.New(config, appConfig)
	case "foobar":
		err, conn = foobar.New(config, appConfig)
	default:
		err = fmt.Errorf("No connector with the name '%s' exists!", name)
	}
	return
}
