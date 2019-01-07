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
package listing

import (
	"fmt"
	dbconnector "github.com/creativesoftwarefdn/weaviate/database/connectors"
	"github.com/creativesoftwarefdn/weaviate/database/connectors/foobar"
	"github.com/creativesoftwarefdn/weaviate/database/connectors/janusgraph"
)

// Build a new connector based on it's name. Returns nil if the connector is unknown.
func NewConnector(name string, config interface{}) (err error, conn dbconnector.DatabaseConnector) {
	switch name {
	case "janusgraph":
		err, conn = janusgraph.New(config)
	case "foobar":
		err, conn = foobar.New(config)
	default:
		err = fmt.Errorf("No connector with the name '%s' exists!", name)
	}
	return
}
