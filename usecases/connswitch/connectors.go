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

// Package connswitch allows for switching the plugged in database at startup
package connswitch

import (
	"fmt"

	dbconnector "github.com/creativesoftwarefdn/weaviate/adapters/connectors"
	"github.com/creativesoftwarefdn/weaviate/adapters/connectors/foobar"
	"github.com/creativesoftwarefdn/weaviate/adapters/connectors/janusgraph"
	"github.com/creativesoftwarefdn/weaviate/usecases/config"
)

// NewConnector builds a new connector based on it's name or errors
func NewConnector(name string, config interface{}, appConfig config.Config) (conn dbconnector.DatabaseConnector, err error) {
	switch name {
	case "janusgraph":
		conn, err = janusgraph.New(config, appConfig)
	case "foobar":
		conn, err = foobar.New(config, appConfig)
	default:
		err = fmt.Errorf("no connector with the name '%s' exists", name)
	}
	return
}
