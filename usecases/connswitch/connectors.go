/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2019 Weaviate. All rights reserved.
 * LICENSE WEAVIATE OPEN SOURCE: https://www.semi.technology/playbook/playbook/contract-weaviate-OSS.html
 * LICENSE WEAVIATE ENTERPRISE: https://www.semi.technology/playbook/contract-weaviate-enterprise.html
 * CONCEPT: Bob van Luijt (@bobvanluijt)
 * CONTACT: hello@semi.technology
 */

// Package connswitch allows for switching the plugged in database at startup
package connswitch

import (
	"fmt"

	dbconnector "github.com/semi-technologies/weaviate/adapters/connectors"
	"github.com/semi-technologies/weaviate/adapters/connectors/foobar"
	"github.com/semi-technologies/weaviate/adapters/connectors/janusgraph"
	"github.com/semi-technologies/weaviate/usecases/config"
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
