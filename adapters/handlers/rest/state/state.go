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

package state

import (
	"github.com/creativesoftwarefdn/weaviate/adapters/connectors"
	"github.com/creativesoftwarefdn/weaviate/database"
	"github.com/creativesoftwarefdn/weaviate/usecases/auth/authentication/anonymous"
	"github.com/creativesoftwarefdn/weaviate/usecases/auth/authentication/oidc"
	"github.com/creativesoftwarefdn/weaviate/usecases/config"
	"github.com/creativesoftwarefdn/weaviate/usecases/locks"
	"github.com/creativesoftwarefdn/weaviate/usecases/network"
	"github.com/sirupsen/logrus"
)

// State is the only source of appliaction-wide state
// NOTE: This is not true yet, se gh-723
// TODO: remove dependencies to anything that's not an ent or uc
type State struct {
	Database        database.Database
	Network         network.Network
	OIDC            *oidc.Client
	AnonymousAccess *anonymous.Client
	ServerConfig    *config.WeaviateConfig
	SchemaManager   database.SchemaManager
	Connector       connectors.DatabaseConnector
	Locks           locks.ConnectorSchemaLock
	Logger          *logrus.Logger
}
