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
package janusgraph

import (
	"context"
	"fmt"
	"net/url"

	client "github.com/SeMI-network/janus-spark-analytics/clients/go"
	"github.com/coreos/etcd/clientv3"
	"github.com/creativesoftwarefdn/weaviate/adapters/connectors"
	dbconnector "github.com/creativesoftwarefdn/weaviate/adapters/connectors"
	"github.com/creativesoftwarefdn/weaviate/adapters/connectors/janusgraph/state"
	"github.com/creativesoftwarefdn/weaviate/entities/schema"
	"github.com/creativesoftwarefdn/weaviate/usecases/config"

	"github.com/creativesoftwarefdn/weaviate/adapters/connectors/janusgraph/gremlin/http_client"

	"github.com/sirupsen/logrus"
)

// Janusgraph has some basic variables.
// This is mandatory, only change it if you need aditional, global variables
type Janusgraph struct {
	client *http_client.Client
	kind   string

	initialized  bool
	stateManager connectors.StateManager

	state state.JanusGraphConnectorState

	// config is the local connector-specific config, such as host:port discover
	// information
	config Config

	// appConfig is the global app-wide config. appConfig can be used if the
	// connectors behavior should depend on application-wide settings
	appConfig config.Config

	serverAddress string
	schema        schema.Schema
	logger        logrus.FieldLogger

	// etcd can be used as an external cache for the analytics api
	etcdClient *clientv3.Client

	// analyticsClient for background analytical jobs
	analyticsClient *client.Client
}

// New Janusgraph Connector
func New(config interface{}, appConfig config.Config) (dbconnector.DatabaseConnector, error) {
	j := &Janusgraph{
		appConfig: appConfig,
	}

	err := j.setConfig(config)
	if err != nil {
		return nil, err
	}

	return j, nil
}

// SetSchema takes actionSchema and thingsSchema as an input and makes them available globally at f.schema
func (f *Janusgraph) SetSchema(schemaInput schema.Schema) {
	f.schema = schemaInput
}

// SetLogger configures the logrus FieldLogger
func (f *Janusgraph) SetLogger(l logrus.FieldLogger) {
	f.logger = l
}

// SetServerAddress is used to fill in a global variable with the server address, but can also be used
// to do some custom actions.
// Does not return anything
func (f *Janusgraph) SetServerAddress(addr string) {
	f.serverAddress = addr
}

// Init 1st initializes the schema in the database and 2nd creates a root key.
func (f *Janusgraph) Init(ctx context.Context) error {
	f.logger.WithField("action", "database_init").
		WithField("connector", "janusgraph").
		Debug("initializing Janusgraph Connector")

	err := f.ensureBasicSchema(ctx)
	if err != nil {
		return err
	}

	if f.config.AnalyticsEngine.Enabled {
		etcdCfg := clientv3.Config{Endpoints: []string{f.appConfig.ConfigurationStorage.URL}}
		f.etcdClient, err = clientv3.New(etcdCfg)
		if err != nil {
			return fmt.Errorf("could not build etcd client: %v", err)
		}

		analyticsURL, err := url.Parse(f.config.AnalyticsEngine.URL)
		if err != nil {
			return fmt.Errorf("could not parse URL for analytics client: %v", err)
		}

		f.analyticsClient = client.New(analyticsURL)
	}

	f.initialized = true

	return nil
}

// Connect connects to the Janusgraph websocket
func (f *Janusgraph) Connect() error {
	f.client = http_client.NewClient(f.config.URL)
	logger := logrus.New()
	logger.Level = logrus.DebugLevel
	f.client.SetLogger(logger)

	err := f.client.Ping()
	if err != nil {
		return fmt.Errorf("Could not connect to Gremlin server; %v", err)
	}

	f.logger.WithField("action", "database_init").
		WithField("connector", "janusgraph").
		Debug("established connection to Gremlin server")

	return nil
}

// Link a connector to this state manager.
// When the internal state of some connector is updated, this state connector will call SetState on the provided conn.
func (j *Janusgraph) SetStateManager(manager connectors.StateManager) {
	j.stateManager = manager
}
