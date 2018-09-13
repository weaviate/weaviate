package janusgraph

import (
	"context"
	errors_ "errors"

	"fmt"

	"github.com/go-openapi/strfmt"

	"github.com/mitchellh/mapstructure"

	"github.com/creativesoftwarefdn/weaviate/config"
	"github.com/creativesoftwarefdn/weaviate/connectors/utils"
	"github.com/creativesoftwarefdn/weaviate/messages"
	"github.com/creativesoftwarefdn/weaviate/models"
	"github.com/creativesoftwarefdn/weaviate/schema"

	"github.com/creativesoftwarefdn/weaviate/gremlin"
	"github.com/creativesoftwarefdn/weaviate/gremlin/http_client"

	"github.com/sirupsen/logrus"
)

// Janusgraph has some basic variables.
// This is mandatory, only change it if you need aditional, global variables
type Janusgraph struct {
	client *http_client.Client
	kind   string

	config        Config
	serverAddress string
	schema        *schema.WeaviateSchema
	messaging     *messages.Messaging
}

// Config represents the config outline for Janusgraph. The Database config shoud be of the following form:
// "database_config" : {
//     "Url": "http://127.0.0.1:8182"
// }
// Notice that the port is the GRPC-port.
type Config struct {
	Url          string
	InitialKey   *string
	InitialToken *string
}

// GetName returns a unique connector name, this name is used to define the connector in the weaviate config
func (f *Janusgraph) GetName() string {
	return "janusgraph"
}

// SetConfig sets variables, which can be placed in the config file section "database_config: {}"
// can be custom for any connector, in the example below there is only host and port available.
//
// Important to bear in mind;
// 1. You need to add these to the struct Config in this document.
// 2. They will become available via f.config.[variable-name]
//
// 	"database": {
// 		"name": "janusgraph",
// 		"database_config" : {
// 			"url": "http://127.0.0.1:8081"
// 		}
// 	},
func (f *Janusgraph) SetConfig(configInput *config.Environment) error {

	// Mandatory: needed to add the JSON config represented as a map in f.config
	err := mapstructure.Decode(configInput.Database.DatabaseConfig, &f.config)

	// Example to: Validate if the essential  config is available, like host and port.
	if err != nil || len(f.config.Url) == 0 {
		return errors_.New("could not get Janusgraph url from config")
	}

	// If success return nil, otherwise return the error (see above)
	return nil
}

// SetSchema takes actionSchema and thingsSchema as an input and makes them available globally at f.schema
// In case you want to modify the schema, this is the place to do so.
// Note: When this function is called, the schemas (action + things) are already validated, so you don't have to build the validation.
func (f *Janusgraph) SetSchema(schemaInput *schema.WeaviateSchema) error {
	f.schema = schemaInput

	// If success return nil, otherwise return the error
	return nil
}

// SetMessaging is used to send messages to the service.
// Available message types are: f.messaging.Infomessage ...DebugMessage ...ErrorMessage ...ExitError (also exits the service) ...InfoMessage
func (f *Janusgraph) SetMessaging(m *messages.Messaging) error {

	// mandatory, adds the message functions to f.messaging to make them globally accessible.
	f.messaging = m

	// If success return nil, otherwise return the error
	return nil
}

// SetServerAddress is used to fill in a global variable with the server address, but can also be used
// to do some custom actions.
// Does not return anything
func (f *Janusgraph) SetServerAddress(addr string) {
	f.serverAddress = addr
}

// Init 1st initializes the schema in the database and 2nd creates a root key.
func (f *Janusgraph) Init() error {
	f.messaging.DebugMessage("Initializeing JanusGraph")

	err := f.ensureRootKeyExists()
	if err != nil {
		return err
	}

	return nil
}

func (j *Janusgraph) ensureRootKeyExists() error {
	q := gremlin.G.V().HasLabel(KEY_LABEL).HasBool("isRoot", true).Count()

	result, err := j.client.Execute(q)
	if err != nil {
		return err
	}

	i, err := result.OneInt()
	if err != nil {
		return err
	}

	if i == 0 {
		j.messaging.InfoMessage("No root key is found, a new one will be generated - RENEW DIRECTLY AFTER RECEIVING THIS MESSAGE")

		// Create new object and fill it
		keyObject := models.Key{}

		var hashedToken string
		var UUID strfmt.UUID

		if j.config.InitialKey != nil && j.config.InitialToken != nil {
			j.messaging.InfoMessage("Using the initial root key & token as specfied in the configuration")
			UUID = strfmt.UUID(*j.config.InitialKey)
			hashedToken = connutils.CreateRootKeyObjectFromTokenAndUUID(&keyObject, UUID, strfmt.UUID(*j.config.InitialToken))
		} else {
			hashedToken, UUID = connutils.CreateRootKeyObject(&keyObject)
		}

		// Add the root-key to the database
		ctx := context.Background()
		err = j.AddKey(ctx, &keyObject, UUID, hashedToken)

		if err != nil {
			return err
		}
	} else {
		j.messaging.InfoMessage("Keys are set and a rootkey is available")
	}

	return nil
}

// Connect connects to the Janusgraph websocket
func (f *Janusgraph) Connect() error {
	f.client = http_client.NewClient(f.config.Url)
	logger := logrus.New()
	logger.Level = logrus.DebugLevel
	f.client.SetLogger(logger)

	err := f.client.Ping()
	if err != nil {
		return fmt.Errorf("Could not connect to Gremlin server; %v", err)
	}

	f.messaging.InfoMessage("Sucessfully pinged Gremlin server")

	return nil
}
