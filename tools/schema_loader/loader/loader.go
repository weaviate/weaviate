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
// Utility to load a database schema into a Weaviate instance.
package loader

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"reflect"

	apiclient "github.com/creativesoftwarefdn/weaviate/client"
	apischema "github.com/creativesoftwarefdn/weaviate/client/schema"
	"github.com/creativesoftwarefdn/weaviate/database/schema"
	"github.com/creativesoftwarefdn/weaviate/models"
	"github.com/go-openapi/runtime"
	httptransport "github.com/go-openapi/runtime/client"
	"github.com/go-openapi/strfmt"
	log "github.com/sirupsen/logrus"
)

type Loader interface {
	SetLogger(logger *log.Logger) Loader
	SetTransport(transport *httptransport.Runtime) Loader
	SetKeyAndToken(key string, token string) Loader

	FromSchemaFiles(actionSchemaFile string, thingSchemaFile string) Loader
	ReplaceExistingClasses(replaceExisting bool) Loader
	Load() error
}

type loader struct {
	log *log.Logger

	// Weaviate connection information
	host   string
	port   string
	scheme string
	key    string
	token  string

	// Schema files
	actionSchemaFile string
	thingSchemaFile  string

	// Behavior
	replaceExisting bool

	// Client and auth
	auth      runtime.ClientAuthInfoWriterFunc
	transport *httptransport.Runtime
	client    *apiclient.WeaviateDecentralisedKnowledgeGraph

	// Internal state
	schema         schema.Schema  // schema to be imported
	weaviateSchema *schema.Schema // The schema as defined in the current database.
}

func New() Loader {
	return &loader{}
}

func (l *loader) SetLogger(logger *log.Logger) Loader {
	l.log = logger
	return l
}

func (l *loader) SetTransport(transport *httptransport.Runtime) Loader {
	l.transport = transport
	return l
}

func (l *loader) SetKeyAndToken(key string, token string) Loader {
	l.key = key
	l.token = token
	return l
}

func (l *loader) ReplaceExistingClasses(replaceExisting bool) Loader {
	l.replaceExisting = replaceExisting
	return l
}

func (l *loader) FromSchemaFiles(actionSchemaFile string, thingSchemaFile string) Loader {
	l.actionSchemaFile = actionSchemaFile
	l.thingSchemaFile = thingSchemaFile
	return l
}

type httpLogger struct {
	log *log.Logger
}

func (h *httpLogger) Printf(format string, args ...interface{}) {
	h.log.Infof("HTTP LOG: "+format, args...)
}

func (h *httpLogger) Debugf(format string, args ...interface{}) {
	h.log.Debugf("HTTP DEBUG: "+format, args...)
}

func (l *loader) Load() error {
	if l.log == nil {
		l.log = log.New()
		l.log.SetOutput(ioutil.Discard)
	}

	// Build client
	l.transport.SetDebug(true)
	l.transport.SetLogger(&httpLogger{log: l.log})
	l.client = apiclient.New(l.transport, strfmt.Default)

	// Create an auth writer that both sets the api key & token.
	l.auth = func(r runtime.ClientRequest, _ strfmt.Registry) error {
		err := r.SetHeaderParam("X-API-KEY", string(l.key))
		if err != nil {
			return err
		}

		return r.SetHeaderParam("X-API-TOKEN", l.token)
	}

	var err error

	// Load schema from disk
	if err = l.loadSchemaFromDisk(); err != nil {
		return err
	}

	// If we're allowed to replace classes, remove them.
	if l.replaceExisting {
		if err = l.getWeaviateSchema(); err != nil {
			return err
		}
		if err = l.maybeDropActionClasses(); err != nil {
			return err
		}
		if err = l.maybeDropThingClasses(); err != nil {
			return err
		}
	}

	// Perform the loading of the schema in Weaviate
	if err = l.defineActionClasses(); err != nil {
		return err
	}

	if err = l.defineThingClasses(); err != nil {
		return err
	}

	if err = l.addActionProperties(); err != nil {
		return err
	}

	if err = l.addThingProperties(); err != nil {
		return err
	}

	return nil
}

func (l *loader) loadSchemaFromDisk() error {
	l.log.Infof("Loading schema from %s and %s", l.actionSchemaFile, l.thingSchemaFile)
	err, actionSchema := loadSemanticSchemaFromDisk(l.actionSchemaFile)
	if err != nil {
		return fmt.Errorf("Could not load action schema from disk; %v", err)
	}

	err, thingSchema := loadSemanticSchemaFromDisk(l.thingSchemaFile)
	if err != nil {
		return fmt.Errorf("Could not load thing schema from disk; %v", err)
	}

	l.schema = schema.Schema{
		Actions: actionSchema,
		Things:  thingSchema,
	}

	return nil
}

func loadSemanticSchemaFromDisk(path string) (error, *models.SemanticSchema) {
	file, err := os.Open(path)
	if err != nil {
		return err, nil
	}

	decoder := json.NewDecoder(file)

	var schema models.SemanticSchema
	err = decoder.Decode(&schema)

	if err != nil {
		return err, nil
	}

	return nil, &schema
}

func (l *loader) getWeaviateSchema() error {
	l.log.Info("Fetching existing schema from Weaviate")

	response, err := l.client.Schema.WeaviateSchemaDump(nil, nil)
	l.log.Info("Fetching existing schema from Weaviate DONE")
	if err != nil {
		return err
	}

	l.weaviateSchema = &schema.Schema{
		Actions: response.Payload.Actions,
		Things:  response.Payload.Things,
	}

	return nil
}

func (l *loader) maybeDropActionClasses() error {
	l.log.Info("Droping conflicting action classes")
	for _, class := range l.weaviateSchema.Actions.Classes {
		l.log.Debugf("Found action class in Weaviate: %s", class.Class)

		sanitizedName := schema.AssertValidClassName(class.Class)
		// Check if this is in the schema to import
		if l.schema.FindClassByName(sanitizedName) != nil {
			params := apischema.NewWeaviateSchemaActionsDeleteParams().WithClassName(class.Class)
			_, err := l.client.Schema.WeaviateSchemaActionsDelete(params, nil)
			if err != nil {
				err = fmt.Errorf("Could not delete conflicting action class %s", class.Class)
				l.log.Debug(err.Error())
				return err
			}
		}
	}
	return nil
}

func (l *loader) maybeDropThingClasses() error {
	l.log.Info("Dropping conflicting thing classes")
	for _, class := range l.weaviateSchema.Things.Classes {
		l.log.Debugf("Found thing class in Weaviate: %s", class.Class)

		sanitizedName := schema.AssertValidClassName(class.Class)
		// Check if this is in the schema to import
		if l.schema.FindClassByName(sanitizedName) != nil {
			params := apischema.NewWeaviateSchemaThingsDeleteParams().WithClassName(class.Class)
			_, err := l.client.Schema.WeaviateSchemaThingsDelete(params, nil)
			if err != nil {
				err = fmt.Errorf("Could not delete conflicting thing class %s", class.Class)
				l.log.Debug(err.Error())
				return err
			}
		}
	}
	return nil
}

func (l *loader) defineActionClasses() error {
	l.log.Info("Defining action classes")

	for _, class := range l.schema.Actions.Classes {
		l.log.Debugf("Defining action class %s", class.Class)

		// Shallow copy of the struct
		var classToAdd models.SemanticSchemaClass = *class
		// Remove properties
		classToAdd.Properties = nil

		params := apischema.NewWeaviateSchemaActionsCreateParams().WithActionClass(&classToAdd)
		_, err := l.client.Schema.WeaviateSchemaActionsCreate(params, nil)
		if err != nil {
			l.log.Debugf("Could not create action class: %s", debugResponse(err))
			return err
		}
	}
	return nil
}

func (l *loader) defineThingClasses() error {
	l.log.Info("Defining thing classes")

	for _, class := range l.schema.Things.Classes {
		// Shallow copy of the struct
		var classToAdd models.SemanticSchemaClass = *class
		// Remove properties
		classToAdd.Properties = nil

		params := apischema.NewWeaviateSchemaThingsCreateParams().WithThingClass(&classToAdd)
		_, err := l.client.Schema.WeaviateSchemaThingsCreate(params, nil)
		if err != nil {
			l.log.Debugf("Could not create thing class: %s", debugResponse(err))
			return err
		}
	}
	return nil
}

func (l *loader) addActionProperties() error {
	l.log.Info("Adding action properties")

	for _, class := range l.schema.Actions.Classes {
		for _, property := range class.Properties {
			l.log.Infof("Adding action property %s for action class %s", property.Name, class.Class)

			params := apischema.NewWeaviateSchemaActionsPropertiesAddParams().WithClassName(class.Class).WithBody(property)
			_, err := l.client.Schema.WeaviateSchemaActionsPropertiesAdd(params, nil)
			if err != nil {
				l.log.Debugf("Could not add property %s for action class %s: %s", property.Name, class.Class, debugResponse(err))
				return err
			}
		}
	}

	return nil
}

func (l *loader) addThingProperties() error {
	l.log.Info("Adding thing properties")

	for _, class := range l.schema.Things.Classes {
		for _, property := range class.Properties {
			l.log.Infof("Adding thing property %s for thing class %s", property.Name, class.Class)

			params := apischema.NewWeaviateSchemaThingsPropertiesAddParams().WithClassName(class.Class).WithBody(property)
			_, err := l.client.Schema.WeaviateSchemaThingsPropertiesAdd(params, nil)
			if err != nil {
				l.log.Debugf("Could not add property %s for thing class %s: %s", property.Name, class.Class, debugResponse(err))
				return err
			}
		}
	}

	return nil
}

func debugResponse(err interface{}) string {
	errorPayload, _ := json.MarshalIndent(err, "", " ")
	return fmt.Sprintf("Error: %s. Response: %s", getType(err), errorPayload)
}

// Get type name of some value, according to https://stackoverflow.com/questions/35790935/using-reflection-in-go-to-get-the-name-of-a-struct
func getType(myvar interface{}) string {
	if t := reflect.TypeOf(myvar); t.Kind() == reflect.Ptr {
		return "*" + t.Elem().Name()
	} else {
		return t.Name()
	}
}
