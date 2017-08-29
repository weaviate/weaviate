/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 Weaviate. All rights reserved.
 * LICENSE: https://github.com/weaviate/weaviate/blob/master/LICENSE
 * AUTHOR: Bob van Luijt (bob@weaviate.com)
 * See www.weaviate.com for details
 * Contact: @weaviate_iot / yourfriends@weaviate.com
 */

package schema

import (
	"encoding/json"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"

	"github.com/weaviate/weaviate/config"
	"github.com/weaviate/weaviate/connectors/utils"
	weaviate_error "github.com/weaviate/weaviate/error"
)

type schemaProperties struct {
	localFile      string
	configLocation string
	schema         Schema
}

// WeaviateSchema represents the used schema's
type WeaviateSchema struct {
	actionSchema schemaProperties
	thingSchema  schemaProperties
}

// LoadSchema from config locations
func (f *WeaviateSchema) LoadSchema(usedConfig *config.Environment) error {
	f.thingSchema.configLocation = usedConfig.Schemas.Thing
	f.actionSchema.configLocation = usedConfig.Schemas.Action

	configFiles := map[string]*schemaProperties{
		"Action": &f.actionSchema,
		"Thing":  &f.thingSchema,
	}

	for cfk, cfv := range configFiles {
		// Continue loop if the file is not set in the config.
		if len(cfv.configLocation) == 0 {
			weaviate_error.ExitError(78, "schema file for '"+cfk+"' not given in config (path: *env*/schemas/"+cfk+"')")
			continue
		}

		// Validate if given location is URL or local file
		_, err := url.ParseRequestURI(cfv.configLocation)

		// With no error, it is an URL
		if err == nil {
			log.Println(cfk + ": Downloading schema file...")
			cfv.localFile = "temp/schema" + string(connector_utils.GenerateUUID()) + ".json"

			// Create local file
			schemaFile, _ := os.Create(cfv.localFile)
			defer schemaFile.Close()

			// Get the file from online
			resp, err := http.Get(cfv.configLocation)
			if err != nil {
				log.Println("ERROR: " + cfk + ": Schema file '" + cfv.localFile + "' could not be downloaded.")
				return err
			}
			defer resp.Body.Close()

			// Write file to local file
			b, _ := io.Copy(schemaFile, resp.Body)
			log.Println(cfk+": Download complete, file size: ", b)
		} else {
			log.Println(cfk + ": Given schema location is not a valid URL, using local file.")

			// Given schema location is not a valid URL, assume it is a local file
			cfv.localFile = cfv.configLocation
		}

		// Read local file which is either just downloaded or given in config.
		log.Println(cfk + ": Read local file " + cfv.localFile)

		fileContents, err := ioutil.ReadFile(cfv.localFile)

		// Return error when error is given reading file.
		if err != nil {
			log.Println("ERROR: " + cfk + ": Schema file '" + cfv.localFile + "' could not be found.")
			return err
		}

		// Merge JSON into Schema objects
		err = json.Unmarshal([]byte(fileContents), &cfv.schema)
		log.Println(cfk + ": File is loaded.")

		// Return error when error is given reading file.
		if err != nil {
			log.Println("ERROR: " + cfk + ": Can not parse schema.")
			return err
		}
	}

	return nil
}
