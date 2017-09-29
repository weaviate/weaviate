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
	errors_ "errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"

	"github.com/weaviate/weaviate/config"
	"github.com/weaviate/weaviate/connectors/utils"
	"github.com/weaviate/weaviate/messages"
)

type schemaProperties struct {
	localFile                string
	schemaLocationFromConfig string
	Schema                   Schema
}

// WeaviateSchema represents the used schema's
type WeaviateSchema struct {
	ActionSchema schemaProperties
	ThingSchema  schemaProperties
}

// LoadSchema from config locations
func (f *WeaviateSchema) LoadSchema(usedConfig *config.Environment) error {
	f.ThingSchema.schemaLocationFromConfig = usedConfig.Schemas.Thing
	f.ActionSchema.schemaLocationFromConfig = usedConfig.Schemas.Action

	configFiles := map[string]*schemaProperties{
		"Action": &f.ActionSchema,
		"Thing":  &f.ThingSchema,
	}

	for cfk, cfv := range configFiles {
		// Continue loop if the file is not set in the config.
		if len(cfv.schemaLocationFromConfig) == 0 {
			return errors_.New("schema file for '" + cfk + "' not given in config (path: *env*/schemas/" + cfk + "')")
		}

		// Validate if given location is URL or local file
		_, err := url.ParseRequestURI(cfv.schemaLocationFromConfig)

		// With no error, it is an URL
		if err == nil {
			messages.InfoMessage(cfk + ": Downloading schema file...")
			cfv.localFile = "temp/schema" + string(connutils.GenerateUUID()) + ".json"

			// Create local file
			schemaFile, _ := os.Create(cfv.localFile)
			defer schemaFile.Close()

			// Get the file from online
			resp, err := http.Get(cfv.schemaLocationFromConfig)
			if err != nil {
				messages.ErrorMessage(cfk + ": Schema file '" + cfv.localFile + "' could not be downloaded.")
				return err
			}
			defer resp.Body.Close()

			// Write file to local file
			b, _ := io.Copy(schemaFile, resp.Body)
			messages.InfoMessage(fmt.Sprintf("%s: Download complete, file size: %d", cfk, b))
		} else {
			messages.InfoMessage(cfk + ": Given schema location is not a valid URL, using local file.")

			// Given schema location is not a valid URL, assume it is a local file
			cfv.localFile = cfv.schemaLocationFromConfig
		}

		// Read local file which is either just downloaded or given in config.
		messages.InfoMessage(cfk + ": Read local file " + cfv.localFile)

		fileContents, err := ioutil.ReadFile(cfv.localFile)

		// Return error when error is given reading file.
		if err != nil {
			messages.ErrorMessage(cfk + ": Schema file '" + cfv.localFile + "' could not be found.")
			return err
		}

		// Merge JSON into Schema objects
		err = json.Unmarshal([]byte(fileContents), &cfv.Schema)
		messages.InfoMessage(cfk + ": File is loaded.")

		// Return error when error is given reading file.
		if err != nil {
			messages.ErrorMessage(cfk + ": Can not parse schema.")
			return err
		}
	}

	return nil
}
