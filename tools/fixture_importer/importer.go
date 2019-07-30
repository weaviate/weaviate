//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
// 
//  Copyright © 2016 - 2019 Weaviate. All rights reserved.
//  LICENSE WEAVIATE OPEN SOURCE: https://www.semi.technology/playbook/playbook/contract-weaviate-OSS.html
//  LICENSE WEAVIATE ENTERPRISE: https://www.semi.technology/playbook/contract-weaviate-enterprise.html
//  CONCEPT: Bob van Luijt (@bobvanluijt)
//  CONTACT: hello@semi.technology
//

package main

// Import fixture data

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"net/url"
	"strings"

	"github.com/go-openapi/runtime"
	httptransport "github.com/go-openapi/runtime/client"
	"github.com/go-openapi/strfmt"
	apiclient "github.com/semi-technologies/weaviate/client"
	"github.com/semi-technologies/weaviate/entities/models"
)

var APITOKEN string
var APIKEY string

// Schema of a weaviate database.
type Schema struct {
	Actions *models.Schema
	Things  *models.Schema
}

var schema Schema
var weaviateUrl url.URL

type DemoDataset struct {
	Things  []DemoInstance `json:"Things"`
	Actions []DemoInstance `json:"Actions"`
}

type DemoInstance map[string]interface{}

var client *apiclient.WeaviateDecentralisedKnowledgeGraph
var auth runtime.ClientAuthInfoWriterFunc

var demoDataset DemoDataset

type fixupAddRef struct {
	kind         string
	fromId       string
	fromProperty string
	toClass      string
	toId         string
	location     string
}

var idMap map[string]string
var thingFixups []fixupAddRef
var thingManyFixups [][]fixupAddRef // for cardinality many where a single thing can require several fixups
var actionFixups []fixupAddRef

var classKinds map[string]string

var weaviateUrlString string
var fixtureFile string

func main() {
	flag.StringVar(&weaviateUrlString, "weaviate-url", "http://localhost:8080/weaviate/v1/", "The address where weaviate can be reached")
	flag.StringVar(&fixtureFile, "fixture-file", "tools/dev/schema/demo_data.json", "The fixtures to import")
	flag.StringVar(&APIKEY, "api-key", "657a48b9-e000-4d9a-b51d-69a0b621c1b9", "The key used to connect to weaviate")
	flag.StringVar(&APITOKEN, "api-token", "57ac8392-1ecc-4e17-9350-c9c866ac832b", "The token used to authenticate the key")
	flag.Parse()

	initSchema()
	initClient()
	initDemoData()

	idMap = map[string]string{}
	thingFixups = []fixupAddRef{}
	actionFixups = []fixupAddRef{}
	classKinds = map[string]string{}

	createThings()
	createActions()

	fixupThings()
	fixupActions()
}

func findClass(schema *models.Schema, className string) *models.Class {
	for _, klass := range schema.Classes {
		if klass.Class == className {
			return klass
		}
	}

	panic(fmt.Sprintf("Class %s not found", className))
}

func findProperty(class *models.Class, propName string) *models.Property {
	for _, prop := range class.Properties {
		if prop.Name == propName {
			return prop
		}
	}

	panic(fmt.Sprintf("property %s in class %s not found", propName, class.Class))
}

func joinErrorMessages(errResponse *models.ErrorResponse) string {
	msgs := make([]string, len(errResponse.Error), len(errResponse.Error))
	for i, err := range errResponse.Error {
		msgs[i] = err.Message
	}

	return strings.Join(msgs, ", ")
}

func loadSemantisSchema(path string) *models.Schema {
	var schema models.Schema

	dat, err := ioutil.ReadFile(path)
	if err != nil {
		panic("Could not read things sche")
	}

	err = json.Unmarshal(dat, &schema)
	if err != nil {
		panic("Could not load demo dataset")
	}

	return &schema
}

func initSchema() {
	schema.Things = loadSemantisSchema("tools/dev/schema/things_schema.json")
	schema.Actions = loadSemantisSchema("tools/dev/schema/actions_schema.json")
}

func initClient() {

	weaviateUrl, err := url.Parse(weaviateUrlString)

	if err != nil {
		panic("URL provided is illegal")
	}

	transport := httptransport.New(weaviateUrl.Host, weaviateUrl.RequestURI(), []string{weaviateUrl.Scheme})
	client = apiclient.New(transport, strfmt.Default)

	auth = func(r runtime.ClientRequest, _ strfmt.Registry) error {
		err := r.SetHeaderParam("X-API-KEY", APIKEY)
		if err != nil {
			return err
		}

		return r.SetHeaderParam("X-API-TOKEN", APITOKEN)
	}
}

func initDemoData() {
	dat, err := ioutil.ReadFile(fixtureFile)
	if err != nil {
		panic("Could not read demo dataset")
	}

	err = json.Unmarshal(dat, &demoDataset)
	if err != nil {
		panic("Could not load demo dataset")
	}
}
