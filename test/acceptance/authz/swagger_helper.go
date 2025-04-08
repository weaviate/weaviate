//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package authz

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand/v2"
	"os"
	"sort"
	"strings"
	"text/tabwriter"

	"github.com/go-openapi/loads"
	"github.com/go-openapi/spec"
	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"

	"github.com/weaviate/weaviate/entities/models"
	eschema "github.com/weaviate/weaviate/entities/schema"
)

type endpoint struct {
	path                   string
	method                 string
	summery                string
	validGeneratedBodyData []byte
}

type collector struct {
	endpoints       []endpoint
	methodEndpoints map[string][]endpoint
}

func newCollector() (*collector, error) {
	c := &collector{
		methodEndpoints: make(map[string][]endpoint),
		endpoints:       make([]endpoint, 0),
	}
	endpoints, err := c.collectEndpoints()
	if err != nil {
		return nil, err
	}
	c.endpoints = endpoints
	return c, nil
}

func (c *collector) collectEndpoints() ([]endpoint, error) {
	document, err := loads.Spec("../../../openapi-specs/schema.json")
	if err != nil {
		return nil, fmt.Errorf("failed to load Swagger spec: %w", err)
	}

	specDoc := document.Spec()
	for path, pathItem := range specDoc.Paths.Paths {
		methods := map[string]*spec.Operation{
			"GET":     pathItem.Get,
			"POST":    pathItem.Post,
			"PUT":     pathItem.Put,
			"DELETE":  pathItem.Delete,
			"PATCH":   pathItem.Patch,
			"HEAD":    pathItem.Head,
			"OPTIONS": pathItem.Options,
		}

		for method, operation := range methods {
			if operation == nil {
				continue
			}

			var requestBodyData []byte
			for _, param := range operation.Parameters {
				if param.In == "body" && param.Schema != nil {
					requestBodyData, err = generateValidRequestBody(&param, specDoc.Definitions)
					if err != nil {
						return nil, fmt.Errorf("failed to generate request body data: %w", err)
					}
				}
			}

			endpoint := endpoint{
				path:                   path,
				method:                 method,
				summery:                operation.Summary,
				validGeneratedBodyData: requestBodyData,
			}

			c.methodEndpoints[method] = append(c.methodEndpoints[method], endpoint)
			c.endpoints = append(c.endpoints, endpoint)
		}
	}

	sort.Slice(c.endpoints, func(i, j int) bool {
		if c.endpoints[i].path == c.endpoints[j].path {
			return c.endpoints[i].method < c.endpoints[j].method
		}
		return c.endpoints[i].path < c.endpoints[j].path
	})

	return c.endpoints, nil
}

func (c *collector) prettyPrint(endpoints ...map[string][]endpoint) {
	if len(endpoints) == 0 {
		print(c.methodEndpoints)
		return
	}

	for _, endpointsMap := range endpoints {
		print(endpointsMap)
	}
}

func print(endpointsByMethod map[string][]endpoint) {
	count := 0
	writer := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', tabwriter.Debug)
	for method, endpoints := range endpointsByMethod {
		fmt.Fprintf(writer, "\n%s Requests:\n", strings.ToUpper(method))
		fmt.Fprintln(writer, "Path\tMethod\tSummary")
		for _, endpoint := range endpoints {
			count++
			fmt.Fprintf(writer, "%s\t%s\t%s\n", endpoint.path, endpoint.method, endpoint.summery)
		}
	}
	fmt.Fprintf(writer, "endpoints count: %d\n", count)
	writer.Flush()
}

func (c *collector) allEndpoints() []endpoint {
	return c.endpoints
}

func generateValidRequestBody(param *spec.Parameter, definitions map[string]spec.Schema) ([]byte, error) {
	if param.In == "body" && param.Schema != nil {
		return generateValidData(param.Schema, definitions)
	}
	return nil, fmt.Errorf("invalid parameter schema")
}

func generateValidData(schema *spec.Schema, definitions map[string]spec.Schema) ([]byte, error) {
	// needs to be at the top, because it contains a SingleRef
	if strings.Contains(schema.Ref.String(), "MultipleRef") {
		ref := &models.MultipleRef{
			&models.SingleRef{Beacon: strfmt.URI(fmt.Sprintf("weaviate://localhost/ABC/%s", uuid.New().String()))},
		}
		jsonData, err := json.Marshal(ref)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal mock data: %w", err)
		}
		return jsonData, nil
	}

	if schema.Ref.String() != "" {
		ref := schema.Ref.String()

		refSchema, err := resolveReference(ref, definitions)
		if err != nil {
			log.Printf("Failed to resolve reference: %v", err)
			return nil, err
		}

		return generateValidData(refSchema, definitions)
	}

	if len(schema.Type) == 0 {
		return []byte("{}"), nil
	}

	var mockData interface{}
	switch schema.Type[0] {
	case "string":
		if len(schema.Enum) > 0 {
			mockData = schema.Enum[rand.IntN(len(schema.Enum))]
		} else if schema.Format == "uuid" {
			mockData = uuid.New().String()
		} else if schema.Format == "date-time" {
			mockData = "2017-07-21T17:32:28Z"
		} else {
			mockData = "ABC"
		}
	case "integer":
		mockData = rand.IntN(100)
	case "boolean":
		mockData = rand.IntN(2) == 0
	case "array":
		var array []interface{}
		if schema.Items != nil && schema.Items.Schema != nil {
			itemSchema := schema.Items.Schema
			if strings.Contains(itemSchema.Ref.String(), "WhereFilter") {
				all := "*"
				whereFilter := &models.WhereFilter{
					Path:      []string{"id"},
					Operator:  "Like",
					ValueText: &all,
				}
				array = append(array, whereFilter)
				mockData = array
				jsonData, err := json.Marshal(mockData)
				if err != nil {
					return nil, fmt.Errorf("failed to marshal mock data: %w", err)
				}
				return jsonData, nil
			}

			if strings.Contains(itemSchema.Ref.String(), "NestedProperty") {
				vTrue := true
				vFalse := false
				nested := &models.NestedProperty{
					Name:            "nested_int",
					DataType:        eschema.DataTypeInt.PropString(),
					IndexFilterable: &vTrue,
					IndexSearchable: &vFalse,
					Tokenization:    "",
				}

				array = append(array, nested)
				mockData = array
				jsonData, err := json.Marshal(mockData)
				if err != nil {
					return nil, fmt.Errorf("failed to marshal mock data: %w", err)
				}
				return jsonData, nil
			}

			if strings.Contains(itemSchema.Ref.String(), "BatchReference") {
				batch := &models.BatchReference{
					From: strfmt.URI(fmt.Sprintf("weaviate://localhost/ABC/%s/ref", uuid.New().String())),
					To:   strfmt.URI(fmt.Sprintf("weaviate://localhost/ABC/%s", uuid.New().String())),
				}
				array = append(array, batch)
				mockData = array
				jsonData, err := json.Marshal(mockData)
				if err != nil {
					return nil, fmt.Errorf("failed to marshal mock data: %w", err)
				}
				return jsonData, nil
			}

			if itemSchema.Ref.String() != "" {
				refSchema, err := resolveReference(itemSchema.Ref.String(), definitions)
				if err != nil {
					log.Printf("Failed to resolve array item reference: %v", err)
					return nil, err
				}
				itemSchema = refSchema
				data, err := generateValidData(itemSchema, definitions)
				if err != nil {
					return nil, err
				}
				var dd interface{}
				err = json.Unmarshal(data, &dd)
				if err != nil {
					return nil, err
				}
				array = append(array, dd)
			}
			if itemSchema.Type[0] == "string" {
				data, err := generateValidData(itemSchema, definitions)
				if err != nil {
					return nil, err
				}
				var dd interface{}
				err = json.Unmarshal(data, &dd)
				if err != nil {
					return nil, err
				}
				array = append(array, dd)
			}
		}
		mockData = array
	case "object":
		obj := make(map[string]interface{})
		for propName, propSchema := range schema.Properties {
			data, err := generateValidData(&propSchema, definitions)
			if err != nil {
				return nil, err
			}
			var dd interface{}
			err = json.Unmarshal(data, &dd)
			if err != nil {
				return nil, err
			}
			obj[propName] = dd
		}
		mockData = obj
	}

	jsonData, err := json.Marshal(mockData)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal mock data: %w", err)
	}
	return jsonData, nil
}

// resolveReference resolves a reference to a schema definition in the Swagger file
func resolveReference(ref string, definitions map[string]spec.Schema) (*spec.Schema, error) {
	ref = strings.TrimPrefix(ref, "#/definitions/")
	if schema, ok := definitions[ref]; ok {
		return &schema, nil
	}
	return nil, fmt.Errorf("reference %s not found", ref)
}
