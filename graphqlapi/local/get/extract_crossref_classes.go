package local_get

import (
	"github.com/creativesoftwarefdn/weaviate/database/schema"
	"github.com/creativesoftwarefdn/weaviate/models"
	"github.com/creativesoftwarefdn/weaviate/network/crossrefs"
)

func extractNetworkRefClassNames(schema schema.Schema) []string {
	var result = []string{}

	if schema.Actions != nil {
		result = append(result, extractFromClasses(schema.Actions.Classes)...)
	}

	if schema.Things != nil {
		result = append(result, extractFromClasses(schema.Things.Classes)...)
	}

	return removeDuplicates(result)
}

func extractFromClasses(classes []*models.SemanticSchemaClass) []string {
	var result = []string{}
	for _, class := range classes {
		result = append(result, extractFromProperties(class.Properties)...)
	}

	return result
}

func extractFromProperties(props []*models.SemanticSchemaClassProperty) []string {
	var result = []string{}
	for _, prop := range props {
		result = append(result, extractFromDataTypes(prop.AtDataType)...)
	}

	return result
}

func extractFromDataTypes(types []string) []string {
	var result = []string{}
	for _, t := range types {
		if _, err := crossrefs.ParseClass(t); err == nil {
			result = append(result, t)
		}
	}

	return result
}

func removeDuplicates(a []string) []string {
	result := []string{}
	seen := map[string]string{}
	for _, val := range a {
		if _, ok := seen[val]; !ok {
			result = append(result, val)
			seen[val] = val
		}
	}
	return result
}
