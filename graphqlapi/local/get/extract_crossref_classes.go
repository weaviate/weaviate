package local_get

import (
	"github.com/creativesoftwarefdn/weaviate/database/schema"
	"github.com/creativesoftwarefdn/weaviate/models"
	"github.com/creativesoftwarefdn/weaviate/network/crossrefs"
)

func extractNetworkRefClassNames(schema *schema.Schema) []crossrefs.NetworkClass {
	var result = []crossrefs.NetworkClass{}

	if schema.Actions != nil {
		result = append(result, extractFromClasses(schema.Actions.Classes)...)
	}

	if schema.Things != nil {
		result = append(result, extractFromClasses(schema.Things.Classes)...)
	}

	return removeDuplicates(result)
}

func extractFromClasses(classes []*models.SemanticSchemaClass) []crossrefs.NetworkClass {
	var result = []crossrefs.NetworkClass{}
	for _, class := range classes {
		result = append(result, extractFromProperties(class.Properties)...)
	}

	return result
}

func extractFromProperties(props []*models.SemanticSchemaClassProperty) []crossrefs.NetworkClass {
	var result = []crossrefs.NetworkClass{}
	for _, prop := range props {
		result = append(result, extractFromDataTypes(prop.AtDataType)...)
	}

	return result
}

func extractFromDataTypes(types []string) []crossrefs.NetworkClass {
	var result = []crossrefs.NetworkClass{}
	for _, t := range types {
		if class, err := crossrefs.ParseClass(t); err == nil {
			result = append(result, class)
		}
	}

	return result
}

func removeDuplicates(a []crossrefs.NetworkClass) []crossrefs.NetworkClass {
	result := []crossrefs.NetworkClass{}
	seen := map[crossrefs.NetworkClass]crossrefs.NetworkClass{}
	for _, val := range a {
		if _, ok := seen[val]; !ok {
			result = append(result, val)
			seen[val] = val
		}
	}
	return result
}
