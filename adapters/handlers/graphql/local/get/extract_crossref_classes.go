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

package get

import (
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/usecases/network/crossrefs"
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

func extractFromClasses(classes []*models.Class) []crossrefs.NetworkClass {
	var result = []crossrefs.NetworkClass{}
	for _, class := range classes {
		result = append(result, extractFromProperties(class.Properties)...)
	}

	return result
}

func extractFromProperties(props []*models.Property) []crossrefs.NetworkClass {
	var result = []crossrefs.NetworkClass{}
	for _, prop := range props {
		result = append(result, extractFromDataTypes(prop.DataType)...)
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
