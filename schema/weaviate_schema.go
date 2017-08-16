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

// Schema is a representation in GO for the custom schema provided.
type Schema struct {
	Context    string  `json:"context"`
	Type       string  `json:"type"`
	Maintainer string  `json:"maintainer"`
	Name       string  `json:"name"`
	Classes    []Class `json:"classes"`
}

// Class is a representation of a class within the schema.
type Class struct {
	Class       string     `json:"class"`
	Description string     `json:"description"`
	Properties  []Property `json:"properties"`
}

// Property provides the structure for the properties of the class items.
type Property struct {
	Name        string   `json:"name"`
	Description string   `json:"description"`
	DataType    []string `json:"@dataType"`
}
