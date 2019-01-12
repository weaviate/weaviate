/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2018 Weaviate. All rights reserved.
 * LICENSE: https://github.com/creativesoftwarefdn/weaviate/blob/develop/LICENSE.md
 * AUTHOR: Bob van Luijt (bob@kub.design)
 * See www.creativesoftwarefdn.org for details
 * Contact: @CreativeSofwFdn / bob@kub.design
 */
package janusgraph

import (
	"fmt"

	"github.com/creativesoftwarefdn/weaviate/database/schema"
	"github.com/creativesoftwarefdn/weaviate/database/schema/kind"
	graphql_local_getmeta "github.com/creativesoftwarefdn/weaviate/graphqlapi/local/getmeta"
	"github.com/creativesoftwarefdn/weaviate/gremlin"
)

// LocalGetMeta based on GraphQL Query params
func (j *Janusgraph) LocalGetMeta(params *graphql_local_getmeta.Params) (interface{}, error) {
	// hard-code to city -> population -> average
	err, prop := j.schema.GetProperty(kind.THING_KIND, schema.ClassName("City"), schema.PropertyName("population"))
	if err != nil {
		return nil, fmt.Errorf("could not find property in schema: %s", err)
	}

	dataType, err := j.schema.FindPropertyDataType(prop.AtDataType)
	if err != nil {
		return nil, fmt.Errorf("could not find data type: %s", err)
	}

	if !dataType.IsPrimitive() {
		return nil, fmt.Errorf("GetMeta is not supported with non primitive types in Janusgraph yet")
	}

	if dataType.AsPrimitive() != schema.DataTypeInt {
		return nil, fmt.Errorf("expected an int in the spike")
	}

	q := gremlin.New().Raw(`g.V().has("classId", "class_3").aggregate("prop_18").by("prop_18").cap("prop_18").limit(1).as("average", "sum", "highest", "lowest", "count").select("average", "sum", "highest", "lowest", "count").by(mean(local)).by(sum(local)).by(max(local)).by(min(local)).by(count(local))`)

	result, err := j.client.Execute(q)
	if err != nil {
		return nil, err
	}

	first, err := result.First()
	if err != nil {
		return nil, err
	}

	return map[string]interface{}{
		"population": first.Datum,
	}, nil
}
