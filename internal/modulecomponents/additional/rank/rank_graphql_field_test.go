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

package rank

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/tailor-inc/graphql"
)

func Test_additionalCrossRankerField(t *testing.T) {
	// given
	crossRankerProvider := &ReRankerProvider{}
	classname := "Class"

	// when
	crossRanker := crossRankerProvider.additionalReRankerField(classname)

	assert.NotNil(t, crossRanker)
	assert.Equal(t, "ClassAdditionalReranker", crossRanker.Type.Name())
	assert.NotNil(t, crossRanker.Type)
	crossRankerObjectList, crossRankerObjectListOK := crossRanker.Type.(*graphql.List)
	assert.True(t, crossRankerObjectListOK)
	crossRankerObject, crossRankerObjectOK := crossRankerObjectList.OfType.(*graphql.Object)
	assert.True(t, crossRankerObjectOK)
	assert.Equal(t, 1, len(crossRankerObject.Fields()))
	assert.NotNil(t, crossRankerObject.Fields()["score"])

	assert.NotNil(t, crossRanker.Args)
	assert.Equal(t, 2, len(crossRanker.Args))
	assert.NotNil(t, crossRanker.Args["query"])
	assert.NotNil(t, crossRanker.Args["property"])
}
