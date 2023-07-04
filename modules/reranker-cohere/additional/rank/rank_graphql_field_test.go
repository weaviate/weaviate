//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
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
	reRankerProvider := &ReRankerCohereProvider{}
	classname := "Class"

	// when
	reRanker := reRankerProvider.additionalReRankerCohereField(classname)

	assert.NotNil(t, reRanker)
	assert.Equal(t, "ClassAdditionalReranker", reRanker.Type.Name())
	assert.NotNil(t, reRanker.Type)
	reRankerObjectList, reRankerObjectListOK := reRanker.Type.(*graphql.List)
	assert.True(t, reRankerObjectListOK)
	reRankerObject, reRankerObjectOK := reRankerObjectList.OfType.(*graphql.Object)
	assert.True(t, reRankerObjectOK)
	assert.Equal(t, 1, len(reRankerObject.Fields()))
	assert.NotNil(t, reRankerObject.Fields()["score"])

	assert.NotNil(t, reRanker.Args)
	assert.Equal(t, 2, len(reRanker.Args))
	assert.NotNil(t, reRanker.Args["query"])
	assert.NotNil(t, reRanker.Args["property"])
}
