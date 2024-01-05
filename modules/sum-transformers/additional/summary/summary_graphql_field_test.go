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

package summary

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/tailor-inc/graphql"
)

func Test_additionalSummaryField(t *testing.T) {
	// given
	summaryProvider := &SummaryProvider{}
	classname := "Class"

	// when
	summary := summaryProvider.additionalSummaryField(classname)

	assert.NotNil(t, summary)
	assert.Equal(t, "ClassAdditionalSummary", summary.Type.Name())
	assert.NotNil(t, summary.Type)
	summaryObjectList, summaryObjectListOK := summary.Type.(*graphql.List)
	assert.True(t, summaryObjectListOK)
	summaryObject, summaryObjectOK := summaryObjectList.OfType.(*graphql.Object)
	assert.True(t, summaryObjectOK)
	assert.Equal(t, 2, len(summaryObject.Fields()))
	assert.NotNil(t, summaryObject.Fields()["property"])
	assert.NotNil(t, summaryObject.Fields()["result"])

	assert.NotNil(t, summary.Args)
	assert.Equal(t, 1, len(summary.Args))
	assert.NotNil(t, summary.Args["properties"])
}
