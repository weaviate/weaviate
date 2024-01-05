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

package spellcheck

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/tailor-inc/graphql"
)

func TestSpellCheckField(t *testing.T) {
	t.Run("should generate spellCheck argument properly", func(t *testing.T) {
		// given
		spellCheckProvider := &SpellCheckProvider{}
		classname := "Class"

		// when
		spellCheck := spellCheckProvider.additionalSpellCheckField(classname)

		// then
		// the built graphQL field needs to support this structure:
		// Type: {
		//   spellCheck: [{
		//     "originalText": "What did the monkey do?",
		//     "didYouMean": "What did the monkey do?"",
		//     "location": "nearText.concepts[0]",
		//     "numberOfCorrections": 1,
		//     "changes": [{
		//         "original": "misspelling",
		//         "didYouMean": "correction"
		//     }]
		//   }]
		// }
		assert.NotNil(t, spellCheck)
		assert.Equal(t, "ClassAdditionalSpellCheck", spellCheck.Type.Name())
		assert.NotNil(t, spellCheck.Type)
		spellCheckObjectList, spellCheckObjectListOK := spellCheck.Type.(*graphql.List)
		assert.True(t, spellCheckObjectListOK)
		spellCheckObject, spellCheckObjectOK := spellCheckObjectList.OfType.(*graphql.Object)
		assert.True(t, spellCheckObjectOK)
		assert.Equal(t, 5, len(spellCheckObject.Fields()))
		assert.NotNil(t, spellCheckObject.Fields()["originalText"])
		assert.NotNil(t, spellCheckObject.Fields()["didYouMean"])
		assert.NotNil(t, spellCheckObject.Fields()["location"])
		assert.NotNil(t, spellCheckObject.Fields()["numberOfCorrections"])
		assert.NotNil(t, spellCheckObject.Fields()["changes"])
		changes := spellCheckObject.Fields()["changes"]
		spellCheckChangesObjectList, spellCheckChangesObjectListOK := changes.Type.(*graphql.List)
		assert.True(t, spellCheckChangesObjectListOK)
		spellCheckChangesObject, spellCheckChangesObjectOK := spellCheckChangesObjectList.OfType.(*graphql.Object)
		assert.True(t, spellCheckChangesObjectOK)
		assert.Equal(t, 2, len(spellCheckChangesObject.Fields()))
		assert.NotNil(t, spellCheckChangesObject.Fields()["original"])
		assert.NotNil(t, spellCheckChangesObject.Fields()["corrected"])
	})
}
