//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2021 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package ask

import (
	"testing"

	"github.com/graphql-go/graphql"
	"github.com/stretchr/testify/assert"
)

func TestNerGraphQLArgument(t *testing.T) {
	t.Run("should generate ner argument properly", func(t *testing.T) {
		// given
		prefix := "Prefix"
		classname := "Class"
		// when
		ner := nerArgument(prefix, classname)

		// then
		// the built graphQL field needs to support this structure:
		// ner {
		//   limit: 1,
		//   certainty: 0.7
		//   properties: ["prop1", "prop2"]
		// }
		assert.NotNil(t, ner)
		assert.Equal(t, "NERTransformersPrefixClassAskInpObj", ner.Type.Name())
		nerFields, ok := ner.Type.(*graphql.InputObject)
		assert.True(t, ok)
		assert.NotNil(t, nerFields)
		assert.Equal(t, 3, len(nerFields.Fields()))
		fields := nerFields.Fields()
		assert.NotNil(t, fields["certainty"])
		assert.NotNil(t, fields["limit"])
		properties := fields["properties"]
		propertiesList, propertiesListOK := properties.Type.(*graphql.List)
		assert.True(t, propertiesListOK)
		assert.Equal(t, "String", propertiesList.OfType.Name())
	})
}
