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

package schema

import (
	"testing"

	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/test/helper/sample-schema/books"
)

func TestUpdateClassWithText2VecOpenAI(t *testing.T) {
	t.Parallel()

	t.Run("update description of legacy vectorizer class", func(t *testing.T) {
		cls := books.ClassOpenAIWithOptions()
		helper.CreateClass(t, cls)
		defer helper.DeleteClass(t, cls.Class)

		cls.Description = "updated description"
		helper.UpdateClass(t, cls)
	})

	t.Run("update description of named vectors class", func(t *testing.T) {
		cls := books.ClassNamedOpenAIWithOptions()
		helper.CreateClass(t, cls)
		defer helper.DeleteClass(t, cls.Class)

		cls.Description = "updated description"
		helper.UpdateClass(t, cls)
	})
}
