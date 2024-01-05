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

package classification

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTfidf(t *testing.T) {
	docs := []string{
		"this pinot wine is a pinot noir",
		"this one is a cabernet sauvignon",
		"this wine is a cabernet franc",
		"this one is a merlot",
	}

	calc := NewTfIdfCalculator(len(docs))
	for _, doc := range docs {
		calc.AddDoc(doc)
	}
	calc.Calculate()

	t.Run("doc 0", func(t *testing.T) {
		doc := 0

		// filler words should have score of 0
		assert.Equal(t, float32(0), calc.Get("this", doc))
		assert.Equal(t, float32(0), calc.Get("is", doc))
		assert.Equal(t, float32(0), calc.Get("a", doc))

		// next highest should be wine, noir, pinot
		wine := calc.Get("wine", doc)
		noir := calc.Get("noir", doc)
		pinot := calc.Get("pinot", doc)

		assert.True(t, wine > 0, "wine greater 0")
		assert.True(t, noir > wine, "noir greater than wine")
		assert.True(t, pinot > noir, "pinot has highest score")
	})

	t.Run("doc 1", func(t *testing.T) {
		doc := 1

		// filler words should have score of 0
		assert.Equal(t, float32(0), calc.Get("this", doc))
		assert.Equal(t, float32(0), calc.Get("is", doc))
		assert.Equal(t, float32(0), calc.Get("a", doc))

		// next highest should be one==cabernet, sauvignon
		one := calc.Get("one", doc)
		cabernet := calc.Get("cabernet", doc)
		sauvignon := calc.Get("sauvignon", doc)

		assert.True(t, one > 0, "one greater 0")
		assert.True(t, cabernet == one, "cabernet equal to one")
		assert.True(t, sauvignon > cabernet, "sauvignon has highest score")
	})

	t.Run("doc 2", func(t *testing.T) {
		doc := 2

		// filler words should have score of 0
		assert.Equal(t, float32(0), calc.Get("this", doc))
		assert.Equal(t, float32(0), calc.Get("is", doc))
		assert.Equal(t, float32(0), calc.Get("a", doc))

		// next highest should be one==cabernet, sauvignon
		wine := calc.Get("wine", doc)
		cabernet := calc.Get("cabernet", doc)
		franc := calc.Get("franc", doc)

		assert.True(t, wine > 0, "wine greater 0")
		assert.True(t, cabernet == wine, "cabernet equal to wine")
		assert.True(t, franc > cabernet, "franc has highest score")
	})

	t.Run("doc 3", func(t *testing.T) {
		doc := 3

		// filler words should have score of 0
		assert.Equal(t, float32(0), calc.Get("this", doc))
		assert.Equal(t, float32(0), calc.Get("is", doc))
		assert.Equal(t, float32(0), calc.Get("a", doc))

		// next highest should be one==cabernet, sauvignon
		one := calc.Get("one", doc)
		merlot := calc.Get("merlot", doc)

		assert.True(t, one > 0, "one greater 0")
		assert.True(t, merlot > one, "merlot has highest score")
	})
}
