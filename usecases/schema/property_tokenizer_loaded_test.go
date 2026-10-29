//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package schema

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/tokenizer"
)

// Test_Validation_GseTokenizerMustBeLoaded pins that a gse tokenization is rejected when
// the tokenizer is enabled but its dictionary did not load at startup. Accepting the
// property used to leave a class whose every BM25 query failed with "only stopwords
// provided", with nothing but a startup log line to explain why.
func Test_Validation_GseTokenizerMustBeLoaded(t *testing.T) {
	cases := []struct {
		tokenization string
		env          string
		loaded       *bool
		wantErr      string
	}{
		{models.PropertyTokenizationGse, "ENABLE_TOKENIZER_GSE", &tokenizer.UseGse, "the GSE tokenizer is enabled but failed to load its dictionary; see the startup log"},
		{models.PropertyTokenizationGseCh, "ENABLE_TOKENIZER_GSE_CH", &tokenizer.UseGseCh, "the Chinese tokenizer is enabled but failed to load its dictionary; see the startup log"},
	}
	for _, tc := range cases {
		t.Run(tc.tokenization, func(t *testing.T) {
			handler, _ := newTestHandler(t, &fakeDB{})
			pdt := newFakePrimitivePDT(schema.DataTypeText)

			t.Setenv(tc.env, "false")
			require.ErrorContains(t, handler.validatePropertyTokenization(tc.tokenization, pdt), "is not enabled")

			t.Setenv(tc.env, "true")
			was := *tc.loaded
			t.Cleanup(func() { *tc.loaded = was })
			*tc.loaded = false
			require.EqualError(t, handler.validatePropertyTokenization(tc.tokenization, pdt), tc.wantErr)

			*tc.loaded = true
			require.NoError(t, handler.validatePropertyTokenization(tc.tokenization, pdt))
		})
	}
}
