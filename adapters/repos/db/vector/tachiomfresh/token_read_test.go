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

package tachiomfresh

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
)

// TestTokenReads: the token thunk gives fetchToken and loadTokens the
// floats the document thunk gives, and a missing document or a position
// past the document is an error.
func TestTokenReads(t *testing.T) {
	tf := newTestIndex(t, 64, 8, 8, hfreshEFConstruction, withDistanceProvider(distancer.NewDotProductProvider()))
	require.NotNil(t, tf.Index.tokenForID)
	ctx := t.Context()

	// five documents of three tokens; the entries name every token
	var refs []tokenRef
	for doc := uint64(0); doc < 5; doc++ {
		tokens := createTestVectors(4, 3)
		for i := range tokens {
			tokens[i][0] += float32(doc) + 1
		}
		tf.mvStore.storeMultiVector(doc, tokens)
		for pos := range tokens {
			refs = append(refs, tokenRef{DocID: uint32(doc), Pos: uint16(pos)})
		}
	}
	viaToken, err := tf.Index.loadTokens(ctx, refs)
	require.NoError(t, err)
	tokens := make([][]float32, len(refs))
	for i, ref := range refs {
		tokens[i], err = tf.Index.fetchToken(ctx, ref)
		require.NoError(t, err)
	}

	withToken := tf.Index.tokenForID
	tf.Index.tokenForID = nil
	viaDoc, err := tf.Index.loadTokens(ctx, refs)
	require.NoError(t, err)
	require.Len(t, viaToken, len(viaDoc))
	for i := range refs {
		require.InDeltaSlice(t, viaDoc[i], viaToken[i], 1e-6, "entry %d via loadTokens", i)
		token, err := tf.Index.fetchToken(ctx, refs[i])
		require.NoError(t, err)
		require.InDeltaSlice(t, token, tokens[i], 1e-6, "entry %d via fetchToken", i)
	}

	// the errors, through the token thunk
	tf.Index.tokenForID = withToken
	tf.mvStore.deleteMultiVector(2)
	_, err = tf.Index.fetchToken(ctx, tokenRef{DocID: 2, Pos: 0})
	require.Error(t, err, "a deleted document")
	_, err = tf.Index.fetchToken(ctx, tokenRef{DocID: 0, Pos: 3})
	require.ErrorContains(t, err, "but token 3", "a position past the document")
	_, err = tf.Index.loadTokens(ctx, refs)
	require.ErrorContains(t, err, "failed to fetch token")
	_, err = tf.Index.loadTokens(ctx, refs[:3])
	require.NoError(t, err, "the live document's entries")

	// a position past the document, through the document thunk: the
	// index's own bounds check
	tf.Index.tokenForID = nil
	_, err = tf.Index.fetchToken(ctx, tokenRef{DocID: 0, Pos: 3})
	require.ErrorContains(t, err, "a posting references token 3")
}
