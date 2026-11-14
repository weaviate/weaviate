package tokens

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/modules/ner-transformers/ent"
)

type countingNERClient struct {
	calls int
}

func (f *countingNERClient) GetTokens(ctx context.Context, property, text string) ([]ent.TokenResult, error) {
	f.calls++

	return []ent.TokenResult{{
		Certainty: 1,
		Entity:    "MISC",
		Property:  property,
		Word:      text,
	}}, nil
}

func TestFindTokensDoesNotCallNERAfterLimitIsReached(t *testing.T) {
	ner := &countingNERClient{}
	provider := New(ner)

	in := []search.Result{{
		ClassName: "Article",
		Schema: map[string]interface{}{
			"title": "first text",
			"body":  "second text",
		},
	}}

	out, err := provider.findTokens(context.Background(), in, &Params{
		Properties: []string{"title", "body"},
		Limit:      ptInt(1),
	})

	require.NoError(t, err)
	require.Len(t, out, 1)
	require.Equal(t, 1, ner.calls)

	tokens, ok := out[0].AdditionalProperties["tokens"].([]ent.TokenResult)
	require.True(t, ok)
	require.Len(t, tokens, 1)
}
