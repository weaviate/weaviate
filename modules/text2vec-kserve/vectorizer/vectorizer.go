package vectorizer

import c "github.com/weaviate/weaviate/modules/text2vec-kserve/clients"

type Vectorizer struct {
	client c.Client
}

func New(client c.Client) *Vectorizer {
	return &Vectorizer{
		client: client,
	}
}
