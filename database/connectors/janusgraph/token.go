package janusgraph

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"

	"github.com/go-openapi/strfmt"

	"github.com/creativesoftwarefdn/weaviate/gremlin"
	"github.com/creativesoftwarefdn/weaviate/models"

	connutils "github.com/creativesoftwarefdn/weaviate/database/connectors/utils"
)

// ValidateToken validates/gets a key to the Grelmin database with the given token (=UUID)
func (f *Janusgraph) ValidateToken(ctx context.Context, UUID strfmt.UUID, keyResponse *models.KeyGetResponse) (token string, err error) {
	q := gremlin.G.V().HasLabel(KEY_VERTEX_LABEL).HasString("uuid", string(UUID))

	result, err := f.client.Execute(q)

	if err != nil {
		return "", err
	}

	vertices, err := result.Vertices()

	// We got something that are not vertices.
	if err != nil {
		return "", err
	}

	// No key is found
	if len(vertices) == 0 {
		return "", errors.New(connutils.StaticKeyNotFound)
	}

	if len(vertices) != 1 {
		return "", fmt.Errorf("More than one key with UUID '%v' found!", UUID)
	}

	vertex := vertices[0]
	fillKeyResponseFromVertex(&vertex, keyResponse)
	tokenToReturn, err := base64.StdEncoding.DecodeString(vertex.AssertPropertyValue("__token").AssertString())

	if err != nil {
		return "", err
	}

	// If success return nil, otherwise return the error
	return string(tokenToReturn), nil
}
