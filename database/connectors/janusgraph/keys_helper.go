package janusgraph

import (
	"strings"

	"github.com/creativesoftwarefdn/weaviate/gremlin"
	"github.com/creativesoftwarefdn/weaviate/models"

	"github.com/go-openapi/strfmt"
)

func fillKeyResponseFromVertex(vertex *gremlin.Vertex, keyResponse *models.KeyGetResponse) {
	keyResponse.KeyID = strfmt.UUID(vertex.AssertPropertyValue("uuid").AssertString())
	keyResponse.KeyExpiresUnix = vertex.AssertPropertyValue("keyExpiresUnix").AssertInt64()
	keyResponse.Write = vertex.AssertPropertyValue("write").AssertBool()
	keyResponse.Email = vertex.AssertPropertyValue("email").AssertString()
	keyResponse.Read = vertex.AssertPropertyValue("read").AssertBool()
	keyResponse.Delete = vertex.AssertPropertyValue("delete").AssertBool()
	keyResponse.Execute = vertex.AssertPropertyValue("execute").AssertBool()
	keyResponse.IPOrigin = strings.Split(vertex.AssertPropertyValue("IPOrigin").AssertString(), ";")

	isRoot := vertex.AssertPropertyValue("isRoot").AssertBool()
	keyResponse.IsRoot = &isRoot
}

// Build a reference to a key (used to link actions and things to a key), from a path from the action or thing, to the key.
func newKeySingleRefFromKeyPath(path *gremlin.Path) *models.SingleRef {
	location := path.Segments[0].AssertEdge().AssertPropertyValue("locationUrl").AssertString()
	return &models.SingleRef{
		NrDollarCref: strfmt.UUID(path.Segments[1].AssertVertex().AssertPropertyValue("uuid").AssertString()),
		Type:         "Key",
		LocationURL:  &location,
	}
}
