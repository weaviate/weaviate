package janusgraph

import (
	"strings"

	"github.com/creativesoftwarefdn/weaviate/gremlin"
	"github.com/creativesoftwarefdn/weaviate/models"

	"github.com/go-openapi/strfmt"
)

func fillKeyResponseFromVertex(vertex *gremlin.Vertex, keyResponse *models.KeyGetResponse) {
	keyResponse.KeyID = strfmt.UUID(vertex.AssertPropertyValue(PROP_UUID).AssertString())
	keyResponse.KeyExpiresUnix = vertex.AssertPropertyValue(PROP_KEY_EXPIRES_UNIX).AssertInt64()
	keyResponse.Write = vertex.AssertPropertyValue(PROP_KEY_WRITE).AssertBool()
	keyResponse.Email = vertex.AssertPropertyValue(PROP_KEY_EMAIL).AssertString()
	keyResponse.Read = vertex.AssertPropertyValue(PROP_KEY_READ).AssertBool()
	keyResponse.Delete = vertex.AssertPropertyValue(PROP_KEY_DELETE).AssertBool()
	keyResponse.Execute = vertex.AssertPropertyValue(PROP_KEY_EXECUTE).AssertBool()
	keyResponse.IPOrigin = strings.Split(vertex.AssertPropertyValue(PROP_KEY_IP_ORIGIN).AssertString(), ";")

	isRoot := vertex.AssertPropertyValue(PROP_KEY_IS_ROOT).AssertBool()
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
