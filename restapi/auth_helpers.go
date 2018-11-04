package restapi

import (
	"github.com/creativesoftwarefdn/weaviate/models"
)

func isRoot(principal interface{}) bool {
	key := (principal.(*models.KeyTokenGetResponse))
	return *key.IsRoot
}
