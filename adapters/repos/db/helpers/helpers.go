package helpers

import "fmt"

var (
	ObjectsBucket []byte = []byte("objects")
	IndexIDBucket []byte = []byte("index_ids")
)

func BucketFromPropName(propName string) []byte {
	return []byte(fmt.Sprintf("property_%s", propName))
}
