package janusgraph

import (
	graphql_local_get "github.com/creativesoftwarefdn/weaviate/graphqlapi/local/get"
  "fmt"
)

func (j *Janusgraph) LocalGetClass(info *graphql_local_get.LocalGetClassParams) (func() interface{}, error) {
  fmt.Printf("JANUSGRAPH LOCAL GET CLASS REVOLERRT!\n")
  return func() interface{} { return nil }, nil
}
