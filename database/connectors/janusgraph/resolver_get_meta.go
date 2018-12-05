package janusgraph

import (
	graphql_local_get_meta "github.com/creativesoftwarefdn/weaviate/graphqlapi/local/get_meta"
)

func (j *Janusgraph) LocalGetMeta(params *graphql_local_get_meta.LocalGetMetaParams) (func() interface{}, error) {
	return nil, nil
}
