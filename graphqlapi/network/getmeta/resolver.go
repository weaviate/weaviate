package network_getmeta

import "github.com/creativesoftwarefdn/weaviate/graphqlapi/network/common"

// Params ties a SubQuery and a single instance
// together
type Params struct {
	SubQuery       common.SubQuery
	TargetInstance string
}
