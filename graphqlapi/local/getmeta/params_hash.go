package getmeta

import (
	"crypto/md5"
	"encoding/json"
	"fmt"

	cf "github.com/creativesoftwarefdn/weaviate/graphqlapi/local/common_filters"
)

// AnalyticsHash is a special hash for use with an external analytics engine
// which has caching capabilities. Anything that would produce a different
// result, such as new or different properties or different analytics props
// will create a different hash. Chaning anayltics-meta information, such as
// 'forceRecalculate' however, will not change the hash. Doing so would prevent
// us from ever retrieving a cached result that wass generated with the
// 'forceRecalculate' option on.
func (p Params) AnalyticsHash() (string, error) {

	// make sure to copy the params, so that we don't accidentaly mutate the
	// original
	params := p
	// always override analytical props to make sure they don't influence the
	// hash
	params.Analytics = cf.AnalyticsProps{}

	return params.md5()
}

func (p Params) md5() (string, error) {
	paramBytes, err := json.Marshal(p)
	if err != nil {
		return "", fmt.Errorf("couldnt convert params to json before hashing: %s", err)
	}

	hash := md5.New()
	fmt.Fprintf(hash, "%s", paramBytes)
	return fmt.Sprintf("%x", hash.Sum(nil)), nil
}
