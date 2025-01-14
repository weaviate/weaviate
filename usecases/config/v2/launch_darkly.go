package v2

import (

	// go-sdk-common/v3/ldcontext defines LaunchDarkly's model for contexts

	"os"
	"time"

	"github.com/launchdarkly/go-sdk-common/v3/ldcontext"

	// go-sdk-common/v3/ldmigration defines LaunchDarkly's model for migration feature flags
	_ "github.com/launchdarkly/go-sdk-common/v3/ldmigration"

	// go-server-sdk/v7 is the main SDK package - here we are aliasing it to "ld"
	ld "github.com/launchdarkly/go-server-sdk/v7"

	// go-server-sdk/v7/ldcomponents is for advanced configuration options
	_ "github.com/launchdarkly/go-server-sdk/v7/ldcomponents"
)

var client *ld.LDClient
var ldContext ldcontext.Context

func init() {
	var err error

	ldApiKey, ok := os.LookupEnv("WEAVIATE_LD_API_KEY")
	if !ok {
		return
	}
	clusterKey, ok := os.LookupEnv("WEAVIATE_LD_CLUSTER_KEY")
	if !ok {
		return
	}
	orgKey, ok := os.LookupEnv("WEAVIATE_LD_ORG_KEY")
	if !ok {
		return
	}

	client, err = ld.MakeClient(ldApiKey, 5*time.Second)
	if err != nil {
		panic(err)
	}
	// Can happen according to docs
	if client == nil {
		panic("LD client no error but nil")
	}

	clusterContext := ldcontext.NewBuilder(clusterKey).Build()
	orgContext := ldcontext.NewBuilder(orgKey).Build()
	ldContext, err = ldcontext.NewMultiBuilder().Add(clusterContext).Add(orgContext).TryBuild()
	if err != nil {
		panic(err)
	}
}
