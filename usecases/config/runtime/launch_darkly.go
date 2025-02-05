//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package runtime

import (

	// go-sdk-common/v3/ldcontext defines LaunchDarkly's model for contexts

	"fmt"
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

type LDIntegration struct {
	// ldClient is not nil if the LD integration has been successfully configured.
	ldClient *ld.LDClient

	// ldContext is the current context configured for this particular process.
	ldContext ldcontext.Context
}

const (
	WeaviateLDApiKey     = "WEAVIATE_LD_API_KEY"
	WeaviateLDClusterKey = "WEAVIATE_LD_CLUSTER_KEY"
	WeaviateLDOrgKey     = "WEAVIATE_LD_ORG_KEY"

	LDContextOrgKey     = "org"
	LDContextClusterKey = "cluster"
	LDContextNodeKey    = "node"
)

// ConfigureLDIntegration will configure the necessary global variables to have `FeatureFlag` struct be able to use LD flags
func ConfigureLDIntegration() (*LDIntegration, error) {
	var err error

	// Fetch all the necessary env variable and exit if one fails
	ldApiKey, ok := os.LookupEnv(WeaviateLDApiKey)
	if !ok {
		return nil, fmt.Errorf("could not locate %s env variable", WeaviateLDApiKey)
	}
	orgKey, ok := os.LookupEnv(WeaviateLDOrgKey)
	if !ok {
		return nil, fmt.Errorf("could not locate %s env variable", WeaviateLDOrgKey)
	}
	clusterKey, ok := os.LookupEnv(WeaviateLDClusterKey)
	if !ok {
		return nil, fmt.Errorf("could not locate %s env variable", WeaviateLDClusterKey)
	}
	// Re-using the current approach to parse the nodeName in the config
	nodeKey, ok := os.LookupEnv("CLUSTER_HOSTNAME")
	if !ok || nodeKey == "" {
		nodeKey, err = os.Hostname()
		if err != nil {
			return nil, fmt.Errorf("could not locate CLUSTER_HOSTNAME env variable")
		}
	}

	// Instantiate the LD client
	ldClient, err := ld.MakeClient(ldApiKey, 5*time.Second)
	if err != nil {
		return nil, fmt.Errorf("could not instantiate LD Client: %w", err)
	}
	// Can happen according to LD SDK docs
	if ldClient == nil {
		return nil, fmt.Errorf("LD client instantiation successful but client is nil")
	}

	// Instantiate the LD context
	orgContext := ldcontext.NewBuilder(orgKey).Kind(LDContextOrgKey).Build()
	clusterContext := ldcontext.NewBuilder(clusterKey).Kind(LDContextClusterKey).Build()
	nodeContext := ldcontext.NewBuilder(nodeKey).Kind(LDContextNodeKey).Build()
	ldContext, err := ldcontext.NewMultiBuilder().Add(clusterContext).Add(orgContext).Add(nodeContext).TryBuild()
	if err != nil {
		return nil, fmt.Errorf("could not instantiate LD context: %w", err)
	}

	// Success
	return &LDIntegration{
		ldClient:  ldClient,
		ldContext: ldContext,
	}, nil
}
