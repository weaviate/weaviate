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

	"os"
	"time"

	"github.com/launchdarkly/go-sdk-common/v3/ldcontext"
	"github.com/sirupsen/logrus"

	// go-sdk-common/v3/ldmigration defines LaunchDarkly's model for migration feature flags
	_ "github.com/launchdarkly/go-sdk-common/v3/ldmigration"

	// go-server-sdk/v7 is the main SDK package - here we are aliasing it to "ld"
	ld "github.com/launchdarkly/go-server-sdk/v7"

	// go-server-sdk/v7/ldcomponents is for advanced configuration options
	_ "github.com/launchdarkly/go-server-sdk/v7/ldcomponents"
)

// ldClient is not nil if the LD integration has been successfully configured. It's a global variable to ease the usage of feature flags and
// ensure they can be instantiated anywhere in the code.
var ldClient *ld.LDClient

// ldContext is the current context configured for this particular process. It is a global variable as it needs to be used when registering
// a flag in the LD SDK
var ldContext ldcontext.Context

// ConfigureLDIntegration will configure the necessary global variables to have `FeatureFlag` struct be able to use LD flags
func ConfigureLDIntegration(logger logrus.FieldLogger) {
	var err error
	logger = logger.WithField("action", "ld_integration")

	// Fetch all the necessary env variable and exit if one fails
	ldApiKey, ok := os.LookupEnv("WEAVIATE_LD_API_KEY")
	if !ok {
		logger.Info("LD integration disabled as WEVIATE_LD_API_KEY is not present")
		return
	}
	clusterKey, ok := os.LookupEnv("WEAVIATE_LD_CLUSTER_KEY")
	if !ok {
		logger.Info("LD integration disabled as WEVIATE_LD_CLUSTER_KEY is not present")
		return
	}
	orgKey, ok := os.LookupEnv("WEAVIATE_LD_ORG_KEY")
	if !ok {
		logger.Info("LD integration disabled as WEVIATE_LD_ORG_KEY is not present")
		return
	}
	nodeKey, ok := os.LookupEnv("CLUSTER_HOSTNAME")
	if !ok || nodeKey == "" {
		nodeKey, err = os.Hostname()
		if err != nil {
			logger.Infof("LD integration disabled as CLUSTER_HOSTNAME is not present and os hostname returned error: %w", err)
			return
		}
	}

	ldClient, err = ld.MakeClient(ldApiKey, 5*time.Second)
	if err != nil {
		ldClient = nil
		logger.Infof("LD integration disabled as LD SDK init returned: %w", err)
		return
	}
	// Can happen according to docs
	if ldClient == nil {
		logger.Info("LD integration disabled as LD SDK init returned success but client is nil")
		return
	}

	clusterContext := ldcontext.NewBuilder(clusterKey).Kind("cluster").Build()
	orgContext := ldcontext.NewBuilder(orgKey).Kind("org").Build()
	nodeContext := ldcontext.NewBuilder(nodeKey).Kind("node").Build()
	ldContext, err = ldcontext.NewMultiBuilder().Add(clusterContext).Add(orgContext).Add(nodeContext).TryBuild()
	if err != nil {
		// Set client to nil to "disable" the LD integration
		ldClient = nil
		logger.Info("LD integration disabled as LD context init returned: %w", err)
		return
	}
	logger.WithFields(logrus.Fields{
		"context_cluster": clusterKey,
		"context_org":     orgKey,
		"context_node":    nodeKey,
	}).Infof("Configured LD integration")
}
