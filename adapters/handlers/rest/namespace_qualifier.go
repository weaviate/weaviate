//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package rest

import (
	"github.com/sirupsen/logrus"

	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/license"
	"github.com/weaviate/weaviate/usecases/schema/namespacing"
	wlnamespaces "github.com/weaviate/weaviate/wl/namespaces"
)

// namespacesFeature names namespaces in the license refusal and the startup
// warning.
const namespacesFeature = "namespaces"

const unlicensedNamespacesDetail = "Every REST, gRPC and MCP data request that names a collection is refused, " +
	"with 403 over REST, PermissionDenied over gRPC and a failed tool call over MCP. " +
	"REST batch references, gRPC unary batch objects and batch references, and a batch stream message " +
	"holding only references are refused per item. Backup, export, replica movement and the debug port " +
	"are not refused, but restoring a namespaced backup needs its namespaces to exist and be active. " +
	"Every namespace operation (create, update, get, list, delete, suspend and resume) is refused with 403, " +
	"so this node cannot create or resume those namespaces. " +
	"Users and roles can still be written, but grant no data access."

// namespaceModeFor is the only code that pairs NAMESPACES_ENABLED with
// Config.WeaviateLicense.
func namespaceModeFor(cfg config.Config) license.Mode {
	return license.ModeFor(cfg.Namespaces.Enabled, cfg.WeaviateLicense)
}

// namespaceQualifier picks the Qualifier that startupRoutine stores in
// state.State for every class-name resolver call on this node. Any mode but
// FeatureOff and FeatureLicensed gets the license refusal.
func namespaceQualifier(mode license.Mode) namespacing.Qualifier {
	switch mode {
	case license.FeatureOff:
		return namespacing.Disabled
	case license.FeatureLicensed:
		return wlnamespaces.NewPrefixing()
	case license.FeatureUnlicensed:
	}
	return namespacing.Refusing(license.Required(namespacesFeature))
}

func logUnlicensedNamespaces(logger logrus.FieldLogger, mode license.Mode) {
	license.LogUnlicensed(logger, mode, namespacesFeature, unlicensedNamespacesDetail)
}
