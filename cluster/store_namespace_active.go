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
//

package cluster

import (
	"strings"

	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/namespaces"
)

// requireNamespaceActive returns nil for an empty namespace,
// [namespaces.ErrNamespaceGone] when the namespace does not exist, or
// [namespaces.ErrNamespaceDeleting] when it exists but is not active.
func requireNamespaceActive(exister namespaces.Exister, namespace string) error {
	if namespace == "" {
		return nil
	}
	if !exister.Exists(namespace) {
		return namespaces.ErrNamespaceGone
	}
	if exister.IsActive(namespace) {
		return nil
	}
	return namespaces.ErrNamespaceDeleting
}

// namespaceFromQualified returns the namespace portion of a qualified name
// ("<ns>:<entity>"). Names without the separator are unnamespaced and
// return "".
func namespaceFromQualified(name string) string {
	if ns, _, ok := strings.Cut(name, schema.NamespaceSeparator); ok {
		return ns
	}
	return ""
}
