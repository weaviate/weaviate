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

package cluster

import (
	"github.com/weaviate/weaviate/usecases/auth/authorization/conv"
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

// subjectNamespace returns the namespace of a grouping subject (e.g.
// "db:customer1:bob" -> "customer1"), or "" for a global, group, or
// unparseable subject.
func subjectNamespace(subject string) string {
	_, authType, namespace, err := conv.SubjectNamespace(subject)
	if err != nil || authType == "" {
		return ""
	}
	return namespace
}
