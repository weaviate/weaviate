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
)

// subjectNamespace returns the namespace of a grouping subject (e.g.
// "db:customer1:bob" -> "customer1"), or "" for a global or group subject. A
// subject that does not parse returns an error: reporting it as
// namespace-less would skip the state gate.
func subjectNamespace(subject string) (string, error) {
	_, authType, namespace, err := conv.SubjectNamespace(subject)
	if err != nil {
		return "", err
	}
	if authType == "" {
		return "", nil
	}
	return namespace, nil
}
