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

package namespacing

import "github.com/weaviate/weaviate/entities/models"

// Refusing returns a Qualifier whose every qualify method returns err. It
// reports namespaces enabled, so namespacing.QualifyRefTarget answers a target
// that passes prefix validation with err instead of returning it unchanged.
func Refusing(err error) Qualifier { return refusing{err: err} }

type refusing struct{ err error }

func (refusing) NamespacesEnabled() bool { return true }

func (q refusing) Qualify(*models.Principal, string) (string, error) {
	return "", q.err
}

func (q refusing) QualifyForCreate(*models.Principal, string) (string, error) {
	return "", q.err
}

func (q refusing) QualifyRefTarget(_, _ string) (qualified, short string, err error) {
	return "", "", q.err
}
