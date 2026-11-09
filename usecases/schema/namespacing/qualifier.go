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

// Qualifier adds the namespace prefix to a name that Resolve, QualifyClass,
// QualifyForCreate, QualifyRefTarget or QualifyPropertyDataTypes has validated.
// A non-nil error may be an authzerrors.Forbidden. Callers wrap a Forbidden
// with %w or return it unchanged, and never replace it or turn it into a 422.
type Qualifier interface {
	// NamespacesEnabled reports the cluster's namespaces flag. It says nothing
	// about whether the other methods succeed.
	NamespacesEnabled() bool
	Qualify(principal *models.Principal, name string) (string, error)
	QualifyForCreate(principal *models.Principal, raw string) (string, error)
	// QualifyRefTarget takes no principal because the target's namespace comes
	// from sourceClass.
	QualifyRefTarget(sourceClass, target string) (qualified, short string, err error)
}

// Disabled is the Qualifier of a node with namespaces off. It returns every
// name unchanged.
var Disabled Qualifier = disabled{}

type disabled struct{}

func (disabled) NamespacesEnabled() bool { return false }

func (disabled) Qualify(_ *models.Principal, name string) (string, error) {
	return name, nil
}

func (disabled) QualifyForCreate(_ *models.Principal, raw string) (string, error) {
	return raw, nil
}

func (disabled) QualifyRefTarget(_, target string) (qualified, short string, err error) {
	return target, target, nil
}
