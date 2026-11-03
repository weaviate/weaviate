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

package namespaces

import (
	"fmt"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/schema/namespacing"
)

// Prefixing is the namespacing.Qualifier of a node with NAMESPACES_ENABLED and
// a well-formed Weaviate license key. It prefixes a name with the caller's
// confined namespace, and a ref target with its source class's namespace.
type Prefixing struct{}

func NewPrefixing() *Prefixing { return &Prefixing{} }

func (*Prefixing) NamespacesEnabled() bool { return true }

// Qualify returns name unchanged for an unconfined caller.
func (*Prefixing) Qualify(principal *models.Principal, name string) (string, error) {
	return namespacing.QualifiedName(namespacing.ConfinedNamespace(principal), name), nil
}

// QualifyForCreate rejects an unconfined caller with namespacing.ErrCreateRequiresNamespace,
// and a raw name longer than namespacing.ShortNameMaxLength.
func (*Prefixing) QualifyForCreate(principal *models.Principal, raw string) (string, error) {
	ns := namespacing.ConfinedNamespace(principal)
	if ns == "" {
		return "", namespacing.ErrCreateRequiresNamespace
	}
	if len(raw) > namespacing.ShortNameMaxLength {
		return "", fmt.Errorf("'%s' is too long: namespaced names must be at most %d characters before qualification", raw, namespacing.ShortNameMaxLength)
	}
	return namespacing.QualifiedName(ns, raw), nil
}

// QualifyRefTarget rejects a target whose prefix names a namespace other than
// sourceClass's, because refs cannot cross namespaces.
func (*Prefixing) QualifyRefTarget(sourceClass, target string) (qualified, short string, err error) {
	sourceNS := namespacing.NamespaceFromQualified(sourceClass)
	if ns := namespacing.NamespaceFromQualified(target); ns != "" && ns != sourceNS {
		return "", "", fmt.Errorf("'%s' is not a valid class name", target)
	}
	short = namespacing.StripQualification(target)
	return namespacing.QualifiedName(sourceNS, short), short, nil
}
