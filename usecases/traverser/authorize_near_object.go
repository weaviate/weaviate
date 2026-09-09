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

package traverser

import (
	"context"
	"strings"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/schema/crossref"
	"github.com/weaviate/weaviate/entities/searchparams"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
)

func (t *Traverser) authorizeNearObjectBeacon(ctx context.Context,
	principal *models.Principal, nearObject *searchparams.NearObject,
	className, tenant string,
) error {
	if nearObject == nil || len(nearObject.Beacon) == 0 {
		// an id-only nearObject always resolves within the searched
		// collection, which the caller has already authorized
		return nil
	}

	ref, err := crossref.Parse(nearObject.Beacon)
	if err != nil {
		return err
	}
	if ref.Class == "" {
		// a class-less beacon resolves within the searched collection (Get,
		// Aggregate) or across all collections (Explore, gated on read
		// access to all collections' data by its resolver)
		return nil
	}

	target := t.resolveAliasIfPresent(schema.UppercaseClassName(ref.Class))
	// the index lookup downstream is case-insensitive, so a same-collection
	// beacon in any casing is covered by the caller's own authorization
	if strings.EqualFold(target, t.resolveAliasIfPresent(schema.UppercaseClassName(className))) {
		return nil
	}

	return t.authorizer.Authorize(ctx, principal, authorization.READ,
		authorization.Objects(target, tenant, ref.TargetID))
}

func (t *Traverser) resolveAliasIfPresent(className string) string {
	if cls := t.schemaGetter.ResolveAlias(className); cls != "" {
		return cls
	}
	return className
}
