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

//go:build integrationTest

package db

import (
	"errors"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/usecases/objects"
)

// buildStorObj is the common constructor for conditional storobj test fixtures.
// cond controls the conditional behaviour; callers set exactly one flag.
func buildStorObj(className string, id strfmt.UUID, cond storobj.Conditional) *storobj.Object {
	return &storobj.Object{
		MarshallerVersion: 1,
		Object: models.Object{
			ID:                 id,
			Class:              className,
			LastUpdateTimeUnix: time.Now().UnixMilli(),
		},
		Conditional: cond,
	}
}

// buildConditionalObject returns a storobj with OnlyIfNotExists=true
// (insert_if_not_exists semantics).
func buildConditionalObject(className string, id strfmt.UUID) *storobj.Object {
	return buildStorObj(className, id, storobj.Conditional{OnlyIfNotExists: true})
}

// buildOnlyIfExistsObject returns a storobj with OnlyIfExists=true
// (update_if_exists semantics).
func buildOnlyIfExistsObject(className string, id strfmt.UUID) *storobj.Object {
	return buildStorObj(className, id, storobj.Conditional{OnlyIfExists: true})
}

// buildUnconditionalObject returns a plain storobj with no conditional flags.
func buildUnconditionalObject(className string, id strfmt.UUID) *storobj.Object {
	return buildStorObj(className, id, storobj.Conditional{})
}

// buildMTConditionalObject returns an insert_if_not_exists storobj for a
// multi-tenant class with the given tenant and UUID.
func buildMTConditionalObject(className, tenant string, id strfmt.UUID) *storobj.Object {
	return &storobj.Object{
		MarshallerVersion: 1,
		Object: models.Object{
			ID:                 id,
			Class:              className,
			Tenant:             tenant,
			LastUpdateTimeUnix: time.Now().UnixMilli(),
		},
		Conditional: storobj.Conditional{
			OnlyIfNotExists: true,
		},
	}
}

// isPreconditionFailed returns true when err is or wraps *objects.ErrPreconditionFailed.
func isPreconditionFailed(err error) bool {
	var pf *objects.ErrPreconditionFailed
	return errors.As(err, &pf)
}
