//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package grpc

import "github.com/weaviate/weaviate/entities/models"

type GrpcProperty struct {
	*models.Property
}

type GrpcNestedProperty struct {
	*models.NestedProperty
}

func (p *GrpcProperty) GetName() string {
	return p.Property.Name
}

func (p *GrpcProperty) GetNestedProperties() []*models.NestedProperty {
	return p.Property.NestedProperties
}

func (p *GrpcNestedProperty) GetName() string {
	return p.NestedProperty.Name
}

func (p *GrpcNestedProperty) GetNestedProperties() []*models.NestedProperty {
	return p.NestedProperty.NestedProperties
}
