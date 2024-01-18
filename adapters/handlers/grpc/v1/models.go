//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package v1

import "github.com/weaviate/weaviate/entities/models"

type Property struct {
	*models.Property
}

type NestedProperty struct {
	*models.NestedProperty
}

func (p *Property) GetName() string {
	return p.Property.Name
}

func (p *Property) GetNestedProperties() []*models.NestedProperty {
	return p.Property.NestedProperties
}

func (p *NestedProperty) GetName() string {
	return p.NestedProperty.Name
}

func (p *NestedProperty) GetNestedProperties() []*models.NestedProperty {
	return p.NestedProperty.NestedProperties
}
