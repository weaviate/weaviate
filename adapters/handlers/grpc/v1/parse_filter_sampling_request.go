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

package v1

import (
	"fmt"

	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/entities/filtersampling"
	"github.com/weaviate/weaviate/entities/schema"
	pb "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
	"github.com/weaviate/weaviate/usecases/config/runtime"
)

type FilterSamplingParser struct {
	authorizedGetClass classGetterWithAuthzFunc
	maxSampleCount     *runtime.DynamicValue[int]
}

func NewFilterSamplingParser(
	authorizedGetClass classGetterWithAuthzFunc,
	maxSampleCount *runtime.DynamicValue[int],
) *FilterSamplingParser {
	return &FilterSamplingParser{
		authorizedGetClass: authorizedGetClass,
		maxSampleCount:     maxSampleCount,
	}
}

func (p *FilterSamplingParser) Parse(req *pb.FilterSamplingRequest) (*filtersampling.Params, schema.DataType, error) {
	if req.Collection == "" {
		return nil, "", fmt.Errorf("collection is required")
	}
	if req.Property == "" {
		return nil, "", fmt.Errorf("property is required")
	}
	if req.SampleCount == 0 {
		return nil, "", fmt.Errorf("sample_count must be greater than 0")
	}

	maxSampleCount := p.maxSampleCount.Get()
	if int(req.SampleCount) > maxSampleCount {
		return nil, "", fmt.Errorf("sample_count must be <= %d", maxSampleCount)
	}

	class, err := p.authorizedGetClass(req.Collection)
	if err != nil {
		return nil, "", err
	}

	prop, err := schema.GetPropertyByName(class, req.Property)
	if err != nil {
		return nil, "", fmt.Errorf("property %q not found in class %s", req.Property, req.Collection)
	}

	// Check if property is a reference using FindPropertyDataTypeWithRefsAndAuth
	dataType, err := schema.FindPropertyDataTypeWithRefsAndAuth(
		p.authorizedGetClass, prop.DataType, false, schema.ClassName(class.Class))
	if err != nil {
		return nil, "", fmt.Errorf("get property data type: %w", err)
	}
	if dataType.IsReference() {
		return nil, "", fmt.Errorf("filter sampling not supported for reference properties")
	}

	// Check if property has a filterable index
	if !inverted.HasFilterableIndex(prop) {
		return nil, "", fmt.Errorf("property %q does not have a filterable index", req.Property)
	}

	tenant := ""
	if req.Tenant != nil {
		tenant = *req.Tenant
	}

	return &filtersampling.Params{
		ClassName:    schema.ClassName(class.Class),
		PropertyName: prop.Name,
		SampleCount:  int(req.SampleCount),
		Tenant:       tenant,
	}, dataType.AsPrimitive(), nil
}
