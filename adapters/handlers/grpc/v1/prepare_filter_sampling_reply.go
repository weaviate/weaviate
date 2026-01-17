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
	"time"

	"github.com/weaviate/weaviate/entities/filtersampling"
	"github.com/weaviate/weaviate/entities/schema"
	pb "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
)

type FilterSamplingReplier struct {
	dataType schema.DataType
}

func NewFilterSamplingReplier(dataType schema.DataType) *FilterSamplingReplier {
	return &FilterSamplingReplier{
		dataType: dataType,
	}
}

func (r *FilterSamplingReplier) Reply(result *filtersampling.Result) *pb.FilterSamplingReply {
	if result == nil {
		return &pb.FilterSamplingReply{}
	}

	samples := make([]*pb.FilterSamplingReply_Sample, len(result.Samples))
	for i, sample := range result.Samples {
		samples[i] = r.convertSample(sample)
	}

	return &pb.FilterSamplingReply{
		Samples:                 samples,
		TotalObjects:            result.TotalObjects,
		EstimatedPercentCovered: result.EstimatedPercentCovered,
	}
}

func (r *FilterSamplingReplier) convertSample(sample filtersampling.Sample) *pb.FilterSamplingReply_Sample {
	pbSample := &pb.FilterSamplingReply_Sample{
		Cardinality: sample.Cardinality,
	}

	switch r.dataType {
	case schema.DataTypeText, schema.DataTypeTextArray:
		if v, ok := sample.Value.(string); ok {
			pbSample.Value = &pb.FilterSamplingReply_Sample_TextValue{TextValue: v}
		}
	case schema.DataTypeInt, schema.DataTypeIntArray:
		switch v := sample.Value.(type) {
		case int64:
			pbSample.Value = &pb.FilterSamplingReply_Sample_IntValue{IntValue: v}
		case int:
			pbSample.Value = &pb.FilterSamplingReply_Sample_IntValue{IntValue: int64(v)}
		}
	case schema.DataTypeNumber, schema.DataTypeNumberArray:
		if v, ok := sample.Value.(float64); ok {
			pbSample.Value = &pb.FilterSamplingReply_Sample_NumberValue{NumberValue: v}
		}
	case schema.DataTypeBoolean, schema.DataTypeBooleanArray:
		if v, ok := sample.Value.(bool); ok {
			pbSample.Value = &pb.FilterSamplingReply_Sample_BoolValue{BoolValue: v}
		}
	case schema.DataTypeDate, schema.DataTypeDateArray:
		switch v := sample.Value.(type) {
		case string:
			pbSample.Value = &pb.FilterSamplingReply_Sample_DateValue{DateValue: v}
		case time.Time:
			pbSample.Value = &pb.FilterSamplingReply_Sample_DateValue{DateValue: v.Format(time.RFC3339Nano)}
		}
	case schema.DataTypeUUID, schema.DataTypeUUIDArray:
		if v, ok := sample.Value.(string); ok {
			pbSample.Value = &pb.FilterSamplingReply_Sample_UuidValue{UuidValue: v}
		}
	default:
		// For any other type, try string conversion
		if v, ok := sample.Value.(string); ok {
			pbSample.Value = &pb.FilterSamplingReply_Sample_TextValue{TextValue: v}
		}
	}

	return pbSample
}
