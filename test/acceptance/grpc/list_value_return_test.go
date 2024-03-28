package test

import (
	"bytes"
	"context"
	"encoding/binary"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	pb "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
	"github.com/weaviate/weaviate/test/helper"
)

const (
	collectionNameLVR = "ListValueReturn"
)

func TestGRPC_ListValueReturn(t *testing.T) {
	grpcClient, _ := newClient(t)
	helper.DeleteClass(t, collectionNameLVR)
	helper.CreateClass(t, &models.Class{
		Class: collectionNameLVR,
		Properties: []*models.Property{
			{
				Name:     "texts",
				DataType: []string{"text[]"},
			},
			{
				Name:     "ints",
				DataType: []string{"int[]"},
			},
			{
				Name:     "bools",
				DataType: []string{"boolean[]"},
			},
			{
				Name:     "numbers",
				DataType: []string{"number[]"},
			},
			{
				Name:     "uuids",
				DataType: []string{"uuid[]"},
			},
			{
				Name:     "dates",
				DataType: []string{"date[]"},
			},
			{
				Name:     "objects",
				DataType: []string{"object[]"},
				NestedProperties: []*models.NestedProperty{{
					Name:     "texts",
					DataType: []string{"text[]"},
				}},
			},
		},
	})
	defer helper.DeleteClass(t, collectionNameLVR)

	var buf bytes.Buffer
	err := binary.Write(&buf, binary.LittleEndian, []float64{1.1, 2.2})
	require.Nil(t, err)
	numbersBytes := buf.Bytes()

	uuid1 := uuid.NewString()
	uuid2 := uuid.NewString()

	batchResp, err := grpcClient.BatchObjects(context.Background(), &pb.BatchObjectsRequest{
		Objects: []*pb.BatchObject{{
			Uuid: uuid.NewString(),
			Properties: &pb.BatchObject_Properties{
				TextArrayProperties: []*pb.TextArrayProperties{
					{
						PropName: "texts",
						Values:   []string{"text1", "text2"},
					},
					{
						PropName: "uuids",
						Values:   []string{uuid1, uuid2},
					},
					{
						PropName: "dates",
						Values: []string{
							"2020-01-01T00:00:00Z",
						},
					},
				},
				IntArrayProperties: []*pb.IntArrayProperties{{
					PropName: "ints",
					Values:   []int64{1, 2},
				}},
				BooleanArrayProperties: []*pb.BooleanArrayProperties{{
					PropName: "bools",
					Values:   []bool{true, false},
				}},
				NumberArrayProperties: []*pb.NumberArrayProperties{{
					PropName:    "numbers",
					ValuesBytes: numbersBytes,
				}},
				ObjectArrayProperties: []*pb.ObjectArrayProperties{{
					PropName: "objects",
					Values: []*pb.ObjectPropertiesValue{{
						TextArrayProperties: []*pb.TextArrayProperties{{
							PropName: "texts",
							Values:   []string{"text1", "text2"},
						}},
					}},
				}},
			},
			Collection: collectionNameLVR,
		}},
	})
	require.Nil(t, err)
	require.Nil(t, batchResp.Errors)

	// Test the list value return
	t.Run("ListValueReturn", func(t *testing.T) {
		in := pb.SearchRequest{
			Collection: collectionNameLVR,
			Properties: &pb.PropertiesRequest{
				NonRefProperties: []string{
					"texts", "ints", "bools", "numbers", "uuids", "dates",
				},
				ObjectProperties: []*pb.ObjectPropertiesRequest{{
					PropName:            "objects",
					PrimitiveProperties: []string{"texts"},
				}},
			},
			Uses_123Api: true,
			Uses_125Api: true,
		}
		searchResp, err := grpcClient.Search(context.Background(), &in)
		require.Nil(t, err)
		require.Len(t, searchResp.Results, 1)
		props := searchResp.Results[0].GetProperties()
		require.NotNil(t, props)
		nonRefProps := props.GetNonRefProps()
		require.NotNil(t, nonRefProps)

		texts := nonRefProps.GetFields()["texts"].GetListValue().GetTextValues()
		require.NotNil(t, texts)
		require.Equal(t, []string{"text1", "text2"}, texts.GetValues())

		ints := nonRefProps.GetFields()["ints"].GetListValue().GetIntValues()
		require.NotNil(t, ints)
		require.Equal(t, []int64{1, 2}, ints.GetValues())

		bools := nonRefProps.GetFields()["bools"].GetListValue().GetBoolValues()
		require.NotNil(t, bools)
		require.Equal(t, []bool{true, false}, bools.GetValues())

		numbers := nonRefProps.GetFields()["numbers"].GetListValue().GetNumberValues()
		require.NotNil(t, numbers)
		require.Equal(t, []float64{1.1, 2.2}, numbers.GetValues())

		uuids := nonRefProps.GetFields()["uuids"].GetListValue().GetUuidValues()
		require.NotNil(t, uuids)
		require.Equal(t, []string{uuid1, uuid2}, uuids.GetValues())

		dates := nonRefProps.GetFields()["dates"].GetListValue().GetDateValues()
		require.NotNil(t, dates)
		require.Equal(t, []string{"2020-01-01T00:00:00Z"}, dates.GetValues())

		objects := nonRefProps.GetFields()["objects"].GetListValue().GetObjectValues()
		require.NotNil(t, objects)
		require.Len(t, objects.GetValues(), 1)
		object := objects.GetValues()[0]
		require.NotNil(t, object)
		texts = object.GetFields()["texts"].GetListValue().GetTextValues()
		require.NotNil(t, texts)
		require.Equal(t, []string{"text1", "text2"}, texts.GetValues())
	})
}
