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

// Code generated by protoc-gen-go. DO NOT EDIT.

package protocol

import (
	reflect "reflect"
	sync "sync"

	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	structpb "google.golang.org/protobuf/types/known/structpb"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

type BatchObjectsRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Objects          []*BatchObject    `protobuf:"bytes,1,rep,name=objects,proto3" json:"objects,omitempty"`
	ConsistencyLevel *ConsistencyLevel `protobuf:"varint,2,opt,name=consistency_level,json=consistencyLevel,proto3,enum=weaviate.v1.ConsistencyLevel,oneof" json:"consistency_level,omitempty"`
}

func (x *BatchObjectsRequest) Reset() {
	*x = BatchObjectsRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_v1_batch_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *BatchObjectsRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*BatchObjectsRequest) ProtoMessage() {}

func (x *BatchObjectsRequest) ProtoReflect() protoreflect.Message {
	mi := &file_v1_batch_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use BatchObjectsRequest.ProtoReflect.Descriptor instead.
func (*BatchObjectsRequest) Descriptor() ([]byte, []int) {
	return file_v1_batch_proto_rawDescGZIP(), []int{0}
}

func (x *BatchObjectsRequest) GetObjects() []*BatchObject {
	if x != nil {
		return x.Objects
	}
	return nil
}

func (x *BatchObjectsRequest) GetConsistencyLevel() ConsistencyLevel {
	if x != nil && x.ConsistencyLevel != nil {
		return *x.ConsistencyLevel
	}
	return ConsistencyLevel_CONSISTENCY_LEVEL_UNSPECIFIED
}

type BatchObject struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Uuid string `protobuf:"bytes,1,opt,name=uuid,proto3" json:"uuid,omitempty"`
	// protolint:disable:next REPEATED_FIELD_NAMES_PLURALIZED
	//
	// Deprecated: Marked as deprecated in v1/batch.proto.
	Vector      []float32               `protobuf:"fixed32,2,rep,packed,name=vector,proto3" json:"vector,omitempty"` // deprecated, will be removed
	Properties  *BatchObject_Properties `protobuf:"bytes,3,opt,name=properties,proto3" json:"properties,omitempty"`
	Collection  string                  `protobuf:"bytes,4,opt,name=collection,proto3" json:"collection,omitempty"`
	Tenant      string                  `protobuf:"bytes,5,opt,name=tenant,proto3" json:"tenant,omitempty"`
	VectorBytes []byte                  `protobuf:"bytes,6,opt,name=vector_bytes,json=vectorBytes,proto3" json:"vector_bytes,omitempty"`
}

func (x *BatchObject) Reset() {
	*x = BatchObject{}
	if protoimpl.UnsafeEnabled {
		mi := &file_v1_batch_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *BatchObject) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*BatchObject) ProtoMessage() {}

func (x *BatchObject) ProtoReflect() protoreflect.Message {
	mi := &file_v1_batch_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use BatchObject.ProtoReflect.Descriptor instead.
func (*BatchObject) Descriptor() ([]byte, []int) {
	return file_v1_batch_proto_rawDescGZIP(), []int{1}
}

func (x *BatchObject) GetUuid() string {
	if x != nil {
		return x.Uuid
	}
	return ""
}

// Deprecated: Marked as deprecated in v1/batch.proto.
func (x *BatchObject) GetVector() []float32 {
	if x != nil {
		return x.Vector
	}
	return nil
}

func (x *BatchObject) GetProperties() *BatchObject_Properties {
	if x != nil {
		return x.Properties
	}
	return nil
}

func (x *BatchObject) GetCollection() string {
	if x != nil {
		return x.Collection
	}
	return ""
}

func (x *BatchObject) GetTenant() string {
	if x != nil {
		return x.Tenant
	}
	return ""
}

func (x *BatchObject) GetVectorBytes() []byte {
	if x != nil {
		return x.VectorBytes
	}
	return nil
}

type BatchObjectsReply struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Took   float32                         `protobuf:"fixed32,1,opt,name=took,proto3" json:"took,omitempty"`
	Errors []*BatchObjectsReply_BatchError `protobuf:"bytes,2,rep,name=errors,proto3" json:"errors,omitempty"`
}

func (x *BatchObjectsReply) Reset() {
	*x = BatchObjectsReply{}
	if protoimpl.UnsafeEnabled {
		mi := &file_v1_batch_proto_msgTypes[2]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *BatchObjectsReply) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*BatchObjectsReply) ProtoMessage() {}

func (x *BatchObjectsReply) ProtoReflect() protoreflect.Message {
	mi := &file_v1_batch_proto_msgTypes[2]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use BatchObjectsReply.ProtoReflect.Descriptor instead.
func (*BatchObjectsReply) Descriptor() ([]byte, []int) {
	return file_v1_batch_proto_rawDescGZIP(), []int{2}
}

func (x *BatchObjectsReply) GetTook() float32 {
	if x != nil {
		return x.Took
	}
	return 0
}

func (x *BatchObjectsReply) GetErrors() []*BatchObjectsReply_BatchError {
	if x != nil {
		return x.Errors
	}
	return nil
}

type BatchObject_Properties struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	NonRefProperties       *structpb.Struct                    `protobuf:"bytes,1,opt,name=non_ref_properties,json=nonRefProperties,proto3" json:"non_ref_properties,omitempty"`
	SingleTargetRefProps   []*BatchObject_SingleTargetRefProps `protobuf:"bytes,2,rep,name=single_target_ref_props,json=singleTargetRefProps,proto3" json:"single_target_ref_props,omitempty"`
	MultiTargetRefProps    []*BatchObject_MultiTargetRefProps  `protobuf:"bytes,3,rep,name=multi_target_ref_props,json=multiTargetRefProps,proto3" json:"multi_target_ref_props,omitempty"`
	NumberArrayProperties  []*NumberArrayProperties            `protobuf:"bytes,4,rep,name=number_array_properties,json=numberArrayProperties,proto3" json:"number_array_properties,omitempty"`
	IntArrayProperties     []*IntArrayProperties               `protobuf:"bytes,5,rep,name=int_array_properties,json=intArrayProperties,proto3" json:"int_array_properties,omitempty"`
	TextArrayProperties    []*TextArrayProperties              `protobuf:"bytes,6,rep,name=text_array_properties,json=textArrayProperties,proto3" json:"text_array_properties,omitempty"`
	BooleanArrayProperties []*BooleanArrayProperties           `protobuf:"bytes,7,rep,name=boolean_array_properties,json=booleanArrayProperties,proto3" json:"boolean_array_properties,omitempty"`
	ObjectProperties       []*ObjectProperties                 `protobuf:"bytes,8,rep,name=object_properties,json=objectProperties,proto3" json:"object_properties,omitempty"`
	ObjectArrayProperties  []*ObjectArrayProperties            `protobuf:"bytes,9,rep,name=object_array_properties,json=objectArrayProperties,proto3" json:"object_array_properties,omitempty"`
}

func (x *BatchObject_Properties) Reset() {
	*x = BatchObject_Properties{}
	if protoimpl.UnsafeEnabled {
		mi := &file_v1_batch_proto_msgTypes[3]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *BatchObject_Properties) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*BatchObject_Properties) ProtoMessage() {}

func (x *BatchObject_Properties) ProtoReflect() protoreflect.Message {
	mi := &file_v1_batch_proto_msgTypes[3]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use BatchObject_Properties.ProtoReflect.Descriptor instead.
func (*BatchObject_Properties) Descriptor() ([]byte, []int) {
	return file_v1_batch_proto_rawDescGZIP(), []int{1, 0}
}

func (x *BatchObject_Properties) GetNonRefProperties() *structpb.Struct {
	if x != nil {
		return x.NonRefProperties
	}
	return nil
}

func (x *BatchObject_Properties) GetSingleTargetRefProps() []*BatchObject_SingleTargetRefProps {
	if x != nil {
		return x.SingleTargetRefProps
	}
	return nil
}

func (x *BatchObject_Properties) GetMultiTargetRefProps() []*BatchObject_MultiTargetRefProps {
	if x != nil {
		return x.MultiTargetRefProps
	}
	return nil
}

func (x *BatchObject_Properties) GetNumberArrayProperties() []*NumberArrayProperties {
	if x != nil {
		return x.NumberArrayProperties
	}
	return nil
}

func (x *BatchObject_Properties) GetIntArrayProperties() []*IntArrayProperties {
	if x != nil {
		return x.IntArrayProperties
	}
	return nil
}

func (x *BatchObject_Properties) GetTextArrayProperties() []*TextArrayProperties {
	if x != nil {
		return x.TextArrayProperties
	}
	return nil
}

func (x *BatchObject_Properties) GetBooleanArrayProperties() []*BooleanArrayProperties {
	if x != nil {
		return x.BooleanArrayProperties
	}
	return nil
}

func (x *BatchObject_Properties) GetObjectProperties() []*ObjectProperties {
	if x != nil {
		return x.ObjectProperties
	}
	return nil
}

func (x *BatchObject_Properties) GetObjectArrayProperties() []*ObjectArrayProperties {
	if x != nil {
		return x.ObjectArrayProperties
	}
	return nil
}

type BatchObject_SingleTargetRefProps struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Uuids    []string `protobuf:"bytes,1,rep,name=uuids,proto3" json:"uuids,omitempty"`
	PropName string   `protobuf:"bytes,2,opt,name=prop_name,json=propName,proto3" json:"prop_name,omitempty"`
}

func (x *BatchObject_SingleTargetRefProps) Reset() {
	*x = BatchObject_SingleTargetRefProps{}
	if protoimpl.UnsafeEnabled {
		mi := &file_v1_batch_proto_msgTypes[4]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *BatchObject_SingleTargetRefProps) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*BatchObject_SingleTargetRefProps) ProtoMessage() {}

func (x *BatchObject_SingleTargetRefProps) ProtoReflect() protoreflect.Message {
	mi := &file_v1_batch_proto_msgTypes[4]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use BatchObject_SingleTargetRefProps.ProtoReflect.Descriptor instead.
func (*BatchObject_SingleTargetRefProps) Descriptor() ([]byte, []int) {
	return file_v1_batch_proto_rawDescGZIP(), []int{1, 1}
}

func (x *BatchObject_SingleTargetRefProps) GetUuids() []string {
	if x != nil {
		return x.Uuids
	}
	return nil
}

func (x *BatchObject_SingleTargetRefProps) GetPropName() string {
	if x != nil {
		return x.PropName
	}
	return ""
}

type BatchObject_MultiTargetRefProps struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Uuids            []string `protobuf:"bytes,1,rep,name=uuids,proto3" json:"uuids,omitempty"`
	PropName         string   `protobuf:"bytes,2,opt,name=prop_name,json=propName,proto3" json:"prop_name,omitempty"`
	TargetCollection string   `protobuf:"bytes,3,opt,name=target_collection,json=targetCollection,proto3" json:"target_collection,omitempty"`
}

func (x *BatchObject_MultiTargetRefProps) Reset() {
	*x = BatchObject_MultiTargetRefProps{}
	if protoimpl.UnsafeEnabled {
		mi := &file_v1_batch_proto_msgTypes[5]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *BatchObject_MultiTargetRefProps) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*BatchObject_MultiTargetRefProps) ProtoMessage() {}

func (x *BatchObject_MultiTargetRefProps) ProtoReflect() protoreflect.Message {
	mi := &file_v1_batch_proto_msgTypes[5]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use BatchObject_MultiTargetRefProps.ProtoReflect.Descriptor instead.
func (*BatchObject_MultiTargetRefProps) Descriptor() ([]byte, []int) {
	return file_v1_batch_proto_rawDescGZIP(), []int{1, 2}
}

func (x *BatchObject_MultiTargetRefProps) GetUuids() []string {
	if x != nil {
		return x.Uuids
	}
	return nil
}

func (x *BatchObject_MultiTargetRefProps) GetPropName() string {
	if x != nil {
		return x.PropName
	}
	return ""
}

func (x *BatchObject_MultiTargetRefProps) GetTargetCollection() string {
	if x != nil {
		return x.TargetCollection
	}
	return ""
}

type BatchObjectsReply_BatchError struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Index int32  `protobuf:"varint,1,opt,name=index,proto3" json:"index,omitempty"`
	Error string `protobuf:"bytes,2,opt,name=error,proto3" json:"error,omitempty"`
}

func (x *BatchObjectsReply_BatchError) Reset() {
	*x = BatchObjectsReply_BatchError{}
	if protoimpl.UnsafeEnabled {
		mi := &file_v1_batch_proto_msgTypes[6]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *BatchObjectsReply_BatchError) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*BatchObjectsReply_BatchError) ProtoMessage() {}

func (x *BatchObjectsReply_BatchError) ProtoReflect() protoreflect.Message {
	mi := &file_v1_batch_proto_msgTypes[6]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use BatchObjectsReply_BatchError.ProtoReflect.Descriptor instead.
func (*BatchObjectsReply_BatchError) Descriptor() ([]byte, []int) {
	return file_v1_batch_proto_rawDescGZIP(), []int{2, 0}
}

func (x *BatchObjectsReply_BatchError) GetIndex() int32 {
	if x != nil {
		return x.Index
	}
	return 0
}

func (x *BatchObjectsReply_BatchError) GetError() string {
	if x != nil {
		return x.Error
	}
	return ""
}

var File_v1_batch_proto protoreflect.FileDescriptor

var file_v1_batch_proto_rawDesc = []byte{
	0x0a, 0x0e, 0x76, 0x31, 0x2f, 0x62, 0x61, 0x74, 0x63, 0x68, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f,
	0x12, 0x0b, 0x77, 0x65, 0x61, 0x76, 0x69, 0x61, 0x74, 0x65, 0x2e, 0x76, 0x31, 0x1a, 0x1c, 0x67,
	0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2f, 0x73,
	0x74, 0x72, 0x75, 0x63, 0x74, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x1a, 0x0d, 0x76, 0x31, 0x2f,
	0x62, 0x61, 0x73, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x22, 0xb0, 0x01, 0x0a, 0x13, 0x42,
	0x61, 0x74, 0x63, 0x68, 0x4f, 0x62, 0x6a, 0x65, 0x63, 0x74, 0x73, 0x52, 0x65, 0x71, 0x75, 0x65,
	0x73, 0x74, 0x12, 0x32, 0x0a, 0x07, 0x6f, 0x62, 0x6a, 0x65, 0x63, 0x74, 0x73, 0x18, 0x01, 0x20,
	0x03, 0x28, 0x0b, 0x32, 0x18, 0x2e, 0x77, 0x65, 0x61, 0x76, 0x69, 0x61, 0x74, 0x65, 0x2e, 0x76,
	0x31, 0x2e, 0x42, 0x61, 0x74, 0x63, 0x68, 0x4f, 0x62, 0x6a, 0x65, 0x63, 0x74, 0x52, 0x07, 0x6f,
	0x62, 0x6a, 0x65, 0x63, 0x74, 0x73, 0x12, 0x4f, 0x0a, 0x11, 0x63, 0x6f, 0x6e, 0x73, 0x69, 0x73,
	0x74, 0x65, 0x6e, 0x63, 0x79, 0x5f, 0x6c, 0x65, 0x76, 0x65, 0x6c, 0x18, 0x02, 0x20, 0x01, 0x28,
	0x0e, 0x32, 0x1d, 0x2e, 0x77, 0x65, 0x61, 0x76, 0x69, 0x61, 0x74, 0x65, 0x2e, 0x76, 0x31, 0x2e,
	0x43, 0x6f, 0x6e, 0x73, 0x69, 0x73, 0x74, 0x65, 0x6e, 0x63, 0x79, 0x4c, 0x65, 0x76, 0x65, 0x6c,
	0x48, 0x00, 0x52, 0x10, 0x63, 0x6f, 0x6e, 0x73, 0x69, 0x73, 0x74, 0x65, 0x6e, 0x63, 0x79, 0x4c,
	0x65, 0x76, 0x65, 0x6c, 0x88, 0x01, 0x01, 0x42, 0x14, 0x0a, 0x12, 0x5f, 0x63, 0x6f, 0x6e, 0x73,
	0x69, 0x73, 0x74, 0x65, 0x6e, 0x63, 0x79, 0x5f, 0x6c, 0x65, 0x76, 0x65, 0x6c, 0x22, 0xca, 0x09,
	0x0a, 0x0b, 0x42, 0x61, 0x74, 0x63, 0x68, 0x4f, 0x62, 0x6a, 0x65, 0x63, 0x74, 0x12, 0x12, 0x0a,
	0x04, 0x75, 0x75, 0x69, 0x64, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x04, 0x75, 0x75, 0x69,
	0x64, 0x12, 0x1a, 0x0a, 0x06, 0x76, 0x65, 0x63, 0x74, 0x6f, 0x72, 0x18, 0x02, 0x20, 0x03, 0x28,
	0x02, 0x42, 0x02, 0x18, 0x01, 0x52, 0x06, 0x76, 0x65, 0x63, 0x74, 0x6f, 0x72, 0x12, 0x43, 0x0a,
	0x0a, 0x70, 0x72, 0x6f, 0x70, 0x65, 0x72, 0x74, 0x69, 0x65, 0x73, 0x18, 0x03, 0x20, 0x01, 0x28,
	0x0b, 0x32, 0x23, 0x2e, 0x77, 0x65, 0x61, 0x76, 0x69, 0x61, 0x74, 0x65, 0x2e, 0x76, 0x31, 0x2e,
	0x42, 0x61, 0x74, 0x63, 0x68, 0x4f, 0x62, 0x6a, 0x65, 0x63, 0x74, 0x2e, 0x50, 0x72, 0x6f, 0x70,
	0x65, 0x72, 0x74, 0x69, 0x65, 0x73, 0x52, 0x0a, 0x70, 0x72, 0x6f, 0x70, 0x65, 0x72, 0x74, 0x69,
	0x65, 0x73, 0x12, 0x1e, 0x0a, 0x0a, 0x63, 0x6f, 0x6c, 0x6c, 0x65, 0x63, 0x74, 0x69, 0x6f, 0x6e,
	0x18, 0x04, 0x20, 0x01, 0x28, 0x09, 0x52, 0x0a, 0x63, 0x6f, 0x6c, 0x6c, 0x65, 0x63, 0x74, 0x69,
	0x6f, 0x6e, 0x12, 0x16, 0x0a, 0x06, 0x74, 0x65, 0x6e, 0x61, 0x6e, 0x74, 0x18, 0x05, 0x20, 0x01,
	0x28, 0x09, 0x52, 0x06, 0x74, 0x65, 0x6e, 0x61, 0x6e, 0x74, 0x12, 0x21, 0x0a, 0x0c, 0x76, 0x65,
	0x63, 0x74, 0x6f, 0x72, 0x5f, 0x62, 0x79, 0x74, 0x65, 0x73, 0x18, 0x06, 0x20, 0x01, 0x28, 0x0c,
	0x52, 0x0b, 0x76, 0x65, 0x63, 0x74, 0x6f, 0x72, 0x42, 0x79, 0x74, 0x65, 0x73, 0x1a, 0xa8, 0x06,
	0x0a, 0x0a, 0x50, 0x72, 0x6f, 0x70, 0x65, 0x72, 0x74, 0x69, 0x65, 0x73, 0x12, 0x45, 0x0a, 0x12,
	0x6e, 0x6f, 0x6e, 0x5f, 0x72, 0x65, 0x66, 0x5f, 0x70, 0x72, 0x6f, 0x70, 0x65, 0x72, 0x74, 0x69,
	0x65, 0x73, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x17, 0x2e, 0x67, 0x6f, 0x6f, 0x67, 0x6c,
	0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2e, 0x53, 0x74, 0x72, 0x75, 0x63,
	0x74, 0x52, 0x10, 0x6e, 0x6f, 0x6e, 0x52, 0x65, 0x66, 0x50, 0x72, 0x6f, 0x70, 0x65, 0x72, 0x74,
	0x69, 0x65, 0x73, 0x12, 0x64, 0x0a, 0x17, 0x73, 0x69, 0x6e, 0x67, 0x6c, 0x65, 0x5f, 0x74, 0x61,
	0x72, 0x67, 0x65, 0x74, 0x5f, 0x72, 0x65, 0x66, 0x5f, 0x70, 0x72, 0x6f, 0x70, 0x73, 0x18, 0x02,
	0x20, 0x03, 0x28, 0x0b, 0x32, 0x2d, 0x2e, 0x77, 0x65, 0x61, 0x76, 0x69, 0x61, 0x74, 0x65, 0x2e,
	0x76, 0x31, 0x2e, 0x42, 0x61, 0x74, 0x63, 0x68, 0x4f, 0x62, 0x6a, 0x65, 0x63, 0x74, 0x2e, 0x53,
	0x69, 0x6e, 0x67, 0x6c, 0x65, 0x54, 0x61, 0x72, 0x67, 0x65, 0x74, 0x52, 0x65, 0x66, 0x50, 0x72,
	0x6f, 0x70, 0x73, 0x52, 0x14, 0x73, 0x69, 0x6e, 0x67, 0x6c, 0x65, 0x54, 0x61, 0x72, 0x67, 0x65,
	0x74, 0x52, 0x65, 0x66, 0x50, 0x72, 0x6f, 0x70, 0x73, 0x12, 0x61, 0x0a, 0x16, 0x6d, 0x75, 0x6c,
	0x74, 0x69, 0x5f, 0x74, 0x61, 0x72, 0x67, 0x65, 0x74, 0x5f, 0x72, 0x65, 0x66, 0x5f, 0x70, 0x72,
	0x6f, 0x70, 0x73, 0x18, 0x03, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x2c, 0x2e, 0x77, 0x65, 0x61, 0x76,
	0x69, 0x61, 0x74, 0x65, 0x2e, 0x76, 0x31, 0x2e, 0x42, 0x61, 0x74, 0x63, 0x68, 0x4f, 0x62, 0x6a,
	0x65, 0x63, 0x74, 0x2e, 0x4d, 0x75, 0x6c, 0x74, 0x69, 0x54, 0x61, 0x72, 0x67, 0x65, 0x74, 0x52,
	0x65, 0x66, 0x50, 0x72, 0x6f, 0x70, 0x73, 0x52, 0x13, 0x6d, 0x75, 0x6c, 0x74, 0x69, 0x54, 0x61,
	0x72, 0x67, 0x65, 0x74, 0x52, 0x65, 0x66, 0x50, 0x72, 0x6f, 0x70, 0x73, 0x12, 0x5a, 0x0a, 0x17,
	0x6e, 0x75, 0x6d, 0x62, 0x65, 0x72, 0x5f, 0x61, 0x72, 0x72, 0x61, 0x79, 0x5f, 0x70, 0x72, 0x6f,
	0x70, 0x65, 0x72, 0x74, 0x69, 0x65, 0x73, 0x18, 0x04, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x22, 0x2e,
	0x77, 0x65, 0x61, 0x76, 0x69, 0x61, 0x74, 0x65, 0x2e, 0x76, 0x31, 0x2e, 0x4e, 0x75, 0x6d, 0x62,
	0x65, 0x72, 0x41, 0x72, 0x72, 0x61, 0x79, 0x50, 0x72, 0x6f, 0x70, 0x65, 0x72, 0x74, 0x69, 0x65,
	0x73, 0x52, 0x15, 0x6e, 0x75, 0x6d, 0x62, 0x65, 0x72, 0x41, 0x72, 0x72, 0x61, 0x79, 0x50, 0x72,
	0x6f, 0x70, 0x65, 0x72, 0x74, 0x69, 0x65, 0x73, 0x12, 0x51, 0x0a, 0x14, 0x69, 0x6e, 0x74, 0x5f,
	0x61, 0x72, 0x72, 0x61, 0x79, 0x5f, 0x70, 0x72, 0x6f, 0x70, 0x65, 0x72, 0x74, 0x69, 0x65, 0x73,
	0x18, 0x05, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x1f, 0x2e, 0x77, 0x65, 0x61, 0x76, 0x69, 0x61, 0x74,
	0x65, 0x2e, 0x76, 0x31, 0x2e, 0x49, 0x6e, 0x74, 0x41, 0x72, 0x72, 0x61, 0x79, 0x50, 0x72, 0x6f,
	0x70, 0x65, 0x72, 0x74, 0x69, 0x65, 0x73, 0x52, 0x12, 0x69, 0x6e, 0x74, 0x41, 0x72, 0x72, 0x61,
	0x79, 0x50, 0x72, 0x6f, 0x70, 0x65, 0x72, 0x74, 0x69, 0x65, 0x73, 0x12, 0x54, 0x0a, 0x15, 0x74,
	0x65, 0x78, 0x74, 0x5f, 0x61, 0x72, 0x72, 0x61, 0x79, 0x5f, 0x70, 0x72, 0x6f, 0x70, 0x65, 0x72,
	0x74, 0x69, 0x65, 0x73, 0x18, 0x06, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x20, 0x2e, 0x77, 0x65, 0x61,
	0x76, 0x69, 0x61, 0x74, 0x65, 0x2e, 0x76, 0x31, 0x2e, 0x54, 0x65, 0x78, 0x74, 0x41, 0x72, 0x72,
	0x61, 0x79, 0x50, 0x72, 0x6f, 0x70, 0x65, 0x72, 0x74, 0x69, 0x65, 0x73, 0x52, 0x13, 0x74, 0x65,
	0x78, 0x74, 0x41, 0x72, 0x72, 0x61, 0x79, 0x50, 0x72, 0x6f, 0x70, 0x65, 0x72, 0x74, 0x69, 0x65,
	0x73, 0x12, 0x5d, 0x0a, 0x18, 0x62, 0x6f, 0x6f, 0x6c, 0x65, 0x61, 0x6e, 0x5f, 0x61, 0x72, 0x72,
	0x61, 0x79, 0x5f, 0x70, 0x72, 0x6f, 0x70, 0x65, 0x72, 0x74, 0x69, 0x65, 0x73, 0x18, 0x07, 0x20,
	0x03, 0x28, 0x0b, 0x32, 0x23, 0x2e, 0x77, 0x65, 0x61, 0x76, 0x69, 0x61, 0x74, 0x65, 0x2e, 0x76,
	0x31, 0x2e, 0x42, 0x6f, 0x6f, 0x6c, 0x65, 0x61, 0x6e, 0x41, 0x72, 0x72, 0x61, 0x79, 0x50, 0x72,
	0x6f, 0x70, 0x65, 0x72, 0x74, 0x69, 0x65, 0x73, 0x52, 0x16, 0x62, 0x6f, 0x6f, 0x6c, 0x65, 0x61,
	0x6e, 0x41, 0x72, 0x72, 0x61, 0x79, 0x50, 0x72, 0x6f, 0x70, 0x65, 0x72, 0x74, 0x69, 0x65, 0x73,
	0x12, 0x4a, 0x0a, 0x11, 0x6f, 0x62, 0x6a, 0x65, 0x63, 0x74, 0x5f, 0x70, 0x72, 0x6f, 0x70, 0x65,
	0x72, 0x74, 0x69, 0x65, 0x73, 0x18, 0x08, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x1d, 0x2e, 0x77, 0x65,
	0x61, 0x76, 0x69, 0x61, 0x74, 0x65, 0x2e, 0x76, 0x31, 0x2e, 0x4f, 0x62, 0x6a, 0x65, 0x63, 0x74,
	0x50, 0x72, 0x6f, 0x70, 0x65, 0x72, 0x74, 0x69, 0x65, 0x73, 0x52, 0x10, 0x6f, 0x62, 0x6a, 0x65,
	0x63, 0x74, 0x50, 0x72, 0x6f, 0x70, 0x65, 0x72, 0x74, 0x69, 0x65, 0x73, 0x12, 0x5a, 0x0a, 0x17,
	0x6f, 0x62, 0x6a, 0x65, 0x63, 0x74, 0x5f, 0x61, 0x72, 0x72, 0x61, 0x79, 0x5f, 0x70, 0x72, 0x6f,
	0x70, 0x65, 0x72, 0x74, 0x69, 0x65, 0x73, 0x18, 0x09, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x22, 0x2e,
	0x77, 0x65, 0x61, 0x76, 0x69, 0x61, 0x74, 0x65, 0x2e, 0x76, 0x31, 0x2e, 0x4f, 0x62, 0x6a, 0x65,
	0x63, 0x74, 0x41, 0x72, 0x72, 0x61, 0x79, 0x50, 0x72, 0x6f, 0x70, 0x65, 0x72, 0x74, 0x69, 0x65,
	0x73, 0x52, 0x15, 0x6f, 0x62, 0x6a, 0x65, 0x63, 0x74, 0x41, 0x72, 0x72, 0x61, 0x79, 0x50, 0x72,
	0x6f, 0x70, 0x65, 0x72, 0x74, 0x69, 0x65, 0x73, 0x1a, 0x49, 0x0a, 0x14, 0x53, 0x69, 0x6e, 0x67,
	0x6c, 0x65, 0x54, 0x61, 0x72, 0x67, 0x65, 0x74, 0x52, 0x65, 0x66, 0x50, 0x72, 0x6f, 0x70, 0x73,
	0x12, 0x14, 0x0a, 0x05, 0x75, 0x75, 0x69, 0x64, 0x73, 0x18, 0x01, 0x20, 0x03, 0x28, 0x09, 0x52,
	0x05, 0x75, 0x75, 0x69, 0x64, 0x73, 0x12, 0x1b, 0x0a, 0x09, 0x70, 0x72, 0x6f, 0x70, 0x5f, 0x6e,
	0x61, 0x6d, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x08, 0x70, 0x72, 0x6f, 0x70, 0x4e,
	0x61, 0x6d, 0x65, 0x1a, 0x75, 0x0a, 0x13, 0x4d, 0x75, 0x6c, 0x74, 0x69, 0x54, 0x61, 0x72, 0x67,
	0x65, 0x74, 0x52, 0x65, 0x66, 0x50, 0x72, 0x6f, 0x70, 0x73, 0x12, 0x14, 0x0a, 0x05, 0x75, 0x75,
	0x69, 0x64, 0x73, 0x18, 0x01, 0x20, 0x03, 0x28, 0x09, 0x52, 0x05, 0x75, 0x75, 0x69, 0x64, 0x73,
	0x12, 0x1b, 0x0a, 0x09, 0x70, 0x72, 0x6f, 0x70, 0x5f, 0x6e, 0x61, 0x6d, 0x65, 0x18, 0x02, 0x20,
	0x01, 0x28, 0x09, 0x52, 0x08, 0x70, 0x72, 0x6f, 0x70, 0x4e, 0x61, 0x6d, 0x65, 0x12, 0x2b, 0x0a,
	0x11, 0x74, 0x61, 0x72, 0x67, 0x65, 0x74, 0x5f, 0x63, 0x6f, 0x6c, 0x6c, 0x65, 0x63, 0x74, 0x69,
	0x6f, 0x6e, 0x18, 0x03, 0x20, 0x01, 0x28, 0x09, 0x52, 0x10, 0x74, 0x61, 0x72, 0x67, 0x65, 0x74,
	0x43, 0x6f, 0x6c, 0x6c, 0x65, 0x63, 0x74, 0x69, 0x6f, 0x6e, 0x22, 0xa4, 0x01, 0x0a, 0x11, 0x42,
	0x61, 0x74, 0x63, 0x68, 0x4f, 0x62, 0x6a, 0x65, 0x63, 0x74, 0x73, 0x52, 0x65, 0x70, 0x6c, 0x79,
	0x12, 0x12, 0x0a, 0x04, 0x74, 0x6f, 0x6f, 0x6b, 0x18, 0x01, 0x20, 0x01, 0x28, 0x02, 0x52, 0x04,
	0x74, 0x6f, 0x6f, 0x6b, 0x12, 0x41, 0x0a, 0x06, 0x65, 0x72, 0x72, 0x6f, 0x72, 0x73, 0x18, 0x02,
	0x20, 0x03, 0x28, 0x0b, 0x32, 0x29, 0x2e, 0x77, 0x65, 0x61, 0x76, 0x69, 0x61, 0x74, 0x65, 0x2e,
	0x76, 0x31, 0x2e, 0x42, 0x61, 0x74, 0x63, 0x68, 0x4f, 0x62, 0x6a, 0x65, 0x63, 0x74, 0x73, 0x52,
	0x65, 0x70, 0x6c, 0x79, 0x2e, 0x42, 0x61, 0x74, 0x63, 0x68, 0x45, 0x72, 0x72, 0x6f, 0x72, 0x52,
	0x06, 0x65, 0x72, 0x72, 0x6f, 0x72, 0x73, 0x1a, 0x38, 0x0a, 0x0a, 0x42, 0x61, 0x74, 0x63, 0x68,
	0x45, 0x72, 0x72, 0x6f, 0x72, 0x12, 0x14, 0x0a, 0x05, 0x69, 0x6e, 0x64, 0x65, 0x78, 0x18, 0x01,
	0x20, 0x01, 0x28, 0x05, 0x52, 0x05, 0x69, 0x6e, 0x64, 0x65, 0x78, 0x12, 0x14, 0x0a, 0x05, 0x65,
	0x72, 0x72, 0x6f, 0x72, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x05, 0x65, 0x72, 0x72, 0x6f,
	0x72, 0x42, 0x6f, 0x0a, 0x23, 0x69, 0x6f, 0x2e, 0x77, 0x65, 0x61, 0x76, 0x69, 0x61, 0x74, 0x65,
	0x2e, 0x63, 0x6c, 0x69, 0x65, 0x6e, 0x74, 0x2e, 0x67, 0x72, 0x70, 0x63, 0x2e, 0x70, 0x72, 0x6f,
	0x74, 0x6f, 0x63, 0x6f, 0x6c, 0x2e, 0x76, 0x31, 0x42, 0x12, 0x57, 0x65, 0x61, 0x76, 0x69, 0x61,
	0x74, 0x65, 0x50, 0x72, 0x6f, 0x74, 0x6f, 0x42, 0x61, 0x74, 0x63, 0x68, 0x5a, 0x34, 0x67, 0x69,
	0x74, 0x68, 0x75, 0x62, 0x2e, 0x63, 0x6f, 0x6d, 0x2f, 0x77, 0x65, 0x61, 0x76, 0x69, 0x61, 0x74,
	0x65, 0x2f, 0x77, 0x65, 0x61, 0x76, 0x69, 0x61, 0x74, 0x65, 0x2f, 0x67, 0x72, 0x70, 0x63, 0x2f,
	0x67, 0x65, 0x6e, 0x65, 0x72, 0x61, 0x74, 0x65, 0x64, 0x3b, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x63,
	0x6f, 0x6c, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_v1_batch_proto_rawDescOnce sync.Once
	file_v1_batch_proto_rawDescData = file_v1_batch_proto_rawDesc
)

func file_v1_batch_proto_rawDescGZIP() []byte {
	file_v1_batch_proto_rawDescOnce.Do(func() {
		file_v1_batch_proto_rawDescData = protoimpl.X.CompressGZIP(file_v1_batch_proto_rawDescData)
	})
	return file_v1_batch_proto_rawDescData
}

var (
	file_v1_batch_proto_msgTypes = make([]protoimpl.MessageInfo, 7)
	file_v1_batch_proto_goTypes  = []interface{}{
		(*BatchObjectsRequest)(nil),              // 0: weaviate.v1.BatchObjectsRequest
		(*BatchObject)(nil),                      // 1: weaviate.v1.BatchObject
		(*BatchObjectsReply)(nil),                // 2: weaviate.v1.BatchObjectsReply
		(*BatchObject_Properties)(nil),           // 3: weaviate.v1.BatchObject.Properties
		(*BatchObject_SingleTargetRefProps)(nil), // 4: weaviate.v1.BatchObject.SingleTargetRefProps
		(*BatchObject_MultiTargetRefProps)(nil),  // 5: weaviate.v1.BatchObject.MultiTargetRefProps
		(*BatchObjectsReply_BatchError)(nil),     // 6: weaviate.v1.BatchObjectsReply.BatchError
		(ConsistencyLevel)(0),                    // 7: weaviate.v1.ConsistencyLevel
		(*structpb.Struct)(nil),                  // 8: google.protobuf.Struct
		(*NumberArrayProperties)(nil),            // 9: weaviate.v1.NumberArrayProperties
		(*IntArrayProperties)(nil),               // 10: weaviate.v1.IntArrayProperties
		(*TextArrayProperties)(nil),              // 11: weaviate.v1.TextArrayProperties
		(*BooleanArrayProperties)(nil),           // 12: weaviate.v1.BooleanArrayProperties
		(*ObjectProperties)(nil),                 // 13: weaviate.v1.ObjectProperties
		(*ObjectArrayProperties)(nil),            // 14: weaviate.v1.ObjectArrayProperties
	}
)
var file_v1_batch_proto_depIdxs = []int32{
	1,  // 0: weaviate.v1.BatchObjectsRequest.objects:type_name -> weaviate.v1.BatchObject
	7,  // 1: weaviate.v1.BatchObjectsRequest.consistency_level:type_name -> weaviate.v1.ConsistencyLevel
	3,  // 2: weaviate.v1.BatchObject.properties:type_name -> weaviate.v1.BatchObject.Properties
	6,  // 3: weaviate.v1.BatchObjectsReply.errors:type_name -> weaviate.v1.BatchObjectsReply.BatchError
	8,  // 4: weaviate.v1.BatchObject.Properties.non_ref_properties:type_name -> google.protobuf.Struct
	4,  // 5: weaviate.v1.BatchObject.Properties.single_target_ref_props:type_name -> weaviate.v1.BatchObject.SingleTargetRefProps
	5,  // 6: weaviate.v1.BatchObject.Properties.multi_target_ref_props:type_name -> weaviate.v1.BatchObject.MultiTargetRefProps
	9,  // 7: weaviate.v1.BatchObject.Properties.number_array_properties:type_name -> weaviate.v1.NumberArrayProperties
	10, // 8: weaviate.v1.BatchObject.Properties.int_array_properties:type_name -> weaviate.v1.IntArrayProperties
	11, // 9: weaviate.v1.BatchObject.Properties.text_array_properties:type_name -> weaviate.v1.TextArrayProperties
	12, // 10: weaviate.v1.BatchObject.Properties.boolean_array_properties:type_name -> weaviate.v1.BooleanArrayProperties
	13, // 11: weaviate.v1.BatchObject.Properties.object_properties:type_name -> weaviate.v1.ObjectProperties
	14, // 12: weaviate.v1.BatchObject.Properties.object_array_properties:type_name -> weaviate.v1.ObjectArrayProperties
	13, // [13:13] is the sub-list for method output_type
	13, // [13:13] is the sub-list for method input_type
	13, // [13:13] is the sub-list for extension type_name
	13, // [13:13] is the sub-list for extension extendee
	0,  // [0:13] is the sub-list for field type_name
}

func init() { file_v1_batch_proto_init() }
func file_v1_batch_proto_init() {
	if File_v1_batch_proto != nil {
		return
	}
	file_v1_base_proto_init()
	if !protoimpl.UnsafeEnabled {
		file_v1_batch_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*BatchObjectsRequest); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_v1_batch_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*BatchObject); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_v1_batch_proto_msgTypes[2].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*BatchObjectsReply); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_v1_batch_proto_msgTypes[3].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*BatchObject_Properties); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_v1_batch_proto_msgTypes[4].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*BatchObject_SingleTargetRefProps); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_v1_batch_proto_msgTypes[5].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*BatchObject_MultiTargetRefProps); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_v1_batch_proto_msgTypes[6].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*BatchObjectsReply_BatchError); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
	}
	file_v1_batch_proto_msgTypes[0].OneofWrappers = []interface{}{}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_v1_batch_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   7,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_v1_batch_proto_goTypes,
		DependencyIndexes: file_v1_batch_proto_depIdxs,
		MessageInfos:      file_v1_batch_proto_msgTypes,
	}.Build()
	File_v1_batch_proto = out.File
	file_v1_batch_proto_rawDesc = nil
	file_v1_batch_proto_goTypes = nil
	file_v1_batch_proto_depIdxs = nil
}
