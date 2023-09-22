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
	ConsistencyLevel *ConsistencyLevel `protobuf:"varint,2,opt,name=consistency_level,json=consistencyLevel,proto3,enum=base.ConsistencyLevel,oneof" json:"consistency_level,omitempty"`
}

func (x *BatchObjectsRequest) Reset() {
	*x = BatchObjectsRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_grpc_batch_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *BatchObjectsRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*BatchObjectsRequest) ProtoMessage() {}

func (x *BatchObjectsRequest) ProtoReflect() protoreflect.Message {
	mi := &file_grpc_batch_proto_msgTypes[0]
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
	return file_grpc_batch_proto_rawDescGZIP(), []int{0}
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
	Vector     []float32               `protobuf:"fixed32,2,rep,packed,name=vector,proto3" json:"vector,omitempty"`
	Properties *BatchObject_Properties `protobuf:"bytes,3,opt,name=properties,proto3" json:"properties,omitempty"`
	ClassName  string                  `protobuf:"bytes,4,opt,name=class_name,json=className,proto3" json:"class_name,omitempty"`
	Tenant     string                  `protobuf:"bytes,5,opt,name=tenant,proto3" json:"tenant,omitempty"`
}

func (x *BatchObject) Reset() {
	*x = BatchObject{}
	if protoimpl.UnsafeEnabled {
		mi := &file_grpc_batch_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *BatchObject) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*BatchObject) ProtoMessage() {}

func (x *BatchObject) ProtoReflect() protoreflect.Message {
	mi := &file_grpc_batch_proto_msgTypes[1]
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
	return file_grpc_batch_proto_rawDescGZIP(), []int{1}
}

func (x *BatchObject) GetUuid() string {
	if x != nil {
		return x.Uuid
	}
	return ""
}

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

func (x *BatchObject) GetClassName() string {
	if x != nil {
		return x.ClassName
	}
	return ""
}

func (x *BatchObject) GetTenant() string {
	if x != nil {
		return x.Tenant
	}
	return ""
}

type BatchObjectsReply struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Results []*BatchObjectsReply_BatchResults `protobuf:"bytes,1,rep,name=results,proto3" json:"results,omitempty"`
	Took    float32                           `protobuf:"fixed32,2,opt,name=took,proto3" json:"took,omitempty"`
}

func (x *BatchObjectsReply) Reset() {
	*x = BatchObjectsReply{}
	if protoimpl.UnsafeEnabled {
		mi := &file_grpc_batch_proto_msgTypes[2]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *BatchObjectsReply) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*BatchObjectsReply) ProtoMessage() {}

func (x *BatchObjectsReply) ProtoReflect() protoreflect.Message {
	mi := &file_grpc_batch_proto_msgTypes[2]
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
	return file_grpc_batch_proto_rawDescGZIP(), []int{2}
}

func (x *BatchObjectsReply) GetResults() []*BatchObjectsReply_BatchResults {
	if x != nil {
		return x.Results
	}
	return nil
}

func (x *BatchObjectsReply) GetTook() float32 {
	if x != nil {
		return x.Took
	}
	return 0
}

type BatchObject_Properties struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	NonRefProperties *structpb.Struct `protobuf:"bytes,1,opt,name=non_ref_properties,json=nonRefProperties,proto3" json:"non_ref_properties,omitempty"`
	// protolint:disable:next REPEATED_FIELD_NAMES_PLURALIZED
	RefPropsSingle []*BatchObject_RefPropertiesSingleTarget `protobuf:"bytes,2,rep,name=ref_props_single,json=refPropsSingle,proto3" json:"ref_props_single,omitempty"`
	// protolint:disable:next REPEATED_FIELD_NAMES_PLURALIZED
	RefPropsMulti          []*BatchObject_RefPropertiesMultiTarget `protobuf:"bytes,3,rep,name=ref_props_multi,json=refPropsMulti,proto3" json:"ref_props_multi,omitempty"`
	NumberArrayProperties  []*NumberArrayProperties                `protobuf:"bytes,4,rep,name=number_array_properties,json=numberArrayProperties,proto3" json:"number_array_properties,omitempty"`
	IntArrayProperties     []*IntArrayProperties                   `protobuf:"bytes,5,rep,name=int_array_properties,json=intArrayProperties,proto3" json:"int_array_properties,omitempty"`
	TextArrayProperties    []*TextArrayProperties                  `protobuf:"bytes,6,rep,name=text_array_properties,json=textArrayProperties,proto3" json:"text_array_properties,omitempty"`
	BooleanArrayProperties []*BooleanArrayProperties               `protobuf:"bytes,7,rep,name=boolean_array_properties,json=booleanArrayProperties,proto3" json:"boolean_array_properties,omitempty"`
}

func (x *BatchObject_Properties) Reset() {
	*x = BatchObject_Properties{}
	if protoimpl.UnsafeEnabled {
		mi := &file_grpc_batch_proto_msgTypes[3]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *BatchObject_Properties) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*BatchObject_Properties) ProtoMessage() {}

func (x *BatchObject_Properties) ProtoReflect() protoreflect.Message {
	mi := &file_grpc_batch_proto_msgTypes[3]
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
	return file_grpc_batch_proto_rawDescGZIP(), []int{1, 0}
}

func (x *BatchObject_Properties) GetNonRefProperties() *structpb.Struct {
	if x != nil {
		return x.NonRefProperties
	}
	return nil
}

func (x *BatchObject_Properties) GetRefPropsSingle() []*BatchObject_RefPropertiesSingleTarget {
	if x != nil {
		return x.RefPropsSingle
	}
	return nil
}

func (x *BatchObject_Properties) GetRefPropsMulti() []*BatchObject_RefPropertiesMultiTarget {
	if x != nil {
		return x.RefPropsMulti
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

type BatchObject_RefPropertiesSingleTarget struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Uuids    []string `protobuf:"bytes,1,rep,name=uuids,proto3" json:"uuids,omitempty"`
	PropName string   `protobuf:"bytes,2,opt,name=prop_name,json=propName,proto3" json:"prop_name,omitempty"`
}

func (x *BatchObject_RefPropertiesSingleTarget) Reset() {
	*x = BatchObject_RefPropertiesSingleTarget{}
	if protoimpl.UnsafeEnabled {
		mi := &file_grpc_batch_proto_msgTypes[4]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *BatchObject_RefPropertiesSingleTarget) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*BatchObject_RefPropertiesSingleTarget) ProtoMessage() {}

func (x *BatchObject_RefPropertiesSingleTarget) ProtoReflect() protoreflect.Message {
	mi := &file_grpc_batch_proto_msgTypes[4]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use BatchObject_RefPropertiesSingleTarget.ProtoReflect.Descriptor instead.
func (*BatchObject_RefPropertiesSingleTarget) Descriptor() ([]byte, []int) {
	return file_grpc_batch_proto_rawDescGZIP(), []int{1, 1}
}

func (x *BatchObject_RefPropertiesSingleTarget) GetUuids() []string {
	if x != nil {
		return x.Uuids
	}
	return nil
}

func (x *BatchObject_RefPropertiesSingleTarget) GetPropName() string {
	if x != nil {
		return x.PropName
	}
	return ""
}

type BatchObject_RefPropertiesMultiTarget struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Uuids            []string `protobuf:"bytes,1,rep,name=uuids,proto3" json:"uuids,omitempty"`
	PropName         string   `protobuf:"bytes,2,opt,name=prop_name,json=propName,proto3" json:"prop_name,omitempty"`
	TargetCollection string   `protobuf:"bytes,3,opt,name=target_collection,json=targetCollection,proto3" json:"target_collection,omitempty"`
}

func (x *BatchObject_RefPropertiesMultiTarget) Reset() {
	*x = BatchObject_RefPropertiesMultiTarget{}
	if protoimpl.UnsafeEnabled {
		mi := &file_grpc_batch_proto_msgTypes[5]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *BatchObject_RefPropertiesMultiTarget) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*BatchObject_RefPropertiesMultiTarget) ProtoMessage() {}

func (x *BatchObject_RefPropertiesMultiTarget) ProtoReflect() protoreflect.Message {
	mi := &file_grpc_batch_proto_msgTypes[5]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use BatchObject_RefPropertiesMultiTarget.ProtoReflect.Descriptor instead.
func (*BatchObject_RefPropertiesMultiTarget) Descriptor() ([]byte, []int) {
	return file_grpc_batch_proto_rawDescGZIP(), []int{1, 2}
}

func (x *BatchObject_RefPropertiesMultiTarget) GetUuids() []string {
	if x != nil {
		return x.Uuids
	}
	return nil
}

func (x *BatchObject_RefPropertiesMultiTarget) GetPropName() string {
	if x != nil {
		return x.PropName
	}
	return ""
}

func (x *BatchObject_RefPropertiesMultiTarget) GetTargetCollection() string {
	if x != nil {
		return x.TargetCollection
	}
	return ""
}

type BatchObjectsReply_BatchResults struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Index int32  `protobuf:"varint,1,opt,name=index,proto3" json:"index,omitempty"`
	Error string `protobuf:"bytes,2,opt,name=error,proto3" json:"error,omitempty"`
}

func (x *BatchObjectsReply_BatchResults) Reset() {
	*x = BatchObjectsReply_BatchResults{}
	if protoimpl.UnsafeEnabled {
		mi := &file_grpc_batch_proto_msgTypes[6]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *BatchObjectsReply_BatchResults) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*BatchObjectsReply_BatchResults) ProtoMessage() {}

func (x *BatchObjectsReply_BatchResults) ProtoReflect() protoreflect.Message {
	mi := &file_grpc_batch_proto_msgTypes[6]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use BatchObjectsReply_BatchResults.ProtoReflect.Descriptor instead.
func (*BatchObjectsReply_BatchResults) Descriptor() ([]byte, []int) {
	return file_grpc_batch_proto_rawDescGZIP(), []int{2, 0}
}

func (x *BatchObjectsReply_BatchResults) GetIndex() int32 {
	if x != nil {
		return x.Index
	}
	return 0
}

func (x *BatchObjectsReply_BatchResults) GetError() string {
	if x != nil {
		return x.Error
	}
	return ""
}

var File_grpc_batch_proto protoreflect.FileDescriptor

var file_grpc_batch_proto_rawDesc = []byte{
	0x0a, 0x10, 0x67, 0x72, 0x70, 0x63, 0x2f, 0x62, 0x61, 0x74, 0x63, 0x68, 0x2e, 0x70, 0x72, 0x6f,
	0x74, 0x6f, 0x12, 0x05, 0x62, 0x61, 0x74, 0x63, 0x68, 0x1a, 0x1c, 0x67, 0x6f, 0x6f, 0x67, 0x6c,
	0x65, 0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2f, 0x73, 0x74, 0x72, 0x75, 0x63,
	0x74, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x1a, 0x0f, 0x67, 0x72, 0x70, 0x63, 0x2f, 0x62, 0x61,
	0x73, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x22, 0xa3, 0x01, 0x0a, 0x13, 0x42, 0x61, 0x74,
	0x63, 0x68, 0x4f, 0x62, 0x6a, 0x65, 0x63, 0x74, 0x73, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74,
	0x12, 0x2c, 0x0a, 0x07, 0x6f, 0x62, 0x6a, 0x65, 0x63, 0x74, 0x73, 0x18, 0x01, 0x20, 0x03, 0x28,
	0x0b, 0x32, 0x12, 0x2e, 0x62, 0x61, 0x74, 0x63, 0x68, 0x2e, 0x42, 0x61, 0x74, 0x63, 0x68, 0x4f,
	0x62, 0x6a, 0x65, 0x63, 0x74, 0x52, 0x07, 0x6f, 0x62, 0x6a, 0x65, 0x63, 0x74, 0x73, 0x12, 0x48,
	0x0a, 0x11, 0x63, 0x6f, 0x6e, 0x73, 0x69, 0x73, 0x74, 0x65, 0x6e, 0x63, 0x79, 0x5f, 0x6c, 0x65,
	0x76, 0x65, 0x6c, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0e, 0x32, 0x16, 0x2e, 0x62, 0x61, 0x73, 0x65,
	0x2e, 0x43, 0x6f, 0x6e, 0x73, 0x69, 0x73, 0x74, 0x65, 0x6e, 0x63, 0x79, 0x4c, 0x65, 0x76, 0x65,
	0x6c, 0x48, 0x00, 0x52, 0x10, 0x63, 0x6f, 0x6e, 0x73, 0x69, 0x73, 0x74, 0x65, 0x6e, 0x63, 0x79,
	0x4c, 0x65, 0x76, 0x65, 0x6c, 0x88, 0x01, 0x01, 0x42, 0x14, 0x0a, 0x12, 0x5f, 0x63, 0x6f, 0x6e,
	0x73, 0x69, 0x73, 0x74, 0x65, 0x6e, 0x63, 0x79, 0x5f, 0x6c, 0x65, 0x76, 0x65, 0x6c, 0x22, 0xc6,
	0x07, 0x0a, 0x0b, 0x42, 0x61, 0x74, 0x63, 0x68, 0x4f, 0x62, 0x6a, 0x65, 0x63, 0x74, 0x12, 0x12,
	0x0a, 0x04, 0x75, 0x75, 0x69, 0x64, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x04, 0x75, 0x75,
	0x69, 0x64, 0x12, 0x16, 0x0a, 0x06, 0x76, 0x65, 0x63, 0x74, 0x6f, 0x72, 0x18, 0x02, 0x20, 0x03,
	0x28, 0x02, 0x52, 0x06, 0x76, 0x65, 0x63, 0x74, 0x6f, 0x72, 0x12, 0x3d, 0x0a, 0x0a, 0x70, 0x72,
	0x6f, 0x70, 0x65, 0x72, 0x74, 0x69, 0x65, 0x73, 0x18, 0x03, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x1d,
	0x2e, 0x62, 0x61, 0x74, 0x63, 0x68, 0x2e, 0x42, 0x61, 0x74, 0x63, 0x68, 0x4f, 0x62, 0x6a, 0x65,
	0x63, 0x74, 0x2e, 0x50, 0x72, 0x6f, 0x70, 0x65, 0x72, 0x74, 0x69, 0x65, 0x73, 0x52, 0x0a, 0x70,
	0x72, 0x6f, 0x70, 0x65, 0x72, 0x74, 0x69, 0x65, 0x73, 0x12, 0x1d, 0x0a, 0x0a, 0x63, 0x6c, 0x61,
	0x73, 0x73, 0x5f, 0x6e, 0x61, 0x6d, 0x65, 0x18, 0x04, 0x20, 0x01, 0x28, 0x09, 0x52, 0x09, 0x63,
	0x6c, 0x61, 0x73, 0x73, 0x4e, 0x61, 0x6d, 0x65, 0x12, 0x16, 0x0a, 0x06, 0x74, 0x65, 0x6e, 0x61,
	0x6e, 0x74, 0x18, 0x05, 0x20, 0x01, 0x28, 0x09, 0x52, 0x06, 0x74, 0x65, 0x6e, 0x61, 0x6e, 0x74,
	0x1a, 0xc8, 0x04, 0x0a, 0x0a, 0x50, 0x72, 0x6f, 0x70, 0x65, 0x72, 0x74, 0x69, 0x65, 0x73, 0x12,
	0x45, 0x0a, 0x12, 0x6e, 0x6f, 0x6e, 0x5f, 0x72, 0x65, 0x66, 0x5f, 0x70, 0x72, 0x6f, 0x70, 0x65,
	0x72, 0x74, 0x69, 0x65, 0x73, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x17, 0x2e, 0x67, 0x6f,
	0x6f, 0x67, 0x6c, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2e, 0x53, 0x74,
	0x72, 0x75, 0x63, 0x74, 0x52, 0x10, 0x6e, 0x6f, 0x6e, 0x52, 0x65, 0x66, 0x50, 0x72, 0x6f, 0x70,
	0x65, 0x72, 0x74, 0x69, 0x65, 0x73, 0x12, 0x56, 0x0a, 0x10, 0x72, 0x65, 0x66, 0x5f, 0x70, 0x72,
	0x6f, 0x70, 0x73, 0x5f, 0x73, 0x69, 0x6e, 0x67, 0x6c, 0x65, 0x18, 0x02, 0x20, 0x03, 0x28, 0x0b,
	0x32, 0x2c, 0x2e, 0x62, 0x61, 0x74, 0x63, 0x68, 0x2e, 0x42, 0x61, 0x74, 0x63, 0x68, 0x4f, 0x62,
	0x6a, 0x65, 0x63, 0x74, 0x2e, 0x52, 0x65, 0x66, 0x50, 0x72, 0x6f, 0x70, 0x65, 0x72, 0x74, 0x69,
	0x65, 0x73, 0x53, 0x69, 0x6e, 0x67, 0x6c, 0x65, 0x54, 0x61, 0x72, 0x67, 0x65, 0x74, 0x52, 0x0e,
	0x72, 0x65, 0x66, 0x50, 0x72, 0x6f, 0x70, 0x73, 0x53, 0x69, 0x6e, 0x67, 0x6c, 0x65, 0x12, 0x53,
	0x0a, 0x0f, 0x72, 0x65, 0x66, 0x5f, 0x70, 0x72, 0x6f, 0x70, 0x73, 0x5f, 0x6d, 0x75, 0x6c, 0x74,
	0x69, 0x18, 0x03, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x2b, 0x2e, 0x62, 0x61, 0x74, 0x63, 0x68, 0x2e,
	0x42, 0x61, 0x74, 0x63, 0x68, 0x4f, 0x62, 0x6a, 0x65, 0x63, 0x74, 0x2e, 0x52, 0x65, 0x66, 0x50,
	0x72, 0x6f, 0x70, 0x65, 0x72, 0x74, 0x69, 0x65, 0x73, 0x4d, 0x75, 0x6c, 0x74, 0x69, 0x54, 0x61,
	0x72, 0x67, 0x65, 0x74, 0x52, 0x0d, 0x72, 0x65, 0x66, 0x50, 0x72, 0x6f, 0x70, 0x73, 0x4d, 0x75,
	0x6c, 0x74, 0x69, 0x12, 0x53, 0x0a, 0x17, 0x6e, 0x75, 0x6d, 0x62, 0x65, 0x72, 0x5f, 0x61, 0x72,
	0x72, 0x61, 0x79, 0x5f, 0x70, 0x72, 0x6f, 0x70, 0x65, 0x72, 0x74, 0x69, 0x65, 0x73, 0x18, 0x04,
	0x20, 0x03, 0x28, 0x0b, 0x32, 0x1b, 0x2e, 0x62, 0x61, 0x73, 0x65, 0x2e, 0x4e, 0x75, 0x6d, 0x62,
	0x65, 0x72, 0x41, 0x72, 0x72, 0x61, 0x79, 0x50, 0x72, 0x6f, 0x70, 0x65, 0x72, 0x74, 0x69, 0x65,
	0x73, 0x52, 0x15, 0x6e, 0x75, 0x6d, 0x62, 0x65, 0x72, 0x41, 0x72, 0x72, 0x61, 0x79, 0x50, 0x72,
	0x6f, 0x70, 0x65, 0x72, 0x74, 0x69, 0x65, 0x73, 0x12, 0x4a, 0x0a, 0x14, 0x69, 0x6e, 0x74, 0x5f,
	0x61, 0x72, 0x72, 0x61, 0x79, 0x5f, 0x70, 0x72, 0x6f, 0x70, 0x65, 0x72, 0x74, 0x69, 0x65, 0x73,
	0x18, 0x05, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x18, 0x2e, 0x62, 0x61, 0x73, 0x65, 0x2e, 0x49, 0x6e,
	0x74, 0x41, 0x72, 0x72, 0x61, 0x79, 0x50, 0x72, 0x6f, 0x70, 0x65, 0x72, 0x74, 0x69, 0x65, 0x73,
	0x52, 0x12, 0x69, 0x6e, 0x74, 0x41, 0x72, 0x72, 0x61, 0x79, 0x50, 0x72, 0x6f, 0x70, 0x65, 0x72,
	0x74, 0x69, 0x65, 0x73, 0x12, 0x4d, 0x0a, 0x15, 0x74, 0x65, 0x78, 0x74, 0x5f, 0x61, 0x72, 0x72,
	0x61, 0x79, 0x5f, 0x70, 0x72, 0x6f, 0x70, 0x65, 0x72, 0x74, 0x69, 0x65, 0x73, 0x18, 0x06, 0x20,
	0x03, 0x28, 0x0b, 0x32, 0x19, 0x2e, 0x62, 0x61, 0x73, 0x65, 0x2e, 0x54, 0x65, 0x78, 0x74, 0x41,
	0x72, 0x72, 0x61, 0x79, 0x50, 0x72, 0x6f, 0x70, 0x65, 0x72, 0x74, 0x69, 0x65, 0x73, 0x52, 0x13,
	0x74, 0x65, 0x78, 0x74, 0x41, 0x72, 0x72, 0x61, 0x79, 0x50, 0x72, 0x6f, 0x70, 0x65, 0x72, 0x74,
	0x69, 0x65, 0x73, 0x12, 0x56, 0x0a, 0x18, 0x62, 0x6f, 0x6f, 0x6c, 0x65, 0x61, 0x6e, 0x5f, 0x61,
	0x72, 0x72, 0x61, 0x79, 0x5f, 0x70, 0x72, 0x6f, 0x70, 0x65, 0x72, 0x74, 0x69, 0x65, 0x73, 0x18,
	0x07, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x1c, 0x2e, 0x62, 0x61, 0x73, 0x65, 0x2e, 0x42, 0x6f, 0x6f,
	0x6c, 0x65, 0x61, 0x6e, 0x41, 0x72, 0x72, 0x61, 0x79, 0x50, 0x72, 0x6f, 0x70, 0x65, 0x72, 0x74,
	0x69, 0x65, 0x73, 0x52, 0x16, 0x62, 0x6f, 0x6f, 0x6c, 0x65, 0x61, 0x6e, 0x41, 0x72, 0x72, 0x61,
	0x79, 0x50, 0x72, 0x6f, 0x70, 0x65, 0x72, 0x74, 0x69, 0x65, 0x73, 0x1a, 0x4e, 0x0a, 0x19, 0x52,
	0x65, 0x66, 0x50, 0x72, 0x6f, 0x70, 0x65, 0x72, 0x74, 0x69, 0x65, 0x73, 0x53, 0x69, 0x6e, 0x67,
	0x6c, 0x65, 0x54, 0x61, 0x72, 0x67, 0x65, 0x74, 0x12, 0x14, 0x0a, 0x05, 0x75, 0x75, 0x69, 0x64,
	0x73, 0x18, 0x01, 0x20, 0x03, 0x28, 0x09, 0x52, 0x05, 0x75, 0x75, 0x69, 0x64, 0x73, 0x12, 0x1b,
	0x0a, 0x09, 0x70, 0x72, 0x6f, 0x70, 0x5f, 0x6e, 0x61, 0x6d, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28,
	0x09, 0x52, 0x08, 0x70, 0x72, 0x6f, 0x70, 0x4e, 0x61, 0x6d, 0x65, 0x1a, 0x7a, 0x0a, 0x18, 0x52,
	0x65, 0x66, 0x50, 0x72, 0x6f, 0x70, 0x65, 0x72, 0x74, 0x69, 0x65, 0x73, 0x4d, 0x75, 0x6c, 0x74,
	0x69, 0x54, 0x61, 0x72, 0x67, 0x65, 0x74, 0x12, 0x14, 0x0a, 0x05, 0x75, 0x75, 0x69, 0x64, 0x73,
	0x18, 0x01, 0x20, 0x03, 0x28, 0x09, 0x52, 0x05, 0x75, 0x75, 0x69, 0x64, 0x73, 0x12, 0x1b, 0x0a,
	0x09, 0x70, 0x72, 0x6f, 0x70, 0x5f, 0x6e, 0x61, 0x6d, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09,
	0x52, 0x08, 0x70, 0x72, 0x6f, 0x70, 0x4e, 0x61, 0x6d, 0x65, 0x12, 0x2b, 0x0a, 0x11, 0x74, 0x61,
	0x72, 0x67, 0x65, 0x74, 0x5f, 0x63, 0x6f, 0x6c, 0x6c, 0x65, 0x63, 0x74, 0x69, 0x6f, 0x6e, 0x18,
	0x03, 0x20, 0x01, 0x28, 0x09, 0x52, 0x10, 0x74, 0x61, 0x72, 0x67, 0x65, 0x74, 0x43, 0x6f, 0x6c,
	0x6c, 0x65, 0x63, 0x74, 0x69, 0x6f, 0x6e, 0x22, 0xa4, 0x01, 0x0a, 0x11, 0x42, 0x61, 0x74, 0x63,
	0x68, 0x4f, 0x62, 0x6a, 0x65, 0x63, 0x74, 0x73, 0x52, 0x65, 0x70, 0x6c, 0x79, 0x12, 0x3f, 0x0a,
	0x07, 0x72, 0x65, 0x73, 0x75, 0x6c, 0x74, 0x73, 0x18, 0x01, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x25,
	0x2e, 0x62, 0x61, 0x74, 0x63, 0x68, 0x2e, 0x42, 0x61, 0x74, 0x63, 0x68, 0x4f, 0x62, 0x6a, 0x65,
	0x63, 0x74, 0x73, 0x52, 0x65, 0x70, 0x6c, 0x79, 0x2e, 0x42, 0x61, 0x74, 0x63, 0x68, 0x52, 0x65,
	0x73, 0x75, 0x6c, 0x74, 0x73, 0x52, 0x07, 0x72, 0x65, 0x73, 0x75, 0x6c, 0x74, 0x73, 0x12, 0x12,
	0x0a, 0x04, 0x74, 0x6f, 0x6f, 0x6b, 0x18, 0x02, 0x20, 0x01, 0x28, 0x02, 0x52, 0x04, 0x74, 0x6f,
	0x6f, 0x6b, 0x1a, 0x3a, 0x0a, 0x0c, 0x42, 0x61, 0x74, 0x63, 0x68, 0x52, 0x65, 0x73, 0x75, 0x6c,
	0x74, 0x73, 0x12, 0x14, 0x0a, 0x05, 0x69, 0x6e, 0x64, 0x65, 0x78, 0x18, 0x01, 0x20, 0x01, 0x28,
	0x05, 0x52, 0x05, 0x69, 0x6e, 0x64, 0x65, 0x78, 0x12, 0x14, 0x0a, 0x05, 0x65, 0x72, 0x72, 0x6f,
	0x72, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x05, 0x65, 0x72, 0x72, 0x6f, 0x72, 0x42, 0x36,
	0x5a, 0x34, 0x67, 0x69, 0x74, 0x68, 0x75, 0x62, 0x2e, 0x63, 0x6f, 0x6d, 0x2f, 0x77, 0x65, 0x61,
	0x76, 0x69, 0x61, 0x74, 0x65, 0x2f, 0x77, 0x65, 0x61, 0x76, 0x69, 0x61, 0x74, 0x65, 0x2f, 0x67,
	0x72, 0x70, 0x63, 0x2f, 0x67, 0x65, 0x6e, 0x65, 0x72, 0x61, 0x74, 0x65, 0x64, 0x2f, 0x70, 0x72,
	0x6f, 0x74, 0x6f, 0x63, 0x6f, 0x6c, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_grpc_batch_proto_rawDescOnce sync.Once
	file_grpc_batch_proto_rawDescData = file_grpc_batch_proto_rawDesc
)

func file_grpc_batch_proto_rawDescGZIP() []byte {
	file_grpc_batch_proto_rawDescOnce.Do(func() {
		file_grpc_batch_proto_rawDescData = protoimpl.X.CompressGZIP(file_grpc_batch_proto_rawDescData)
	})
	return file_grpc_batch_proto_rawDescData
}

var (
	file_grpc_batch_proto_msgTypes = make([]protoimpl.MessageInfo, 7)
	file_grpc_batch_proto_goTypes  = []interface{}{
		(*BatchObjectsRequest)(nil),                   // 0: batch.BatchObjectsRequest
		(*BatchObject)(nil),                           // 1: batch.BatchObject
		(*BatchObjectsReply)(nil),                     // 2: batch.BatchObjectsReply
		(*BatchObject_Properties)(nil),                // 3: batch.BatchObject.Properties
		(*BatchObject_RefPropertiesSingleTarget)(nil), // 4: batch.BatchObject.RefPropertiesSingleTarget
		(*BatchObject_RefPropertiesMultiTarget)(nil),  // 5: batch.BatchObject.RefPropertiesMultiTarget
		(*BatchObjectsReply_BatchResults)(nil),        // 6: batch.BatchObjectsReply.BatchResults
		(ConsistencyLevel)(0),                         // 7: base.ConsistencyLevel
		(*structpb.Struct)(nil),                       // 8: google.protobuf.Struct
		(*NumberArrayProperties)(nil),                 // 9: base.NumberArrayProperties
		(*IntArrayProperties)(nil),                    // 10: base.IntArrayProperties
		(*TextArrayProperties)(nil),                   // 11: base.TextArrayProperties
		(*BooleanArrayProperties)(nil),                // 12: base.BooleanArrayProperties
	}
)

var file_grpc_batch_proto_depIdxs = []int32{
	1,  // 0: batch.BatchObjectsRequest.objects:type_name -> batch.BatchObject
	7,  // 1: batch.BatchObjectsRequest.consistency_level:type_name -> base.ConsistencyLevel
	3,  // 2: batch.BatchObject.properties:type_name -> batch.BatchObject.Properties
	6,  // 3: batch.BatchObjectsReply.results:type_name -> batch.BatchObjectsReply.BatchResults
	8,  // 4: batch.BatchObject.Properties.non_ref_properties:type_name -> google.protobuf.Struct
	4,  // 5: batch.BatchObject.Properties.ref_props_single:type_name -> batch.BatchObject.RefPropertiesSingleTarget
	5,  // 6: batch.BatchObject.Properties.ref_props_multi:type_name -> batch.BatchObject.RefPropertiesMultiTarget
	9,  // 7: batch.BatchObject.Properties.number_array_properties:type_name -> base.NumberArrayProperties
	10, // 8: batch.BatchObject.Properties.int_array_properties:type_name -> base.IntArrayProperties
	11, // 9: batch.BatchObject.Properties.text_array_properties:type_name -> base.TextArrayProperties
	12, // 10: batch.BatchObject.Properties.boolean_array_properties:type_name -> base.BooleanArrayProperties
	11, // [11:11] is the sub-list for method output_type
	11, // [11:11] is the sub-list for method input_type
	11, // [11:11] is the sub-list for extension type_name
	11, // [11:11] is the sub-list for extension extendee
	0,  // [0:11] is the sub-list for field type_name
}

func init() { file_grpc_batch_proto_init() }
func file_grpc_batch_proto_init() {
	if File_grpc_batch_proto != nil {
		return
	}
	file_grpc_base_proto_init()
	if !protoimpl.UnsafeEnabled {
		file_grpc_batch_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
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
		file_grpc_batch_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
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
		file_grpc_batch_proto_msgTypes[2].Exporter = func(v interface{}, i int) interface{} {
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
		file_grpc_batch_proto_msgTypes[3].Exporter = func(v interface{}, i int) interface{} {
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
		file_grpc_batch_proto_msgTypes[4].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*BatchObject_RefPropertiesSingleTarget); i {
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
		file_grpc_batch_proto_msgTypes[5].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*BatchObject_RefPropertiesMultiTarget); i {
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
		file_grpc_batch_proto_msgTypes[6].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*BatchObjectsReply_BatchResults); i {
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
	file_grpc_batch_proto_msgTypes[0].OneofWrappers = []interface{}{}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_grpc_batch_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   7,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_grpc_batch_proto_goTypes,
		DependencyIndexes: file_grpc_batch_proto_depIdxs,
		MessageInfos:      file_grpc_batch_proto_msgTypes,
	}.Build()
	File_grpc_batch_proto = out.File
	file_grpc_batch_proto_rawDesc = nil
	file_grpc_batch_proto_goTypes = nil
	file_grpc_batch_proto_depIdxs = nil
}
