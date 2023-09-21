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
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	reflect "reflect"
	sync "sync"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

type ConsistencyLevel int32

const (
	ConsistencyLevel_CONSISTENCY_LEVEL_UNSPECIFIED ConsistencyLevel = 0
	ConsistencyLevel_CONSISTENCY_LEVEL_ONE         ConsistencyLevel = 1
	ConsistencyLevel_CONSISTENCY_LEVEL_QUORUM      ConsistencyLevel = 2
	ConsistencyLevel_CONSISTENCY_LEVEL_ALL         ConsistencyLevel = 3
)

// Enum value maps for ConsistencyLevel.
var (
	ConsistencyLevel_name = map[int32]string{
		0: "CONSISTENCY_LEVEL_UNSPECIFIED",
		1: "CONSISTENCY_LEVEL_ONE",
		2: "CONSISTENCY_LEVEL_QUORUM",
		3: "CONSISTENCY_LEVEL_ALL",
	}
	ConsistencyLevel_value = map[string]int32{
		"CONSISTENCY_LEVEL_UNSPECIFIED": 0,
		"CONSISTENCY_LEVEL_ONE":         1,
		"CONSISTENCY_LEVEL_QUORUM":      2,
		"CONSISTENCY_LEVEL_ALL":         3,
	}
)

func (x ConsistencyLevel) Enum() *ConsistencyLevel {
	p := new(ConsistencyLevel)
	*p = x
	return p
}

func (x ConsistencyLevel) String() string {
	return protoimpl.X.EnumStringOf(x.Descriptor(), protoreflect.EnumNumber(x))
}

func (ConsistencyLevel) Descriptor() protoreflect.EnumDescriptor {
	return file_grpc_base_proto_enumTypes[0].Descriptor()
}

func (ConsistencyLevel) Type() protoreflect.EnumType {
	return &file_grpc_base_proto_enumTypes[0]
}

func (x ConsistencyLevel) Number() protoreflect.EnumNumber {
	return protoreflect.EnumNumber(x)
}

// Deprecated: Use ConsistencyLevel.Descriptor instead.
func (ConsistencyLevel) EnumDescriptor() ([]byte, []int) {
	return file_grpc_base_proto_rawDescGZIP(), []int{0}
}

type NumberArrayProperties struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Values   []float64 `protobuf:"fixed64,1,rep,packed,name=values,proto3" json:"values,omitempty"`
	PropName string    `protobuf:"bytes,2,opt,name=prop_name,json=propName,proto3" json:"prop_name,omitempty"`
}

func (x *NumberArrayProperties) Reset() {
	*x = NumberArrayProperties{}
	if protoimpl.UnsafeEnabled {
		mi := &file_grpc_base_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *NumberArrayProperties) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*NumberArrayProperties) ProtoMessage() {}

func (x *NumberArrayProperties) ProtoReflect() protoreflect.Message {
	mi := &file_grpc_base_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use NumberArrayProperties.ProtoReflect.Descriptor instead.
func (*NumberArrayProperties) Descriptor() ([]byte, []int) {
	return file_grpc_base_proto_rawDescGZIP(), []int{0}
}

func (x *NumberArrayProperties) GetValues() []float64 {
	if x != nil {
		return x.Values
	}
	return nil
}

func (x *NumberArrayProperties) GetPropName() string {
	if x != nil {
		return x.PropName
	}
	return ""
}

type IntArrayProperties struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Values   []int64 `protobuf:"varint,1,rep,packed,name=values,proto3" json:"values,omitempty"`
	PropName string  `protobuf:"bytes,2,opt,name=prop_name,json=propName,proto3" json:"prop_name,omitempty"`
}

func (x *IntArrayProperties) Reset() {
	*x = IntArrayProperties{}
	if protoimpl.UnsafeEnabled {
		mi := &file_grpc_base_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *IntArrayProperties) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*IntArrayProperties) ProtoMessage() {}

func (x *IntArrayProperties) ProtoReflect() protoreflect.Message {
	mi := &file_grpc_base_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use IntArrayProperties.ProtoReflect.Descriptor instead.
func (*IntArrayProperties) Descriptor() ([]byte, []int) {
	return file_grpc_base_proto_rawDescGZIP(), []int{1}
}

func (x *IntArrayProperties) GetValues() []int64 {
	if x != nil {
		return x.Values
	}
	return nil
}

func (x *IntArrayProperties) GetPropName() string {
	if x != nil {
		return x.PropName
	}
	return ""
}

type TextArrayProperties struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Values   []string `protobuf:"bytes,1,rep,name=values,proto3" json:"values,omitempty"`
	PropName string   `protobuf:"bytes,2,opt,name=prop_name,json=propName,proto3" json:"prop_name,omitempty"`
}

func (x *TextArrayProperties) Reset() {
	*x = TextArrayProperties{}
	if protoimpl.UnsafeEnabled {
		mi := &file_grpc_base_proto_msgTypes[2]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *TextArrayProperties) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*TextArrayProperties) ProtoMessage() {}

func (x *TextArrayProperties) ProtoReflect() protoreflect.Message {
	mi := &file_grpc_base_proto_msgTypes[2]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use TextArrayProperties.ProtoReflect.Descriptor instead.
func (*TextArrayProperties) Descriptor() ([]byte, []int) {
	return file_grpc_base_proto_rawDescGZIP(), []int{2}
}

func (x *TextArrayProperties) GetValues() []string {
	if x != nil {
		return x.Values
	}
	return nil
}

func (x *TextArrayProperties) GetPropName() string {
	if x != nil {
		return x.PropName
	}
	return ""
}

type BooleanArrayProperties struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Values   []bool `protobuf:"varint,1,rep,packed,name=values,proto3" json:"values,omitempty"`
	PropName string `protobuf:"bytes,2,opt,name=prop_name,json=propName,proto3" json:"prop_name,omitempty"`
}

func (x *BooleanArrayProperties) Reset() {
	*x = BooleanArrayProperties{}
	if protoimpl.UnsafeEnabled {
		mi := &file_grpc_base_proto_msgTypes[3]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *BooleanArrayProperties) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*BooleanArrayProperties) ProtoMessage() {}

func (x *BooleanArrayProperties) ProtoReflect() protoreflect.Message {
	mi := &file_grpc_base_proto_msgTypes[3]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use BooleanArrayProperties.ProtoReflect.Descriptor instead.
func (*BooleanArrayProperties) Descriptor() ([]byte, []int) {
	return file_grpc_base_proto_rawDescGZIP(), []int{3}
}

func (x *BooleanArrayProperties) GetValues() []bool {
	if x != nil {
		return x.Values
	}
	return nil
}

func (x *BooleanArrayProperties) GetPropName() string {
	if x != nil {
		return x.PropName
	}
	return ""
}

var File_grpc_base_proto protoreflect.FileDescriptor

var file_grpc_base_proto_rawDesc = []byte{
	0x0a, 0x0f, 0x67, 0x72, 0x70, 0x63, 0x2f, 0x62, 0x61, 0x73, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74,
	0x6f, 0x12, 0x04, 0x62, 0x61, 0x73, 0x65, 0x22, 0x4c, 0x0a, 0x15, 0x4e, 0x75, 0x6d, 0x62, 0x65,
	0x72, 0x41, 0x72, 0x72, 0x61, 0x79, 0x50, 0x72, 0x6f, 0x70, 0x65, 0x72, 0x74, 0x69, 0x65, 0x73,
	0x12, 0x16, 0x0a, 0x06, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x73, 0x18, 0x01, 0x20, 0x03, 0x28, 0x01,
	0x52, 0x06, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x73, 0x12, 0x1b, 0x0a, 0x09, 0x70, 0x72, 0x6f, 0x70,
	0x5f, 0x6e, 0x61, 0x6d, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x08, 0x70, 0x72, 0x6f,
	0x70, 0x4e, 0x61, 0x6d, 0x65, 0x22, 0x49, 0x0a, 0x12, 0x49, 0x6e, 0x74, 0x41, 0x72, 0x72, 0x61,
	0x79, 0x50, 0x72, 0x6f, 0x70, 0x65, 0x72, 0x74, 0x69, 0x65, 0x73, 0x12, 0x16, 0x0a, 0x06, 0x76,
	0x61, 0x6c, 0x75, 0x65, 0x73, 0x18, 0x01, 0x20, 0x03, 0x28, 0x03, 0x52, 0x06, 0x76, 0x61, 0x6c,
	0x75, 0x65, 0x73, 0x12, 0x1b, 0x0a, 0x09, 0x70, 0x72, 0x6f, 0x70, 0x5f, 0x6e, 0x61, 0x6d, 0x65,
	0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x08, 0x70, 0x72, 0x6f, 0x70, 0x4e, 0x61, 0x6d, 0x65,
	0x22, 0x4a, 0x0a, 0x13, 0x54, 0x65, 0x78, 0x74, 0x41, 0x72, 0x72, 0x61, 0x79, 0x50, 0x72, 0x6f,
	0x70, 0x65, 0x72, 0x74, 0x69, 0x65, 0x73, 0x12, 0x16, 0x0a, 0x06, 0x76, 0x61, 0x6c, 0x75, 0x65,
	0x73, 0x18, 0x01, 0x20, 0x03, 0x28, 0x09, 0x52, 0x06, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x73, 0x12,
	0x1b, 0x0a, 0x09, 0x70, 0x72, 0x6f, 0x70, 0x5f, 0x6e, 0x61, 0x6d, 0x65, 0x18, 0x02, 0x20, 0x01,
	0x28, 0x09, 0x52, 0x08, 0x70, 0x72, 0x6f, 0x70, 0x4e, 0x61, 0x6d, 0x65, 0x22, 0x4d, 0x0a, 0x16,
	0x42, 0x6f, 0x6f, 0x6c, 0x65, 0x61, 0x6e, 0x41, 0x72, 0x72, 0x61, 0x79, 0x50, 0x72, 0x6f, 0x70,
	0x65, 0x72, 0x74, 0x69, 0x65, 0x73, 0x12, 0x16, 0x0a, 0x06, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x73,
	0x18, 0x01, 0x20, 0x03, 0x28, 0x08, 0x52, 0x06, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x73, 0x12, 0x1b,
	0x0a, 0x09, 0x70, 0x72, 0x6f, 0x70, 0x5f, 0x6e, 0x61, 0x6d, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28,
	0x09, 0x52, 0x08, 0x70, 0x72, 0x6f, 0x70, 0x4e, 0x61, 0x6d, 0x65, 0x2a, 0x89, 0x01, 0x0a, 0x10,
	0x43, 0x6f, 0x6e, 0x73, 0x69, 0x73, 0x74, 0x65, 0x6e, 0x63, 0x79, 0x4c, 0x65, 0x76, 0x65, 0x6c,
	0x12, 0x21, 0x0a, 0x1d, 0x43, 0x4f, 0x4e, 0x53, 0x49, 0x53, 0x54, 0x45, 0x4e, 0x43, 0x59, 0x5f,
	0x4c, 0x45, 0x56, 0x45, 0x4c, 0x5f, 0x55, 0x4e, 0x53, 0x50, 0x45, 0x43, 0x49, 0x46, 0x49, 0x45,
	0x44, 0x10, 0x00, 0x12, 0x19, 0x0a, 0x15, 0x43, 0x4f, 0x4e, 0x53, 0x49, 0x53, 0x54, 0x45, 0x4e,
	0x43, 0x59, 0x5f, 0x4c, 0x45, 0x56, 0x45, 0x4c, 0x5f, 0x4f, 0x4e, 0x45, 0x10, 0x01, 0x12, 0x1c,
	0x0a, 0x18, 0x43, 0x4f, 0x4e, 0x53, 0x49, 0x53, 0x54, 0x45, 0x4e, 0x43, 0x59, 0x5f, 0x4c, 0x45,
	0x56, 0x45, 0x4c, 0x5f, 0x51, 0x55, 0x4f, 0x52, 0x55, 0x4d, 0x10, 0x02, 0x12, 0x19, 0x0a, 0x15,
	0x43, 0x4f, 0x4e, 0x53, 0x49, 0x53, 0x54, 0x45, 0x4e, 0x43, 0x59, 0x5f, 0x4c, 0x45, 0x56, 0x45,
	0x4c, 0x5f, 0x41, 0x4c, 0x4c, 0x10, 0x03, 0x42, 0x36, 0x5a, 0x34, 0x67, 0x69, 0x74, 0x68, 0x75,
	0x62, 0x2e, 0x63, 0x6f, 0x6d, 0x2f, 0x77, 0x65, 0x61, 0x76, 0x69, 0x61, 0x74, 0x65, 0x2f, 0x77,
	0x65, 0x61, 0x76, 0x69, 0x61, 0x74, 0x65, 0x2f, 0x67, 0x72, 0x70, 0x63, 0x2f, 0x67, 0x65, 0x6e,
	0x65, 0x72, 0x61, 0x74, 0x65, 0x64, 0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x63, 0x6f, 0x6c, 0x62,
	0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_grpc_base_proto_rawDescOnce sync.Once
	file_grpc_base_proto_rawDescData = file_grpc_base_proto_rawDesc
)

func file_grpc_base_proto_rawDescGZIP() []byte {
	file_grpc_base_proto_rawDescOnce.Do(func() {
		file_grpc_base_proto_rawDescData = protoimpl.X.CompressGZIP(file_grpc_base_proto_rawDescData)
	})
	return file_grpc_base_proto_rawDescData
}

var file_grpc_base_proto_enumTypes = make([]protoimpl.EnumInfo, 1)
var file_grpc_base_proto_msgTypes = make([]protoimpl.MessageInfo, 4)
var file_grpc_base_proto_goTypes = []interface{}{
	(ConsistencyLevel)(0),          // 0: base.ConsistencyLevel
	(*NumberArrayProperties)(nil),  // 1: base.NumberArrayProperties
	(*IntArrayProperties)(nil),     // 2: base.IntArrayProperties
	(*TextArrayProperties)(nil),    // 3: base.TextArrayProperties
	(*BooleanArrayProperties)(nil), // 4: base.BooleanArrayProperties
}
var file_grpc_base_proto_depIdxs = []int32{
	0, // [0:0] is the sub-list for method output_type
	0, // [0:0] is the sub-list for method input_type
	0, // [0:0] is the sub-list for extension type_name
	0, // [0:0] is the sub-list for extension extendee
	0, // [0:0] is the sub-list for field type_name
}

func init() { file_grpc_base_proto_init() }
func file_grpc_base_proto_init() {
	if File_grpc_base_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_grpc_base_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*NumberArrayProperties); i {
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
		file_grpc_base_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*IntArrayProperties); i {
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
		file_grpc_base_proto_msgTypes[2].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*TextArrayProperties); i {
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
		file_grpc_base_proto_msgTypes[3].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*BooleanArrayProperties); i {
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
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_grpc_base_proto_rawDesc,
			NumEnums:      1,
			NumMessages:   4,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_grpc_base_proto_goTypes,
		DependencyIndexes: file_grpc_base_proto_depIdxs,
		EnumInfos:         file_grpc_base_proto_enumTypes,
		MessageInfos:      file_grpc_base_proto_msgTypes,
	}.Build()
	File_grpc_base_proto = out.File
	file_grpc_base_proto_rawDesc = nil
	file_grpc_base_proto_goTypes = nil
	file_grpc_base_proto_depIdxs = nil
}
