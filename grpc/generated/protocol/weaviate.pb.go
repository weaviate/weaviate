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

	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

var File_weaviate_proto protoreflect.FileDescriptor

var file_weaviate_proto_rawDesc = []byte{
	0x0a, 0x0e, 0x77, 0x65, 0x61, 0x76, 0x69, 0x61, 0x74, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f,
	0x12, 0x08, 0x77, 0x65, 0x61, 0x76, 0x69, 0x61, 0x74, 0x65, 0x1a, 0x0b, 0x62, 0x61, 0x74, 0x63,
	0x68, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x1a, 0x10, 0x73, 0x65, 0x61, 0x72, 0x63, 0x68, 0x5f,
	0x67, 0x65, 0x74, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x32, 0x94, 0x01, 0x0a, 0x08, 0x57, 0x65,
	0x61, 0x76, 0x69, 0x61, 0x74, 0x65, 0x12, 0x3a, 0x0a, 0x06, 0x53, 0x65, 0x61, 0x72, 0x63, 0x68,
	0x12, 0x17, 0x2e, 0x77, 0x65, 0x61, 0x76, 0x69, 0x61, 0x74, 0x65, 0x2e, 0x53, 0x65, 0x61, 0x72,
	0x63, 0x68, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x1a, 0x15, 0x2e, 0x77, 0x65, 0x61, 0x76,
	0x69, 0x61, 0x74, 0x65, 0x2e, 0x53, 0x65, 0x61, 0x72, 0x63, 0x68, 0x52, 0x65, 0x70, 0x6c, 0x79,
	0x22, 0x00, 0x12, 0x4c, 0x0a, 0x0c, 0x42, 0x61, 0x74, 0x63, 0x68, 0x4f, 0x62, 0x6a, 0x65, 0x63,
	0x74, 0x73, 0x12, 0x1d, 0x2e, 0x77, 0x65, 0x61, 0x76, 0x69, 0x61, 0x74, 0x65, 0x2e, 0x42, 0x61,
	0x74, 0x63, 0x68, 0x4f, 0x62, 0x6a, 0x65, 0x63, 0x74, 0x73, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73,
	0x74, 0x1a, 0x1b, 0x2e, 0x77, 0x65, 0x61, 0x76, 0x69, 0x61, 0x74, 0x65, 0x2e, 0x42, 0x61, 0x74,
	0x63, 0x68, 0x4f, 0x62, 0x6a, 0x65, 0x63, 0x74, 0x73, 0x52, 0x65, 0x70, 0x6c, 0x79, 0x22, 0x00,
	0x42, 0x60, 0x0a, 0x19, 0x69, 0x6f, 0x2e, 0x77, 0x65, 0x61, 0x76, 0x69, 0x61, 0x74, 0x65, 0x2e,
	0x67, 0x72, 0x70, 0x63, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x63, 0x6f, 0x6c, 0x42, 0x0d, 0x57,
	0x65, 0x61, 0x76, 0x69, 0x61, 0x74, 0x65, 0x50, 0x72, 0x6f, 0x74, 0x6f, 0x5a, 0x34, 0x67, 0x69,
	0x74, 0x68, 0x75, 0x62, 0x2e, 0x63, 0x6f, 0x6d, 0x2f, 0x77, 0x65, 0x61, 0x76, 0x69, 0x61, 0x74,
	0x65, 0x2f, 0x77, 0x65, 0x61, 0x76, 0x69, 0x61, 0x74, 0x65, 0x2f, 0x67, 0x72, 0x70, 0x63, 0x2f,
	0x67, 0x65, 0x6e, 0x65, 0x72, 0x61, 0x74, 0x65, 0x64, 0x3b, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x63,
	0x6f, 0x6c, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var file_weaviate_proto_goTypes = []interface{}{
	(*SearchRequest)(nil),       // 0: weaviate.SearchRequest
	(*BatchObjectsRequest)(nil), // 1: weaviate.BatchObjectsRequest
	(*SearchReply)(nil),         // 2: weaviate.SearchReply
	(*BatchObjectsReply)(nil),   // 3: weaviate.BatchObjectsReply
}

var file_weaviate_proto_depIdxs = []int32{
	0, // 0: weaviate.Weaviate.Search:input_type -> weaviate.SearchRequest
	1, // 1: weaviate.Weaviate.BatchObjects:input_type -> weaviate.BatchObjectsRequest
	2, // 2: weaviate.Weaviate.Search:output_type -> weaviate.SearchReply
	3, // 3: weaviate.Weaviate.BatchObjects:output_type -> weaviate.BatchObjectsReply
	2, // [2:4] is the sub-list for method output_type
	0, // [0:2] is the sub-list for method input_type
	0, // [0:0] is the sub-list for extension type_name
	0, // [0:0] is the sub-list for extension extendee
	0, // [0:0] is the sub-list for field type_name
}

func init() { file_weaviate_proto_init() }
func file_weaviate_proto_init() {
	if File_weaviate_proto != nil {
		return
	}
	file_batch_proto_init()
	file_search_get_proto_init()
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_weaviate_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   0,
			NumExtensions: 0,
			NumServices:   1,
		},
		GoTypes:           file_weaviate_proto_goTypes,
		DependencyIndexes: file_weaviate_proto_depIdxs,
	}.Build()
	File_weaviate_proto = out.File
	file_weaviate_proto_rawDesc = nil
	file_weaviate_proto_goTypes = nil
	file_weaviate_proto_depIdxs = nil
}
