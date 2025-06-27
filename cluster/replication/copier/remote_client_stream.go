//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package copier

import (
	"context"

	protocol "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
	"google.golang.org/grpc/metadata"
)

// Create non-generic wrapper interface so it can be generated with mockery
type FileMetadataStream interface {
	Context() context.Context
	CloseSend() error
	Header() (metadata.MD, error)
	Trailer() metadata.MD
	RecvMsg(m interface{}) error
	SendMsg(interface{}) error
	Send(*protocol.GetFileMetadataRequest) error
	Recv() (*protocol.FileMetadata, error)
}

// Create non-generic wrapper interface so it can be generated with mockery
type FileChunkStream interface {
	Context() context.Context
	CloseSend() error
	Header() (metadata.MD, error)
	Trailer() metadata.MD
	RecvMsg(m interface{}) error
	SendMsg(interface{}) error
	Send(*protocol.GetFileRequest) error
	Recv() (*protocol.FileChunk, error)
}
