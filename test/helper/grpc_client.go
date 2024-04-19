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

package helper

import (
	"crypto/tls"
	"fmt"
	"strings"

	pb "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

func CreateGrpcConnectionClient(host string) (*grpc.ClientConn, error) {
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithBlock())
	if strings.HasSuffix(host, ":443") {
		tlsConfig := &tls.Config{
			InsecureSkipVerify: true,
		}
		opts = append(opts, grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)))
	} else {
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}
	conn, err := grpc.Dial(host, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to dial: %w", err)
	}
	return conn, nil
}

func CreateGrpcWeaviateClient(conn *grpc.ClientConn) pb.WeaviateClient {
	return pb.NewWeaviateClient(conn)
}
