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

package query

// Config represents any type of configs and flags to control the querier
type Config struct {
	GRPCListenAddr string `long:"grpc.listen" description:"gRPC address that query node listens at" default:"0.0.0.0:7071"`
	SchemaAddr     string `long:"schema.addr" description:"address to get schema information" default:"http://0.0.0.0:8080"`
	S3URL          string `long:"s3.url" description:"s3 URL to query offloaded tenants (e.g: s3://<url>)"`
	S3Endpoint     string `long:"s3.endpoint" description:"s3 endpoint to if mocking s3 (e.g: via minio)"`

	// NOTE(kavi): This `DataPath` makes `querier` statefulset. Depend on disk to serve query reqeust.
	// Main rationale is, we first download the objects from object store and put it on local disk
	// We need this, to make it work with existing query helpers, where we assume we serve query from
	// local disk. This will go away eventually once we start reading from object store into memory directly.
	DataPath string `long:"datapath" description:"place to look for tenant data after downloading it from object storage" default:"/tmp/weaviate"`

	VectorizerAddr string `long:"vectorize-addr" description:"vectorizer address to be used to vectorize near-text query" default:"0.0.0.0:9999"`

	// MetadataGRPCAddress is the host which will be used to connect to the metadata node's gRPC server.
	// Note that this should be replaced later to avoid having a single metadata node as a single point of failure.
	// If MetadataGRPCAddress is empty, the querier will connect to localhost.
	MetadataGRPCAddress string `long:"metadata.grpc.address" description:"metadata grpc address" default:":9050"`

	NoCache        bool  `long:"no-cache" description:"disable disk based cache on query path"`
	CacheMaxSizeGB int64 `long:"cache-max-size" description:"max size allocated for cache in GBs. More than this start evicting the cache" default:"20"`
}
